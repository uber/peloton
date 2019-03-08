// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package launcher

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	aurora "github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/protocol"
	"go.uber.org/thriftrw/wire"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// LaunchableTask contains the changes to the task runtime, expressed as the
// map RuntimeDiff, to make the task launchable and the configuration of
// the task
type LaunchableTask struct {
	// RuntimeDiff is the diff to be applied to the task runtime,
	// before launch it
	RuntimeDiff jobmgrcommon.RuntimeDiff
	// Config is the task config of the task to be launched
	Config *task.TaskConfig
	// ConfigAddOn is the task config add on
	ConfigAddOn *models.ConfigAddOn
}

// LaunchableTaskInfo contains the info of a task to be launched
type LaunchableTaskInfo struct {
	*task.TaskInfo
	// ConfigAddOn is the task config add on
	ConfigAddOn *models.ConfigAddOn
}

// Assignment information used to generate "AssignedTask"
type assignmentInfo struct {
	// Mesos task id
	taskID string
	// Mesos slave id that this task has been assigned to
	slaveID string
	// The name of the machine that this task has been assigned to
	slaveHost string
	// Ports reserved on the machine while this task is running
	assignedPorts map[string]int32
	// The instance ID assigned to this task
	instanceID int32
}

// Launcher defines the interface of task launcher which launches
// tasks from the placed queues of resource pool
type Launcher interface {
	// ProcessPlacement launches tasks to hostmgr
	ProcessPlacement(
		ctx context.Context,
		tasks []*hostsvc.LaunchableTask,
		placement *resmgr.Placement) error
	// LaunchStatefulTasks launches stateful task with reserved resource to
	// hostmgr directly.
	LaunchStatefulTasks(
		ctx context.Context,
		selectedTasks []*hostsvc.LaunchableTask,
		hostname string,
		selectedPorts []uint32,
		hostOfferID *peloton.HostOfferID,
		checkVolume bool,
	) error
	// GetLaunchableTasks returns current task configuration and
	// the runtime diff which needs to be patched onto existing runtime for
	// each launchable task. The second return value contains the tasks that
	// were skipped, for example because they were not found.
	GetLaunchableTasks(
		ctx context.Context,
		tasks []*peloton.TaskID,
		hostname string,
		agentID *mesos.AgentID,
		selectedPorts []uint32,
	) (map[string]*LaunchableTask, []*peloton.TaskID, error)
	// CreateLaunchableTasks generates list of hostsvc.LaunchableTask and a map
	// of skipped TaskInfo from map of TaskInfo
	CreateLaunchableTasks(
		ctx context.Context, tasks map[string]*LaunchableTaskInfo) (
		[]*hostsvc.LaunchableTask, map[string]*LaunchableTaskInfo)
	// TryReturnOffers returns the offers in the placement back to host manager
	TryReturnOffers(
		ctx context.Context,
		err error,
		placement *resmgr.Placement) error
}

// launcher implements the Launcher interface
type launcher struct {
	sync.Mutex
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	jobFactory    cached.JobFactory
	taskStore     storage.TaskStore
	volumeStore   storage.PersistentVolumeStore
	secretInfoOps ormobjects.SecretInfoOps
	metrics       *Metrics
	retryPolicy   backoff.RetryPolicy
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second

	// default secret operations cassandra timeout
	_defaultSecretInfoOpsTimeout = 10 * time.Second
)

var (
	errEmptyTasks         = errors.New("empty tasks infos")
	errLaunchInvalidOffer = errors.New("invalid offer to launch tasks")
)

var taskLauncher *launcher
var onceInitTaskLauncher sync.Once

// InitTaskLauncher initializes a Task Launcher
func InitTaskLauncher(
	d *yarpc.Dispatcher,
	hostMgrClientName string,
	jobFactory cached.JobFactory,
	taskStore storage.TaskStore,
	volumeStore storage.PersistentVolumeStore,
	ormStore *ormobjects.Store,
	parent tally.Scope,
) {
	onceInitTaskLauncher.Do(func() {
		if taskLauncher != nil {
			log.Warning("Task launcher has already been initialized")
			return
		}

		taskLauncher = &launcher{
			hostMgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(hostMgrClientName)),
			jobFactory:    jobFactory,
			taskStore:     taskStore,
			volumeStore:   volumeStore,
			secretInfoOps: ormobjects.NewSecretInfoOps(ormStore),
			metrics:       NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
			// TODO: make launch retry policy config.
			retryPolicy: backoff.NewRetryPolicy(3, 15*time.Second),
		}
	})
}

// GetLauncher returns the task scheduler instance
func GetLauncher() Launcher {
	if taskLauncher == nil {
		log.Fatal("Task launcher is not initialized")
	}
	return taskLauncher
}

// ProcessPlacements launches tasks to host manager
func (l *launcher) ProcessPlacement(
	ctx context.Context,
	tasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement,
) error {
	l.metrics.LauncherGoRoutines.Inc(1)

	// Populate custom executor data with placement info
	for _, launchableTask := range tasks {
		err := populateExecutorData(launchableTask, placement)
		if err != nil {
			l.TryReturnOffers(ctx, err, placement)
			log.WithError(err).WithFields(log.Fields{
				"placement":       placement,
				"launchable_task": launchableTask,
			}).Error("failed to populate custom executor data while launching tasks")
			return err
		}
	}

	err := l.launchTasks(ctx, tasks, placement)
	l.TryReturnOffers(ctx, err, placement)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"placement":   placement,
			"tasks_total": len(tasks),
		}).Error("failed to launch tasks to hostmgr")
		l.metrics.TaskRequeuedOnLaunchFail.Inc(int64(len(tasks)))
	}
	return err
}

func (l *launcher) isLauncherRetryableError(err error) bool {
	switch err {
	case errLaunchInvalidOffer:
		return false
	}

	log.WithError(err).Warn("task launch need to be retried")
	l.metrics.TaskLaunchRetry.Inc(1)
	// s.InternalServiceError
	return true
}

// GetLaunchableTasks returns current task configuration and
// the runtime diff which needs to be patched onto existing runtime for
// each launchable task.
func (l *launcher) GetLaunchableTasks(
	ctx context.Context,
	tasks []*peloton.TaskID,
	hostname string,
	agentID *mesos.AgentID,
	selectedPorts []uint32) (
	map[string]*LaunchableTask,
	[]*peloton.TaskID,
	error) {
	portsIndex := 0

	launchableTasks := make(map[string]*LaunchableTask)
	skippedTasks := make([]*peloton.TaskID, 0)
	getTaskInfoStart := time.Now()

	for _, taskID := range tasks {
		id, instanceID, err := util.ParseTaskID(taskID.GetValue())
		jobID := &peloton.JobID{Value: id}

		cachedJob := l.jobFactory.GetJob(jobID)
		if cachedJob == nil {
			skippedTasks = append(skippedTasks, taskID)
			continue
		}
		cachedTask, err := cachedJob.AddTask(ctx, uint32(instanceID))
		if err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      jobID.GetValue(),
					"instance_id": uint32(instanceID),
				}).Error("cannot add and recover task from DB")
			continue
		}

		cachedRuntime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      jobID.GetValue(),
					"instance_id": uint32(instanceID),
				}).Error("cannot fetch task runtime")
			continue
		}

		// TODO: We need to add batch api's for getting all tasks in one shot
		taskConfig, configAddOn, err := l.taskStore.GetTaskConfig(ctx, jobID, uint32(instanceID), cachedRuntime.GetConfigVersion())
		if err != nil {
			log.WithError(err).WithField("task_id", taskID.GetValue()).
				Error("not able to get task configuration")
			continue
		}

		runtimeDiff := make(jobmgrcommon.RuntimeDiff)

		// Generate volume ID if not set for stateful task.
		if taskConfig.GetVolume() != nil {
			if cachedRuntime.GetVolumeID() == nil || hostname != cachedRuntime.GetHost() {
				newVolumeID := &peloton.VolumeID{
					Value: uuid.New(),
				}
				log.WithFields(log.Fields{
					"task_id":       taskID,
					"hostname":      hostname,
					"new_volume_id": newVolumeID,
				}).Info("generates new volume id for task")
				// Generates volume ID if first time launch the stateful task,
				// OR task is being launched to a different host.
				runtimeDiff[jobmgrcommon.VolumeIDField] = newVolumeID
			}
		}

		if cachedRuntime.GetGoalState() != task.TaskState_KILLED {
			runtimeDiff[jobmgrcommon.HostField] = hostname
			runtimeDiff[jobmgrcommon.AgentIDField] = agentID
			runtimeDiff[jobmgrcommon.StateField] = task.TaskState_LAUNCHED
		}

		if selectedPorts != nil {
			// Reset runtime ports to get new ports assignment if placement has ports.
			ports := make(map[string]uint32)
			// Assign selected dynamic port to task per port config.
			for _, portConfig := range taskConfig.GetPorts() {
				if portConfig.GetValue() != 0 {
					// Skip static port.
					continue
				}
				if portsIndex >= len(selectedPorts) {
					// This should never happen.
					log.WithFields(log.Fields{
						"selected_ports": selectedPorts,
						"task_id":        taskID,
					}).Error("placement contains less selected ports than required.")
					return nil, nil, errors.New("invalid placement")
				}
				ports[portConfig.GetName()] = selectedPorts[portsIndex]
				portsIndex++
			}
			runtimeDiff[jobmgrcommon.PortsField] = ports
		}

		runtimeDiff[jobmgrcommon.MessageField] = "Add hostname and ports"
		runtimeDiff[jobmgrcommon.ReasonField] = "REASON_UPDATE_OFFER"

		launchableTasks[taskID.GetValue()] = &LaunchableTask{
			RuntimeDiff: runtimeDiff,
			Config:      taskConfig,
			ConfigAddOn: configAddOn,
		}
	}

	if len(launchableTasks) == 0 && len(skippedTasks) == 0 {
		return nil, nil, errEmptyTasks
	}

	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Debug("GetTaskInfo")

	l.metrics.GetDBTaskInfo.Record(getTaskInfoDuration)

	return launchableTasks, skippedTasks, nil
}

// updateTaskRuntime updates task runtime with goalstate, reason and message
// for the given task id.
func (l *launcher) updateTaskRuntime(
	ctx context.Context, taskID string,
	goalstate task.TaskState, reason string, message string) error {
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: goalstate,
		jobmgrcommon.ReasonField:    reason,
		jobmgrcommon.MessageField:   message,
	}
	jobID, instanceID, err := util.ParseTaskID(taskID)
	if err != nil {
		return err
	}
	cachedJob := l.jobFactory.GetJob(&peloton.JobID{Value: jobID})
	if cachedJob == nil {
		return fmt.Errorf("jobID %v not found in cache", jobID)
	}
	// update the task in DB and cache, and then schedule to goalstate
	err = cachedJob.PatchTasks(ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{uint32(instanceID): runtimeDiff})
	if err != nil {
		return err
	}
	return nil
}

// CreateLaunchableTasks generates list of hostsvc.LaunchableTask from map of
// task.TaskInfo. For tasks containing secrets, it tries to populate secrets
// from DB. If some tasks are not launched, return a map of skipped taskInfos.
func (l *launcher) CreateLaunchableTasks(
	ctx context.Context, tasks map[string]*LaunchableTaskInfo,
) (launchableTasks []*hostsvc.LaunchableTask,
	skippedTaskInfos map[string]*LaunchableTaskInfo) {
	skippedTaskInfos = make(map[string]*LaunchableTaskInfo)
	for id, launchableTaskInfo := range tasks {
		// if task config has secret volumes, populate secret data in config
		err := l.populateSecrets(ctx, launchableTaskInfo.Config)
		if err != nil {
			if yarpcerrors.IsNotFound(err) {
				// This is not retryable and we will never recover
				// from this error. Mark the task runtime as KILLED
				// before dropping it so that we don't try to launch it
				// again. No need to enqueue to goalstate engine here.
				// The caller does that for all tasks in TaskInfo

				// TODO: Notify resmgr that the state of this task
				// is failed and it should not retry this task
				// Need a private resmgr API for this.
				err = l.updateTaskRuntime(
					ctx, id,
					task.TaskState_KILLED, "REASON_SECRET_NOT_FOUND",
					err.Error())
				if err != nil {
					// Not retrying here, worst case we will attempt to launch
					// this task again from ProcessPlacement() call, and mark
					// goalstate properly in the next iteration.
					log.WithError(err).WithField("task_id", id).
						Error("failed to update goalstate to KILLED")
				}
			} else {
				// Skip this task in case of transient error but add it to
				// skippedTaskInfos so that the caller can ask resmgr to
				// launch this task again
				log.WithError(err).WithField("task_id", id).
					Error("populateSecrets failed. skipping task")
				skippedTaskInfos[id] = launchableTaskInfo
			}
			// skip the task for which we could not populate secrets
			continue
		}

		// Strip off labels with prefix common.SystemLabelPrefix. This is a temporary
		// fix to ensure job creates dont fail on clients adding the system labels
		// TODO: remove this once all Peloton clients have been modified
		// to not add these labels to jobs submitted through them
		var labels []*peloton.Label
		for _, label := range launchableTaskInfo.GetConfig().GetLabels() {
			if !strings.HasPrefix(label.GetKey(), common.SystemLabelPrefix+".") {
				labels = append(labels, label)
			}
		}
		launchableTaskInfo.Config.Labels = labels

		// Set system labels as task labels
		for _, label := range launchableTaskInfo.ConfigAddOn.GetSystemLabels() {
			launchableTaskInfo.Config.Labels = append(
				launchableTaskInfo.Config.Labels,
				label)
		}
		// Add volume info into launchable task if task config has volume.
		launchableTask := hostsvc.LaunchableTask{
			TaskId: launchableTaskInfo.Runtime.GetMesosTaskId(),
			Config: launchableTaskInfo.Config,
			Ports:  launchableTaskInfo.Runtime.GetPorts(),
			Id: &peloton.TaskID{Value: util.CreatePelotonTaskID(
				launchableTaskInfo.GetJobId().GetValue(),
				launchableTaskInfo.GetInstanceId(),
			),
			},
		}
		if launchableTaskInfo.Config.GetVolume() != nil {
			diskResource := util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(float64(launchableTaskInfo.Config.GetVolume().GetSizeMB())).
				Build()
			launchableTask.Volume = &hostsvc.Volume{
				Id:            launchableTaskInfo.Runtime.GetVolumeID(),
				ContainerPath: launchableTaskInfo.Config.GetVolume().GetContainerPath(),
				Resource:      diskResource,
			}
		}
		launchableTasks = append(launchableTasks, &launchableTask)
	}
	return launchableTasks, skippedTaskInfos
}

func (l *launcher) LaunchStatefulTasks(
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	hostname string,
	selectedPorts []uint32,
	hostOfferID *peloton.HostOfferID,
	checkVolume bool,
) error {
	ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
	defer cancel()

	volumeID := &peloton.VolumeID{
		Value: selectedTasks[0].GetVolume().GetId().GetValue(),
	}
	createVolume := false
	if checkVolume {
		pv, err := l.volumeStore.GetPersistentVolume(ctx, volumeID)
		if err != nil {
			_, ok := err.(*storage.VolumeNotFoundError)
			if !ok {
				// volume store db read error.
				return err
			}
			// First time launch stateful task and create volume.
			createVolume = true
		} else if pv.GetState() != volume.VolumeState_CREATED {
			// Retry launch task but volume is not created.
			createVolume = true
		}
	}

	var err error
	var operations []*hostsvc.OfferOperation
	hostOperationFactory := NewHostOperationsFactory(selectedTasks, hostname, selectedPorts)
	if createVolume {
		// Reserve, Create and Launch the task.
		operations, err = hostOperationFactory.GetHostOperations(
			[]hostsvc.OfferOperation_Type{
				hostsvc.OfferOperation_RESERVE,
				hostsvc.OfferOperation_CREATE,
				hostsvc.OfferOperation_LAUNCH,
			})
	} else {
		// Launch using the volume.
		operations, err = hostOperationFactory.GetHostOperations(
			[]hostsvc.OfferOperation_Type{
				hostsvc.OfferOperation_LAUNCH,
			})
	}

	if err != nil {
		return err
	}

	var request = &hostsvc.OfferOperationsRequest{
		Hostname:   hostname,
		Operations: operations,
		Id:         hostOfferID,
	}

	log.WithFields(log.Fields{
		"request":        request,
		"selected_tasks": selectedTasks,
	}).Info("OfferOperations called")

	response, err := l.hostMgrClient.OfferOperations(ctx, request)
	if err != nil {
		return err
	}
	if response.GetError() != nil {
		return errors.New(response.Error.String())
	}
	return nil
}

func (l *launcher) launchBatchTasks(
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
	defer cancel()
	var request = &hostsvc.LaunchTasksRequest{
		Hostname: placement.GetHostname(),
		Tasks:    selectedTasks,
		AgentId:  placement.GetAgentId(),
		Id:       placement.GetHostOfferID(),
	}

	log.WithField("request", request).Debug("LaunchTasks Called")

	response, err := l.hostMgrClient.LaunchTasks(ctx, request)
	if err != nil {
		return err
	}
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"response":  response,
			"placement": placement,
		}).Error("hostmgr launch tasks got error resp")
		if response.GetError().GetInvalidOffers() != nil {
			return errLaunchInvalidOffer
		}
		return errors.New(response.Error.String())
	}
	return nil
}

func (l *launcher) launchTasks(
	ctx context.Context,
	selectedTasks []*hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	// TODO: Add retry Logic for tasks launching failure
	var err error
	if len(selectedTasks) == 0 {
		return errEmptyTasks
	}

	log.WithField("tasks", selectedTasks).Debug("Launching Tasks")

	callStart := time.Now()
	if placement.Type != resmgr.TaskType_STATEFUL {
		err = backoff.Retry(
			func() error {
				return l.launchBatchTasks(ctx, selectedTasks, placement)
			}, l.retryPolicy, l.isLauncherRetryableError)
	} else {
		err = l.LaunchStatefulTasks(
			ctx,
			selectedTasks,
			placement.GetHostname(),
			placement.GetPorts(),
			placement.GetHostOfferID(),
			true, /* checkVolume */
		)
	}
	callDuration := time.Since(callStart)

	if err != nil {
		l.metrics.TaskLaunchFail.Inc(1)
		return err
	}

	l.metrics.TaskLaunch.Inc(int64(len(selectedTasks)))

	log.WithFields(log.Fields{
		"num_tasks": len(selectedTasks),
		"placement": placement,
		"hostname":  placement.GetHostname(),
		"duration":  callDuration.Seconds(),
	}).Debug("Launched tasks")
	l.metrics.LaunchTasksCallDuration.Record(callDuration)
	return nil
}

func (l *launcher) TryReturnOffers(ctx context.Context, err error, placement *resmgr.Placement) error {
	if err != nil && err != errLaunchInvalidOffer {
		request := &hostsvc.ReleaseHostOffersRequest{
			HostOffers: []*hostsvc.HostOffer{{
				Hostname: placement.Hostname,
				AgentId:  placement.AgentId,
				Id:       placement.GetHostOfferID(),
			}},
		}
		ctx, cancel := context.WithTimeout(ctx, _rpcTimeout)
		defer cancel()
		_, err = l.hostMgrClient.ReleaseHostOffers(ctx, request)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"hostname": placement.Hostname,
				"agentid":  placement.AgentId,
			}).Error("failed to release host offers")
		}
	}
	return err
}

// populateSecrets checks task config for secret volumes.
// If the config has volumes of type secret, it means that the Value field
// of that secret contains the secret ID. This function queries
// the DB to fetch the secret by secret ID and then replaces
// the secret Value by the fetched secret data.
// We do this to prevent secrets from being leaked as a part
// of job or task config and populate the task config with
// actual secrets just before task launch.
func (l *launcher) populateSecrets(
	ctx context.Context,
	taskConfig *task.TaskConfig) error {
	if taskConfig.GetContainer().GetType() != mesos.ContainerInfo_MESOS {
		return nil
	}
	for _, volume := range taskConfig.GetContainer().GetVolumes() {
		if volume.GetSource().GetType() == mesos.Volume_Source_SECRET &&
			volume.GetSource().GetSecret().GetValue().GetData() != nil {
			// Replace secret ID with actual secret here.
			// This is done to make sure secrets are read from the DB
			// when it is absolutely necessary and that they are not
			// persisted in any place other than the secret_info table
			// (for example as part of job/task config)
			ctx, cancel := context.WithTimeout(
				context.Background(), _defaultSecretInfoOpsTimeout)
			defer cancel()

			secretID := string(volume.GetSource().GetSecret().GetValue().GetData())
			secretInfoObj, err := l.secretInfoOps.GetSecret(
				ctx,
				secretID,
			)

			if err != nil {
				l.metrics.TaskPopulateSecretFail.Inc(1)
				return err
			}
			secretStr, err := base64.StdEncoding.DecodeString(secretInfoObj.Data)
			if err != nil {
				l.metrics.TaskPopulateSecretFail.Inc(1)
				return err
			}
			volume.GetSource().GetSecret().GetValue().Data =
				[]byte(secretStr)
		}
	}
	return nil
}

// populateExecutorData transforms executor data in TaskConfig to data
// usable by actual custom executor. Currently, it only supports aurora
// thermos executor, in which case, it will pack the existing executor
// data along with placement information to binary-serialized AssignedTask
// thrift struct.
func populateExecutorData(
	launchableTask *hostsvc.LaunchableTask,
	placement *resmgr.Placement) error {
	executorData := launchableTask.GetConfig().GetExecutor().GetData()
	if launchableTask.GetConfig().GetExecutor().GetType() !=
		mesos.ExecutorInfo_CUSTOM || len(executorData) == 0 {
		return nil
	}

	taskID := launchableTask.GetTaskId().GetValue()
	_, instanceID, err := util.ParseJobAndInstanceID(taskID)
	if err != nil {
		return err
	}

	assignedPorts := make(map[string]int32)
	for name, num := range launchableTask.GetPorts() {
		assignedPorts[name] = int32(num)
	}
	assignment := assignmentInfo{
		taskID:        taskID,
		slaveID:       placement.GetAgentId().GetValue(),
		slaveHost:     placement.GetHostname(),
		assignedPorts: assignedPorts,
		instanceID:    int32(instanceID),
	}

	transformedData, err := generateAssignedTask(executorData, assignment)
	if err != nil {
		return err
	}
	launchableTask.GetConfig().GetExecutor().Data = transformedData

	return nil
}

// generateAssignedTask takes in binary form of "TaskConfig" thrift struct
// along with task assignment information, and generates "AssignedTask"
// thrift struct in binary form.
func generateAssignedTask(
	taskConfigData []byte,
	assignment assignmentInfo) ([]byte, error) {
	taskConfigWireValue, err := protocol.Binary.Decode(
		bytes.NewReader(taskConfigData),
		wire.TStruct,
	)
	if err != nil {
		return []byte{}, err
	}

	taskConfig := &aurora.TaskConfig{}
	err = taskConfig.FromWire(taskConfigWireValue)
	if err != nil {
		return []byte{}, err
	}

	assignedTask := &aurora.AssignedTask{}
	assignedTask.TaskId = &assignment.taskID
	assignedTask.SlaveId = &assignment.slaveID
	assignedTask.SlaveHost = &assignment.slaveHost
	assignedTask.Task = taskConfig
	assignedTask.AssignedPorts = assignment.assignedPorts
	assignedTask.InstanceId = &assignment.instanceID

	assignedTaskWireValue, err := assignedTask.ToWire()
	if err != nil {
		return []byte{}, err
	}
	var assignedTaskBuffer bytes.Buffer
	err = protocol.Binary.Encode(assignedTaskWireValue, &assignedTaskBuffer)
	if err != nil {
		return []byte{}, err
	}

	return assignedTaskBuffer.Bytes(), nil
}
