package launcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

// Launcher defines the interface of task launcher which launches
// tasks from the placed queues of resource pool
type Launcher interface {
	// ProcessPlacement launches tasks to hostmgr
	ProcessPlacement(ctx context.Context, tasks []*hostsvc.LaunchableTask, placement *resmgr.Placement) error
	// LaunchStatefulTasks launches stateful task with reserved resource to hostmgr directly.
	LaunchStatefulTasks(ctx context.Context, selectedTasks []*hostsvc.LaunchableTask, hostname string, selectedPorts []uint32, checkVolume bool) error
	// GetLaunchableTasks returns launchable tasks after updating their runtime state with the placement information
	GetLaunchableTasks(ctx context.Context, tasks []*peloton.TaskID, hostname string, agentID *mesos.AgentID, selectedPorts []uint32) (map[string]*task.TaskInfo, error)
	// CreateLaunchableTasks generates list of hostsvc.LaunchableTask and a map
	// of skipped TaskInfo from map of TaskInfo
	CreateLaunchableTasks(
		ctx context.Context, tasks map[string]*task.TaskInfo) (
		[]*hostsvc.LaunchableTask, map[string]*task.TaskInfo)
	// TryReturnOffers returns the offers in the placement back to host manager
	TryReturnOffers(ctx context.Context, err error, placement *resmgr.Placement) error
}

// launcher implements the Launcher interface
type launcher struct {
	sync.Mutex
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	jobFactory    cached.JobFactory
	taskStore     storage.TaskStore
	volumeStore   storage.PersistentVolumeStore
	secretStore   storage.SecretStore
	metrics       *Metrics
	retryPolicy   backoff.RetryPolicy
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second

	// default secret store cassandra timeout
	_defaultSecretStoreTimeout = 10 * time.Second
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
	secretStore storage.SecretStore,
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
			secretStore:   secretStore,
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
func (l *launcher) ProcessPlacement(ctx context.Context, tasks []*hostsvc.LaunchableTask, placement *resmgr.Placement) error {
	l.metrics.LauncherGoRoutines.Inc(1)
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

// The taskinfo being returned contains the current task configuration and
// the new runtime which needs to be updated into the DB for each launchable task.
func (l *launcher) GetLaunchableTasks(
	ctx context.Context,
	tasks []*peloton.TaskID,
	hostname string,
	agentID *mesos.AgentID,
	selectedPorts []uint32) (map[string]*task.TaskInfo, error) {
	portsIndex := 0

	tasksInfo := make(map[string]*task.TaskInfo)
	getTaskInfoStart := time.Now()

	for _, taskID := range tasks {
		id, instanceID, err := util.ParseTaskID(taskID.GetValue())
		jobID := &peloton.JobID{Value: id}

		cachedJob := l.jobFactory.GetJob(jobID)
		if cachedJob == nil {
			// job does not exist, just ignore the task?
			continue
		}
		cachedTask := cachedJob.AddTask(uint32(instanceID))

		cachedRuntime, err := cachedTask.GetRunTime(ctx)
		if err != nil {
			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      jobID.GetValue(),
					"instance_id": uint32(instanceID),
				}).Error("cannot fetch task runtime")
			continue
		}

		// TODO: We need to add batch api's for getting all tasks in one shot
		taskConfig, err := l.taskStore.GetTaskConfig(ctx, jobID, uint32(instanceID), cachedRuntime.GetConfigVersion())
		if err != nil {
			log.WithError(err).WithField("task_id", taskID.GetValue()).
				Error("not able to get task configuration")
			continue
		}

		runtime := &task.RuntimeInfo{}

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
				runtime.VolumeID = newVolumeID
			}
		}

		if cachedRuntime.GetGoalState() != task.TaskState_KILLED {
			runtime.Host = hostname
			runtime.AgentID = agentID
			runtime.State = task.TaskState_LAUNCHED
		}

		if selectedPorts != nil {
			// Reset runtime ports to get new ports assignment if placement has ports.
			runtime.Ports = make(map[string]uint32)
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
					return nil, errors.New("invalid placement")
				}
				runtime.Ports[portConfig.GetName()] = selectedPorts[portsIndex]
				portsIndex++
			}
		}

		runtime.Message = "Add hostname and ports"
		runtime.Reason = "REASON_UPDATE_OFFER"

		taskInfo := &task.TaskInfo{
			InstanceId: uint32(instanceID),
			JobId:      jobID,
			Config:     taskConfig,
			Runtime:    runtime,
		}
		tasksInfo[taskID.GetValue()] = taskInfo
	}

	if len(tasksInfo) == 0 {
		return nil, errEmptyTasks
	}

	getTaskInfoDuration := time.Since(getTaskInfoStart)

	log.WithFields(log.Fields{
		"num_tasks": len(tasks),
		"duration":  getTaskInfoDuration.Seconds(),
	}).Debug("GetTaskInfo")

	l.metrics.GetDBTaskInfo.Record(getTaskInfoDuration)

	return tasksInfo, nil
}

// updateTaskRuntime updates task runtime with goalstate, reason and message
// for the given task id.
func (l *launcher) updateTaskRuntime(
	ctx context.Context, taskID string,
	goalstate task.TaskState, reason string, message string) error {
	runtime := &task.RuntimeInfo{
		GoalState: goalstate,
		Reason:    reason,
		Message:   message,
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
	err = cachedJob.UpdateTasks(ctx,
		map[uint32]*task.RuntimeInfo{uint32(instanceID): runtime},
		cached.UpdateCacheAndDB)
	if err != nil {
		return err
	}
	return nil
}

// CreateLaunchableTasks generates list of hostsvc.LaunchableTask from map of
// task.TaskInfo. For tasks containing secrets, it tries to populate secrets
// from DB. If some tasks are not launched, return a map of skipped taskInfos.
func (l *launcher) CreateLaunchableTasks(
	ctx context.Context, tasks map[string]*task.TaskInfo,
) (launchableTasks []*hostsvc.LaunchableTask,
	skippedTaskInfos map[string]*task.TaskInfo) {
	skippedTaskInfos = make(map[string]*task.TaskInfo)
	for id, taskInfo := range tasks {
		// if task config has secret volumes, populate secret data in config
		err := l.populateSecrets(ctx, taskInfo.GetConfig())
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
				skippedTaskInfos[id] = taskInfo
			}
			// skip the task for which we could not populate secrets
			continue
		}
		// Add volume info into launchable task if task config has volume.
		launchableTask := hostsvc.LaunchableTask{
			TaskId: taskInfo.GetRuntime().GetMesosTaskId(),
			Config: taskInfo.GetConfig(),
			Ports:  taskInfo.GetRuntime().GetPorts(),
		}
		if taskInfo.GetConfig().GetVolume() != nil {
			diskResource := util.NewMesosResourceBuilder().
				WithName("disk").
				WithValue(float64(taskInfo.GetConfig().GetVolume().GetSizeMB())).
				Build()
			launchableTask.Volume = &hostsvc.Volume{
				Id:            taskInfo.GetRuntime().GetVolumeID(),
				ContainerPath: taskInfo.GetConfig().GetVolume().GetContainerPath(),
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
	checkVolume bool) error {
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
				context.Background(), _defaultSecretStoreTimeout)
			defer cancel()
			secret, err := l.secretStore.GetSecret(
				ctx, &peloton.SecretID{
					Value: string(volume.GetSource().GetSecret().GetValue().
						GetData()),
				},
			)
			if err != nil {
				l.metrics.TaskPopulateSecretFail.Inc(1)
				return err
			}
			volume.GetSource().GetSecret().GetValue().Data =
				secret.GetValue().GetData()
		}
	}
	return nil
}
