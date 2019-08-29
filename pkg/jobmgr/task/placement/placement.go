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

package placement

import (
	"context"
	"encoding/base64"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
)

// Config is placement processor specific config
type Config struct {
	// PlacementDequeueLimit is the limit which placement processor get the
	// placements
	PlacementDequeueLimit int `yaml:"placement_dequeue_limit"`

	// GetPlacementsTimeout is the timeout value for placement processor to
	// call GetPlacements
	GetPlacementsTimeout int `yaml:"get_placements_timeout_ms"`
}

// Processor defines the interface of placement processor
// which dequeues placed tasks and sends them to host manager via lifecyclemgr.
type Processor interface {
	// Start starts the placement processor goroutines
	Start() error
	// Stop stops the placement processor goroutines
	Stop() error
}

// processor implements the task placement Processor interface.
type processor struct {
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	taskConfigV2Ops ormobjects.TaskConfigV2Ops
	secretInfoOps   ormobjects.SecretInfoOps
	lifeCycle       lifecycle.LifeCycle
	hmVersion       api.Version
	lm              lifecyclemgr.Manager
	config          *Config
	metrics         *Metrics
}

const (
	// Time out for the function to time out
	_rpcTimeout = 10 * time.Second
	// maxRetryCount is the maximum number of times a transient error from the DB will be retried.
	// This is a safety mechanism to avoid placement engine getting stuck in
	// retrying errors wrongly classified as transient errors.
	maxRetryCount = 1000
	// Default secret operations cassandra timeout.
	_defaultSecretInfoOpsTimeout = 10 * time.Second
)

// InitProcessor initializes placement processor
func InitProcessor(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	hmVersion api.Version,
	ormStore *ormobjects.Store,
	config *Config,
	parent tally.Scope,
) Processor {
	return &processor{
		resMgrClient:    resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		lm:              lifecyclemgr.New(hmVersion, d, parent),
		taskConfigV2Ops: ormobjects.NewTaskConfigV2Ops(ormStore),
		secretInfoOps:   ormobjects.NewSecretInfoOps(ormStore),
		config:          config,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		lifeCycle:       lifecycle.NewLifeCycle(),
		hmVersion:       hmVersion,
	}
}

// Start starts Processor
func (p *processor) Start() error {
	if !p.lifeCycle.Start() {
		// already running
		log.Warn("placement processor is already running, no action will be performed")
		return nil
	}
	log.Info("starting placement processor")

	go p.run()

	log.Info("placement processor started")
	return nil
}

func (p *processor) run() {
	for {
		select {
		case <-p.lifeCycle.StopCh():
			p.lifeCycle.StopComplete()
			return
		default:
			p.process()
		}
	}
}

func (p *processor) process() {
	placements, err := p.getPlacements()
	if err != nil {
		log.WithError(err).Error("jobmgr failed to dequeue placements")
		return
	}

	select {
	case <-p.lifeCycle.StopCh():
		// placement dequeued but not processed
		log.WithField("placements", placements).
			Warn("ignoring placement after dequeue due to lost leadership")
		return
	default:
		break
	}

	if len(placements) == 0 {
		// log a debug to make it not verbose
		log.Debug("No placements")
		return
	}

	ctx := context.Background()

	// Getting and launching placements in different go routine
	log.WithField("placements", placements).Debug("Start processing placements")
	for _, placement := range placements {
		go p.processPlacement(ctx, placement)
	}
}

// prepareTasksForLaunch returns current task configuration and
// the runtime diff which needs to be patched onto existing runtime for
// each launchable task. It returns error only when the placement contains
// less than required selected ports.
func (p *processor) prepareTasksForLaunch(
	ctx context.Context,
	taskIDs []*mesos.TaskID,
	hostname string,
	agentID string,
	selectedPorts []uint32,
) (
	map[string]*lifecyclemgr.LaunchableTaskInfo,
	[]*peloton.TaskID,
	error,
) {
	portsIndex := 0
	taskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo)
	skippedTaskIDs := make([]*peloton.TaskID, 0)

	for _, mtaskID := range taskIDs {
		id, instanceID, err := util.ParseJobAndInstanceID(mtaskID.GetValue())
		if err != nil {
			log.WithField("mesos_task_id", mtaskID.GetValue()).
				WithError(err).
				Error("Failed to parse mesos task id")
			continue
		}

		jobID := &peloton.JobID{Value: id}
		ptaskID := &peloton.TaskID{
			Value: util.CreatePelotonTaskID(id, instanceID),
		}
		ptaskIDStr := ptaskID.GetValue()

		cachedJob := p.jobFactory.GetJob(jobID)
		if cachedJob == nil {
			skippedTaskIDs = append(skippedTaskIDs, ptaskID)
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

		if cachedRuntime.GetMesosTaskId().GetValue() != mtaskID.GetValue() {
			log.WithFields(log.Fields{
				"job_id":        jobID.GetValue(),
				"instance_id":   uint32(instanceID),
				"mesos_task_id": mtaskID.GetValue(),
			}).Info("skipping launch of old run")
			skippedTaskIDs = append(skippedTaskIDs, ptaskID)
			continue
		}

		taskConfig, configAddOn, err := p.taskConfigV2Ops.GetTaskConfig(
			ctx,
			jobID,
			uint32(instanceID),
			cachedRuntime.GetConfigVersion())
		if err != nil {
			log.WithError(err).
				WithField("task_id", ptaskID.GetValue()).
				Error("not able to get task configuration")
			continue
		}

		var spec *pbpod.PodSpec
		if p.hmVersion.IsV1() {
			// TODO: unify this call with p.taskConfigV2Ops.GetTaskConfig().
			spec, err = p.taskConfigV2Ops.GetPodSpec(
				ctx,
				jobID,
				uint32(instanceID),
				cachedRuntime.GetConfigVersion())
			if err != nil {
				log.WithError(err).
					WithField("task_id", ptaskIDStr).
					Error("not able to get pod spec")
				continue
			}
		}

		runtimeDiff := make(jobmgrcommon.RuntimeDiff)
		if cachedRuntime.GetGoalState() != task.TaskState_KILLED {
			runtimeDiff[jobmgrcommon.HostField] = hostname
			runtimeDiff[jobmgrcommon.AgentIDField] = &mesos.AgentID{
				Value: &agentID,
			}
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
						"task_id":        ptaskIDStr,
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

		if cachedRuntime.GetGoalState() == task.TaskState_KILLED {
			if cachedRuntime.GetState() != task.TaskState_KILLED {
				// Received placement for task which needs to be killed,
				// retry killing the task.
				p.goalStateDriver.EnqueueTask(
					jobID,
					uint32(instanceID),
					time.Now(),
				)
			}
			// Skip launching of killed tasks.
			skippedTaskIDs = append(skippedTaskIDs, ptaskID)
			log.WithField("task_id", ptaskIDStr).
				Info("skipping launch of killed task")
			continue
		}

		// Patch task runtime with the generated runtime diff.
		retry := 0
		for retry < maxRetryCount {
			_, _, err := cachedJob.PatchTasks(
				ctx,
				map[uint32]jobmgrcommon.RuntimeDiff{
					uint32(instanceID): runtimeDiff,
				},
				false,
			)
			if err == nil {
				runtime, _ := cachedTask.GetRuntime(ctx)
				taskInfos[ptaskIDStr] = &lifecyclemgr.LaunchableTaskInfo{
					TaskInfo: &task.TaskInfo{
						Runtime:    runtime,
						Config:     taskConfig,
						InstanceId: uint32(instanceID),
						JobId:      jobID,
					},
					ConfigAddOn: configAddOn,
					Spec:        spec,
				}
				break
			}

			if common.IsTransientError(err) {
				// TBD add a max retry to bail out after a few retries.
				log.WithError(err).WithFields(log.Fields{
					"job_id":      jobID,
					"instance_id": instanceID,
				}).Warn("retrying update task runtime on transient error")
				retry++
				continue
			}

			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      jobID,
					"instance_id": instanceID,
				}).Error("cannot process placement due to non-transient db error")
			delete(taskInfos, ptaskIDStr)
			break
		}
	}

	return taskInfos, skippedTaskIDs, nil
}

func (p *processor) processPlacement(
	ctx context.Context,
	placement *resmgr.Placement,
) {
	var taskIDs []*mesos.TaskID
	for _, t := range placement.GetTaskIDs() {
		taskIDs = append(taskIDs, t.GetMesosTaskID())
	}

	launchableTaskInfos, skippedTaskIDs, err := p.prepareTasksForLaunch(
		ctx,
		taskIDs,
		placement.GetHostname(),
		placement.GetAgentId().GetValue(),
		placement.GetPorts(),
	)
	if err != nil {
		if newErr := p.lm.TerminateLease(
			ctx,
			placement.GetHostname(),
			placement.GetAgentId().GetValue(),
			placement.GetHostOfferID().GetValue(),
		); newErr != nil {
			err = errors.Wrap(err, newErr.Error())
		}
		// We do not return error, so it should be logged here.
		log.WithError(err).
			WithFields(log.Fields{
				"placement":   placement,
				"tasks_total": len(taskIDs),
			}).Error("failed to get launchable tasks")
		p.metrics.TaskRequeuedOnLaunchFail.Inc(int64(len(taskIDs)))
		return
	}

	// Populate secrets in place in task config of launchableTaskInfos.
	skippedTaskInfos := p.populateSecrets(ctx, launchableTaskInfos)
	if len(skippedTaskInfos) != 0 {
		// Process the task launches that were skipped due to transient DB
		// errors when fetching secrets. Continue with the rest of the launches.
		p.processSkippedLaunches(ctx, skippedTaskInfos)
	}

	err = p.lm.Launch(
		ctx,
		placement.GetHostOfferID().GetValue(),
		placement.GetHostname(),
		placement.GetAgentId().GetValue(),
		launchableTaskInfos,
		nil,
	)
	if err != nil {
		// We do not return error, so it should be logged here.
		log.WithError(err).
			WithFields(log.Fields{
				"placement":   placement,
				"tasks_total": len(launchableTaskInfos),
			}).Error("launch error, process skipped launches")
		p.processSkippedLaunches(ctx, launchableTaskInfos)
		return
	}
	p.enqueueTaskToGoalState(launchableTaskInfos)

	// Kill skipped/unknown tasks. We ignore errors because that would indicate
	// a channel/network error. We will retry when we can reconnect to
	// resource-manager.
	p.KillResManagerTasks(ctx, skippedTaskIDs)
}

// populateSecrets populates the eligible tasks with secret data.
// For the tasks which have transient errors when fetching the
// secret data from DB, it returns them as skipped.
func (p *processor) populateSecrets(
	ctx context.Context,
	taskInfos map[string]*lifecyclemgr.LaunchableTaskInfo,
) map[string]*lifecyclemgr.LaunchableTaskInfo {
	skippedTaskInfos := make(map[string]*lifecyclemgr.LaunchableTaskInfo)
	for id, taskInfo := range taskInfos {
		// If task config has secret volumes, populate secret data in config.
		err := p.populateTaskConfigWithSecrets(ctx, taskInfo.Config)
		if err != nil {
			if yarpcerrors.IsNotFound(errors.Cause(err)) {
				// This is not retryable and we will never recover
				// from this error. Mark the task runtime as KILLED
				// before dropping it so that we don't try to launch it
				// again. No need to enqueue to goalstate engine here.
				// The caller does that for all tasks in TaskInfo.

				// TODO: Notify resmgr that the state of this task
				// is failed and it should not retry this task
				// Need a private resmgr API for this.
				if err = p.updateTaskRuntime(
					ctx,
					id,
					task.TaskState_KILLED,
					"REASON_SECRET_NOT_FOUND",
					err.Error(),
				); err != nil {
					// Not retrying here, worst case we will attempt to launch
					// this task again from ProcessPlacement() call, and mark
					// goalstate properly in the next iteration.
					log.WithError(err).WithField("task_id", id).
						Error("failed to update goalstate to KILLED")
				}
			} else {
				// Skip this task in case of transient error but add it to
				// skippedTaskInfos so that the caller can ask resmgr to
				// launch this task again.
				log.WithError(err).
					WithField("task_id", id).
					Error("populateSecrets failed. skipping task")
				skippedTaskInfos[id] = taskInfo
			}
		}
	}
	return skippedTaskInfos
}

// populateTaskConfigWithSecrets checks task config for secret volumes.
// If the config has volumes of type secret, it means that the Value field
// of that secret contains the secret ID. This function queries
// the DB to fetch the secret by secret ID and then replaces
// the secret Value by the fetched secret data.
// We do this to prevent secrets from being leaked as a part
// of job or task config and populate the task config with
// actual secrets just before task launch.
func (p *processor) populateTaskConfigWithSecrets(
	ctx context.Context,
	taskConfig *task.TaskConfig,
) error {
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
			// (for example as part of job/task config).
			ctx, cancel := context.WithTimeout(
				context.Background(), _defaultSecretInfoOpsTimeout)
			defer cancel()

			secretID := string(
				volume.GetSource().GetSecret().GetValue().GetData())
			secretInfoObj, err := p.secretInfoOps.GetSecret(
				ctx,
				secretID,
			)

			if err != nil {
				p.metrics.TaskPopulateSecretFail.Inc(1)
				return err
			}
			secretStr, err := base64.StdEncoding.DecodeString(
				secretInfoObj.Data,
			)
			if err != nil {
				p.metrics.TaskPopulateSecretFail.Inc(1)
				return err
			}
			volume.GetSource().GetSecret().GetValue().Data =
				[]byte(secretStr)
		}
	}
	return nil
}

// updateTaskRuntime updates task runtime with goalstate, reason and message
// for the given task id.
func (p *processor) updateTaskRuntime(
	ctx context.Context,
	taskID string,
	goalstate task.TaskState,
	reason string,
	message string,
) error {
	runtimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField: goalstate,
		jobmgrcommon.ReasonField:    reason,
		jobmgrcommon.MessageField:   message,
	}
	jobID, instanceID, err := util.ParseTaskID(taskID)
	if err != nil {
		return err
	}
	cachedJob := p.jobFactory.GetJob(&peloton.JobID{Value: jobID})
	if cachedJob == nil {
		return yarpcerrors.InternalErrorf("jobID %v not found in cache", jobID)
	}
	// Update the task in DB and cache, and then schedule to goalstate.
	_, _, err = cachedJob.PatchTasks(
		ctx,
		map[uint32]jobmgrcommon.RuntimeDiff{uint32(instanceID): runtimeDiff},
		false,
	)
	if err != nil {
		return err
	}
	return nil
}

// processSkippedLaunches tries to kill the tasks in resmgr and
// if the kill goes through enqueue the task into resmgr.
func (p *processor) processSkippedLaunches(
	ctx context.Context,
	taskInfoMap map[string]*lifecyclemgr.LaunchableTaskInfo,
) {
	var skippedTaskIDs []*peloton.TaskID
	for id := range taskInfoMap {
		skippedTaskIDs = append(skippedTaskIDs, &peloton.TaskID{Value: id})
	}
	// Kill and enqueue skipped tasks back to resmgr to launch again, instead of
	// waiting for resmgr timeout.
	if err := p.KillResManagerTasks(ctx, skippedTaskIDs); err == nil {
		p.enqueueTasksToResMgr(ctx, taskInfoMap)
	}
}

func (p *processor) enqueueTaskToGoalState(
	taskInfos map[string]*lifecyclemgr.LaunchableTaskInfo,
) {
	for id := range taskInfos {
		jobID, instanceID, err := util.ParseTaskID(id)
		if err != nil {
			log.WithError(err).
				WithField("task_id", id).
				Error("failed to parse the task id in placement processor")
			continue
		}

		p.goalStateDriver.EnqueueTask(
			&peloton.JobID{Value: jobID},
			uint32(instanceID),
			time.Now())

		cachedJob := p.jobFactory.AddJob(&peloton.JobID{Value: jobID})
		goalstate.EnqueueJobWithDefaultDelay(
			&peloton.JobID{Value: jobID},
			p.goalStateDriver,
			cachedJob)
	}
}

func (p *processor) getPlacements() ([]*resmgr.Placement, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), _rpcTimeout)
	defer cancelFunc()

	request := &resmgrsvc.GetPlacementsRequest{
		Limit:   uint32(p.config.PlacementDequeueLimit),
		Timeout: uint32(p.config.GetPlacementsTimeout),
	}

	callStart := time.Now()
	response, err := p.resMgrClient.GetPlacements(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		p.metrics.GetPlacementFail.Inc(1)
		return nil, err
	}

	if response.GetError() != nil {
		p.metrics.GetPlacementFail.Inc(1)
		return nil, errors.New(response.GetError().String())
	}

	if len(response.GetPlacements()) != 0 {
		log.WithFields(log.Fields{
			"num_placements": len(response.Placements),
			"duration":       callDuration.Seconds(),
		}).Info("GetPlacements")
	}

	// TODO: turn getplacement metric into gauge so we can
	//       get the current get_placements counts
	p.metrics.GetPlacement.Inc(int64(len(response.GetPlacements())))
	p.metrics.GetPlacementsCallDuration.Record(callDuration)
	return response.GetPlacements(), nil
}

// enqueueTask enqueues given task to resmgr to launch again.
func (p *processor) enqueueTasksToResMgr(
	ctx context.Context,
	tasks map[string]*lifecyclemgr.LaunchableTaskInfo,
) (err error) {
	defer func() {
		if err == nil {
			return
		}
		var taskIDs []string
		for taskID := range tasks {
			taskIDs = append(taskIDs, taskID)
		}
		log.WithError(err).WithFields(log.Fields{
			"task_ids":    taskIDs,
			"tasks_total": len(tasks),
		}).Error("failed to enqueue tasks to resmgr")
	}()

	if len(tasks) == 0 {
		return nil
	}

	for _, t := range tasks {
		healthState := taskutil.GetInitialHealthState(t.GetConfig())
		runtimeDiff := taskutil.RegenerateMesosTaskIDDiff(
			t.JobId, t.InstanceId, t.GetRuntime(), healthState)
		runtimeDiff[jobmgrcommon.MessageField] = "Regenerate placement"
		runtimeDiff[jobmgrcommon.ReasonField] = "REASON_HOST_REJECT_OFFER"
		retry := 0
		for retry < maxRetryCount {
			cachedJob := p.jobFactory.AddJob(t.JobId)
			_, _, err = cachedJob.PatchTasks(
				ctx,
				map[uint32]jobmgrcommon.RuntimeDiff{uint32(t.InstanceId): runtimeDiff},
				false,
			)
			if err == nil {
				p.goalStateDriver.EnqueueTask(t.JobId, t.InstanceId, time.Now())
				goalstate.EnqueueJobWithDefaultDelay(
					t.JobId, p.goalStateDriver, cachedJob)
				break
			}
			if common.IsTransientError(err) {
				log.WithError(err).WithFields(log.Fields{
					"job_id":      t.JobId,
					"instance_id": t.InstanceId,
				}).Warn("retrying update task runtime on transient error")
			} else {
				return err
			}
			retry++
		}
	}
	return err
}

// Stop stops placement processor
func (p *processor) Stop() error {
	if !(p.lifeCycle.Stop()) {
		log.Warn("placement processor is already stopped, no action will be performed")
		return nil
	}

	p.lifeCycle.Wait()
	log.Info("placement processor stopped")
	return nil
}

// KillResManagerTasks issues a kill request to resource-manager for
// specified tasks
func (p *processor) KillResManagerTasks(ctx context.Context,
	tasks []*peloton.TaskID) error {
	if len(tasks) == 0 {
		return nil
	}
	req := &resmgrsvc.KillTasksRequest{Tasks: tasks}
	ctx, cancelFunc := context.WithTimeout(ctx, _rpcTimeout)
	defer cancelFunc()
	resp, err := p.resMgrClient.KillTasks(ctx, req)
	if err != nil {
		log.WithError(err).WithField("task_list", tasks).
			Error("placement: resource-manager KillTasks failed")
	} else {
		log.WithField("task_list", tasks).
			Info("placement: killed resource-manager tasks")
		if len(resp.Error) > 0 {
			// Not really much we can do about these errors, just log them
			log.WithField("errors", resp.Error).
				Error("placement: resource-manager KillTasks errors")
		}
	}
	return err
}
