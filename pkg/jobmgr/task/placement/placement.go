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
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/task/launcher"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"
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
// which dequeues placed tasks and sends them to host manager via task launcher
type Processor interface {
	// Start starts the placement processor goroutines
	Start() error
	// Stop stops the placement processor goroutines
	Stop() error
}

// launcher implements the Launcher interface
type processor struct {
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	jobFactory      cached.JobFactory
	goalStateDriver goalstate.Driver
	taskLauncher    launcher.Launcher
	lifeCycle       lifecycle.LifeCycle
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
)

// InitProcessor initializes placement processor
func InitProcessor(
	d *yarpc.Dispatcher,
	resMgrClientName string,
	jobFactory cached.JobFactory,
	goalStateDriver goalstate.Driver,
	taskLauncher launcher.Launcher,
	config *Config,
	parent tally.Scope,
) Processor {
	return &processor{
		resMgrClient:    resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(resMgrClientName)),
		jobFactory:      jobFactory,
		goalStateDriver: goalStateDriver,
		taskLauncher:    taskLauncher,
		config:          config,
		metrics:         NewMetrics(parent.SubScope("jobmgr").SubScope("task")),
		lifeCycle:       lifecycle.NewLifeCycle(),
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

func (p *processor) processPlacement(ctx context.Context, placement *resmgr.Placement) {
	var tasks []*mesos.TaskID
	for _, t := range placement.GetTaskIDs() {
		tasks = append(tasks, t.GetMesosTaskID())
	}

	launchableTasks, skippedTasks, err := p.taskLauncher.GetLaunchableTasks(
		ctx,
		tasks,
		placement.GetHostname(),
		placement.GetAgentId(),
		placement.GetPorts(),
	)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"placement":   placement,
				"tasks_total": len(tasks),
			}).Error("Failed to get launchable tasks")

		err = p.taskLauncher.TryReturnOffers(ctx, err, placement)
		if err != nil {
			log.WithError(err).
				WithField("placement", placement).
				Error("Failed to return offers for placement")
		}
		return
	}

	launchableTaskInfos, tasksToSkip := p.createTaskInfos(ctx, launchableTasks)
	skippedTasks = append(skippedTasks, tasksToSkip...)

	if len(launchableTaskInfos) > 0 {

		skippedTaskInfos, err := p.taskLauncher.Launch(
			ctx,
			launchableTaskInfos,
			placement,
		)
		if err != nil {
			p.processSkippedLaunches(ctx, launchableTaskInfos)
			return
		}
		p.processSkippedLaunches(ctx, skippedTaskInfos)
		// Finally, enqueue tasks into goalstate
		p.enqueueTaskToGoalState(launchableTaskInfos)
	}

	// Kill skipped/unknown tasks. We ignore errors because that would indicate
	// a channel/network error. We will retry when we can reconnect to
	// resource-manager
	p.KillResManagerTasks(ctx, skippedTasks)
}

// processSkippedLaunches tries to kill the tasks in resmgr and
// if the kill goes through enqueue the task into resmgr
func (p *processor) processSkippedLaunches(
	ctx context.Context,
	taskInfoMap map[string]*launcher.LaunchableTaskInfo,
) {
	var skippedTaskIDs []*peloton.TaskID
	for id := range taskInfoMap {
		skippedTaskIDs = append(skippedTaskIDs, &peloton.TaskID{Value: id})
	}
	// kill and enqueue skipped tasks back to resmgr to launch again, instead of
	// waiting for resmgr timeout
	if err := p.KillResManagerTasks(ctx, skippedTaskIDs); err == nil {
		p.enqueueTasksToResMgr(ctx, taskInfoMap)
	}
}

func (p *processor) enqueueTaskToGoalState(taskInfos map[string]*launcher.LaunchableTaskInfo) {
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

func (p *processor) createTaskInfos(
	ctx context.Context,
	lauchableTasks map[string]*launcher.LaunchableTask,
) (map[string]*launcher.LaunchableTaskInfo, []*peloton.TaskID) {
	taskInfos := make(map[string]*launcher.LaunchableTaskInfo)
	skippedTasks := make([]*peloton.TaskID, 0)
	for taskID, launchableTask := range lauchableTasks {
		id, instanceID, err := util.ParseTaskID(taskID)
		if err != nil {
			continue
		}
		jobID := &peloton.JobID{Value: id}
		cachedJob := p.jobFactory.AddJob(jobID)
		cachedTask, err := cachedJob.AddTask(ctx, uint32(instanceID))
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("cannot add and recover task")
			continue
		}

		cachedRuntime, err := cachedTask.GetRuntime(ctx)
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("cannot fetch task runtime")
			continue
		}

		if cachedRuntime.GetGoalState() == task.TaskState_KILLED {
			if cachedRuntime.GetState() != task.TaskState_KILLED {
				// Received placement for task which needs to be killed, retry killing the task.
				p.goalStateDriver.EnqueueTask(jobID, uint32(instanceID), time.Now())
			}

			// Skip launching of deleted tasks.
			skippedTasks = append(skippedTasks, &peloton.TaskID{Value: taskID})
			log.WithField("task_id", taskID).Info("skipping launch of killed task")
			continue
		}
		retry := 0
		for retry < maxRetryCount {
			_, _, err := cachedJob.PatchTasks(
				ctx,
				map[uint32]jobmgrcommon.RuntimeDiff{
					uint32(instanceID): launchableTask.RuntimeDiff,
				},
				false,
			)
			if err == nil {
				runtime, _ := cachedTask.GetRuntime(ctx)
				taskInfos[taskID] = &launcher.LaunchableTaskInfo{
					TaskInfo: &task.TaskInfo{
						Runtime:    runtime,
						Config:     launchableTask.Config,
						InstanceId: uint32(instanceID),
						JobId:      jobID,
					},
					ConfigAddOn: launchableTask.ConfigAddOn,
					Spec:        launchableTask.Spec,
				}
				break
			}

			if common.IsTransientError(err) {
				// TBD add a max retry to bail out after a few retries.
				log.WithError(err).WithFields(log.Fields{
					"job_id":      id,
					"instance_id": instanceID,
				}).Warn("retrying update task runtime on transient error")
				retry++
				continue
			}

			log.WithError(err).
				WithFields(log.Fields{
					"job_id":      id,
					"instance_id": instanceID,
				}).Error("cannot process placement due to non-transient db error")
			delete(taskInfos, taskID)
			break
		}
	}
	return taskInfos, skippedTasks
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
func (p *processor) enqueueTasksToResMgr(ctx context.Context, tasks map[string]*launcher.LaunchableTaskInfo) (err error) {
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
