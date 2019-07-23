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

package resmgr

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/models"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/common/lifecycle"
	cmn_recovery "github.com/uber/peloton/pkg/common/recovery"
	"github.com/uber/peloton/pkg/common/statemachine"
	taskutil "github.com/uber/peloton/pkg/common/util/task"
	"github.com/uber/peloton/pkg/resmgr/respool"
	rmtask "github.com/uber/peloton/pkg/resmgr/task"
	"github.com/uber/peloton/pkg/storage"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	hostmgrBackoffRetryInterval = 100 * time.Millisecond
)

var (
	// jobStates represents the job states which need recovery
	jobStates = []job.JobState{
		job.JobState_INITIALIZED,
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_UNKNOWN,
	}
	// taskStatesToSkip represents the task states which need to be skipped when doing recovery
	taskStatesToSkip = map[task.TaskState]bool{
		task.TaskState_SUCCEEDED:   true,
		task.TaskState_FAILED:      true,
		task.TaskState_KILLED:      true,
		task.TaskState_LOST:        true,
		task.TaskState_INITIALIZED: true,
	}
	// runningTaskStates are the task states which are 'running' on the underlying system (such as Mesos).
	runningTaskStates = map[task.TaskState]bool{
		task.TaskState_LAUNCHED:   true,
		task.TaskState_STARTING:   true,
		task.TaskState_RUNNING:    true,
		task.TaskState_KILLING:    true,
		task.TaskState_PREEMPTING: true,
	}
)

/*
RecoveryHandler performs recovery of jobs which are in non-terminated
states and re-queues the tasks in the pending queue.

This is performed in 2 phases when the resource manager gains leadership

Phase 1 - Performs recovery of all the *running* tasks by adding to the
task tracker so that the resource accounting can be done and transitions the
task state machine to the correct state.
Failure to perform recovery of any task in this phase results in the failure
of the whole recovery process and resource manager would fail to start up.
After successful completion of this phase the handler returns so that the
entitlement calculation can start and resource manager doesn't block anymore
incoming requests.

Phase 2 - This phase is performed in the background and involves recovery of
non-running tasks by the re-enqueueing them resource manager.
Failure in this phase is non-fatal.

Recovery of maintenance queue is performed
*/
type RecoveryHandler struct {
	metrics         *Metrics
	scope           tally.Scope
	taskStore       storage.TaskStore
	activeJobsOps   ormobjects.ActiveJobsOps
	jobConfigOps    ormobjects.JobConfigOps
	jobRuntimeOps   ormobjects.JobRuntimeOps
	handler         *ServiceHandler
	config          Config
	hostmgrClient   hostsvc.InternalHostServiceYARPCClient
	nonRunningTasks []*resmgrsvc.EnqueueGangsRequest
	tracker         rmtask.Tracker
	resTree         respool.Tree

	//used for testing
	finished chan bool

	// Lifecycle manager
	lifecycle lifecycle.LifeCycle
}

// NewRecovery initializes the RecoveryHandler
func NewRecovery(
	parent tally.Scope,
	taskStore storage.TaskStore,
	activeJobsOps ormobjects.ActiveJobsOps,
	jobConfigOps ormobjects.JobConfigOps,
	jobRuntimeOps ormobjects.JobRuntimeOps,
	handler *ServiceHandler,
	tree respool.Tree,
	config Config,
	hostmgrClient hostsvc.InternalHostServiceYARPCClient,
) *RecoveryHandler {
	return &RecoveryHandler{
		metrics:       NewMetrics(parent),
		scope:         parent,
		config:        config,
		taskStore:     taskStore,
		activeJobsOps: activeJobsOps,
		jobConfigOps:  jobConfigOps,
		jobRuntimeOps: jobRuntimeOps,
		handler:       handler,
		hostmgrClient: hostmgrClient,
		tracker:       rmtask.GetTracker(),
		resTree:       tree,
		finished:      make(chan bool),
		lifecycle:     lifecycle.NewLifeCycle(),
	}
}

// Stop stops the recovery handler
func (r *RecoveryHandler) Stop() error {
	if !r.lifecycle.Stop() {
		log.Warn("Recovery handler is already stopped, no" +
			" action will be performed")
		return nil
	}
	log.Info("Stopping recovery")

	return nil
}

// Start loads all the jobs and tasks which are not in terminal state
// and requeue them
func (r *RecoveryHandler) Start() error {
	if !r.lifecycle.Start() {
		log.Warn("Recovery handler is already started, no" +
			" action will be performed")
		return nil
	}

	ctx := context.Background()
	log.Info("Starting jobs recovery on startup")

	defer r.metrics.RecoveryTimer.Start().Stop()

	err := cmn_recovery.RecoverActiveJobs(
		ctx,
		r.scope,
		r.activeJobsOps,
		r.jobConfigOps,
		r.jobRuntimeOps,
		r.requeueTasksInRange,
	)
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to recover running tasks")
		return err
	}
	log.Info("Recovery completed successfully for running tasks")

	// We can start the recovery of non-running tasks now in the background
	r.finished = make(chan bool)
	go r.recoverNonRunningTasks()

	r.metrics.RecoverySuccess.Inc(1)
	return nil
}

// performs recovery of non-running tasks by enqueueing them in the resource
// manager handler
func (r *RecoveryHandler) recoverNonRunningTasks() {
	defer close(r.finished)
	log.Info("Recovery starting for non-running tasks")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	successTasks, failedTasks := 0, 0

	for _, nr := range r.nonRunningTasks {
		select {
		case <-r.lifecycle.StopCh():
			return
		default:
			resp, err := r.handler.EnqueueGangs(ctx, nr)
			if resp.GetError() != nil {
				if resp.GetError().GetFailure() != nil &&
					resp.GetError().GetFailure().GetFailed() != nil {
					for _, fail := range resp.GetError().GetFailure().GetFailed() {
						log.WithFields(log.Fields{
							"task_id ": fail.Task.Id.Value,
							"error":    fail.GetMessage(),
						}).Error("Failed to enqueue gang in recovery")
						failedTasks++
					}
				} else {
					log.WithFields(log.Fields{
						"gangs": nr.Gangs,
						"error": resp.GetError().String(),
					}).Error("Failed to enqueue gang in recovery")
					failedTasks += len(nr.Gangs)
				}
			}

			if err != nil {
				log.WithFields(log.Fields{
					"gangs": nr.Gangs,
					"error": err.Error(),
				}).Error("Failed to enqueue gang in recovery")
				failedTasks += len(nr.Gangs)
			}

			if err == nil && resp.GetError() == nil {
				successTasks += len(nr.Gangs)
			}
		}
	}

	r.metrics.RecoveryEnqueueSuccessCount.Inc(int64(successTasks))
	r.metrics.RecoveryEnqueueFailedCount.Inc(int64(failedTasks))
	log.Info("Recovery of non running tasks completed")
}

func (r *RecoveryHandler) requeueTasksInRange(ctx context.Context,
	jobID string, jobConfig *job.JobConfig, configAddOn *models.ConfigAddOn,
	jobRuntime *job.RuntimeInfo, batch cmn_recovery.TasksBatch, errChan chan<- error) {
	nonRunningTasks, runningTasks, err := r.loadTasksInRange(ctx, jobID,
		batch.From, batch.To)

	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			// Due to task_config table deprecation, we might see old jobs
			// fail to recover due to their task config was created in
			// task_config table instead of task_config_v2. Only log it
			// instead of error it out.
			log.WithError(err).
				WithField("non_running_count", len(nonRunningTasks)).
				WithField("running_count", len(runningTasks)).
				WithField("job_id", jobID).
				Error("Tasks fail to recover")
			return
		}
		errChan <- err
		return
	}
	log.WithField("non_running_count", len(nonRunningTasks)).
		WithField("running_count", len(runningTasks)).
		WithField("job_id", jobID).
		Info("Tasks to recover")

	r.addNonRunningTasks(nonRunningTasks, jobConfig)

	// enqueuing running tasks
	addedTasks, err := r.addRunningTasks(runningTasks, jobConfig)

	if err == nil {
		r.metrics.RecoveryRunningSuccessCount.Inc(int64(addedTasks))
	} else {
		r.metrics.RecoveryRunningFailCount.Inc(int64(len(runningTasks)) - int64(
			addedTasks))
		errChan <- err
		return
	}

	return
}

func (r *RecoveryHandler) addNonRunningTasks(notRunningTasks []*task.TaskInfo,
	jobConfig *job.JobConfig) {
	if len(notRunningTasks) == 0 {
		return
	}
	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   taskutil.ConvertToResMgrGangs(notRunningTasks, jobConfig),
		ResPool: jobConfig.RespoolID,
	}
	log.WithField("request", request).Debug("Adding non running tasks")
	r.nonRunningTasks = append(r.nonRunningTasks, request)
}

func (r *RecoveryHandler) addRunningTasks(
	tasks []*task.TaskInfo,
	config *job.JobConfig) (int, error) {

	runningTasksAdded := 0
	if len(tasks) == 0 {
		return runningTasksAdded, nil
	}

	resPool, err := r.resTree.Get(config.RespoolID)
	if err != nil {
		return runningTasksAdded, errors.Errorf("respool %s does not exist",
			config.RespoolID.Value)
	}

	for _, taskInfo := range tasks {
		err = r.addTaskToTracker(taskInfo, config, resPool)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"job_id":      taskInfo.JobId,
				"instance_id": taskInfo.InstanceId,
			}).Error("Failed to add to tracker")
			return runningTasksAdded, err
		}
		runningTasksAdded++
	}
	return runningTasksAdded, nil
}

func (r *RecoveryHandler) addTaskToTracker(
	taskInfo *task.TaskInfo,
	config *job.JobConfig,
	respool respool.ResPool) error {
	rmTask := taskutil.ConvertTaskToResMgrTask(taskInfo, config)
	err := r.tracker.AddTask(
		rmTask,
		r.handler.GetStreamHandler(),
		respool,
		r.config.RmTaskConfig)
	if err != nil {
		return errors.Wrap(err, "unable to add running task to tracker")
	}

	if err = r.tracker.AddResources(rmTask.Id); err != nil {
		return errors.Wrap(err, "could not add resources")
	}

	if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING {
		err = r.tracker.GetTask(rmTask.Id).
			TransitTo(task.TaskState_RUNNING.String(), statemachine.WithReason("task recovered into running state"))
	} else if taskInfo.GetRuntime().GetState() == task.TaskState_LAUNCHED {
		err = r.tracker.GetTask(rmTask.Id).
			TransitTo(task.TaskState_LAUNCHED.String(), statemachine.WithReason("task recovered into launched state"))
	}
	if err != nil {
		return errors.Wrap(err, "transition failed in task state machine")
	}
	return nil
}

func isTaskRunning(state task.TaskState) bool {
	_, ok := runningTaskStates[state]
	return ok
}

func (r *RecoveryHandler) loadTasksInRange(
	ctx context.Context,
	jobID string,
	from, to uint32) ([]*task.TaskInfo, []*task.TaskInfo, error) {

	log.WithFields(log.Fields{
		"job_id": jobID,
		"from":   from,
		"to":     to,
	}).Info("Checking job instance range")

	if from > to {
		return nil, nil, fmt.Errorf("invalid job instance range [%v, %v)",
			from, to)
	} else if from == to {
		return nil, nil, nil
	}

	pbJobID := &peloton.JobID{Value: jobID}
	var nonRunningTasks []*task.TaskInfo
	var runningTasks []*task.TaskInfo
	taskInfoMap, err := r.taskStore.GetTasksForJobByRange(
		ctx,
		pbJobID,
		&task.InstanceRange{
			From: from,
			To:   to,
		})
	for taskID, taskInfo := range taskInfoMap {
		if _, ok := taskStatesToSkip[taskInfo.GetRuntime().GetState()]; !ok {
			log.WithFields(log.Fields{
				"job_id":     jobID,
				"task_id":    taskID,
				"task_state": taskInfo.GetRuntime().GetState().String(),
			}).Debugf("found task for recovery")
			if isTaskRunning(taskInfo.GetRuntime().GetState()) {
				runningTasks = append(runningTasks, taskInfo)
			} else {
				nonRunningTasks = append(nonRunningTasks, taskInfo)
			}
		}
	}
	return nonRunningTasks, runningTasks, err
}
