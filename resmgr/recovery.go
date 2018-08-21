package resmgr

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common/lifecycle"
	cmn_recovery "code.uber.internal/infra/peloton/common/recovery"
	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/storage"
	taskutil "code.uber.internal/infra/peloton/util/task"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	restoreMaintenaceQueueTimeout = 10 * time.Second
	hostmgrBackoffRetryInterval   = 100 * time.Millisecond
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
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	handler         *ServiceHandler
	config          Config
	hostmgrClient   hostsvc.InternalHostServiceYARPCClient
	nonRunningTasks []*resmgrsvc.EnqueueGangsRequest
	finished        chan bool //used for testing
	tracker         rmtask.Tracker
	lifecycle       lifecycle.LifeCycle // Lifecycle manager
}

// NewRecovery initializes the RecoveryHandler
func NewRecovery(
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	handler *ServiceHandler,
	config Config,
	hostmgrClient hostsvc.InternalHostServiceYARPCClient,
) *RecoveryHandler {
	return &RecoveryHandler{
		jobStore:      jobStore,
		taskStore:     taskStore,
		handler:       handler,
		metrics:       NewMetrics(parent),
		config:        config,
		hostmgrClient: hostmgrClient,
		tracker:       rmtask.GetTracker(),
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

	// Wait for recoveryHandler to be stopped
	r.lifecycle.Wait()

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

	err := cmn_recovery.RecoverJobsByState(
		ctx,
		r.jobStore,
		jobStates,
		r.requeueTasksInRange,
	)
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to recover running tasks")
		return err
	}
	log.Info("Recovery completed successfully for running tasks")

	// Restart draining process in background
	go r.restartDrainingProcess(ctx)

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
	jobID string, jobConfig *job.JobConfig, jobRuntime *job.RuntimeInfo,
	batch cmn_recovery.TasksBatch, errChan chan<- error) {
	nonRunningTasks, runningTasks, err := r.loadTasksInRange(ctx, jobID,
		batch.From, batch.To)

	if err != nil {
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

	resPool, err := respool.GetTree().Get(config.RespoolID)
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
			if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING ||
				taskInfo.GetRuntime().GetState() == task.TaskState_LAUNCHED {
				runningTasks = append(runningTasks, taskInfo)
			} else {
				nonRunningTasks = append(nonRunningTasks, taskInfo)
			}
		}
	}
	return nonRunningTasks, runningTasks, err
}

func (r *RecoveryHandler) restartDrainingProcess(ctx context.Context) {
	defer r.lifecycle.StopComplete()

	contextWithTimeout, cancel := context.WithTimeout(
		ctx,
		restoreMaintenaceQueueTimeout)
	defer cancel()
	ticker := time.NewTicker(time.Duration(hostmgrBackoffRetryInterval))
	defer ticker.Stop()
	log.Info("Recovery starting for maintenance queue")

	for {
		select {
		case <-r.lifecycle.StopCh():
			return
		case <-ticker.C:
			_, err := r.hostmgrClient.RestoreMaintenanceQueue(
				contextWithTimeout,
				&hostsvc.RestoreMaintenanceQueueRequest{})
			if err != nil {
				log.WithError(err).Warn("RestoreMaintenanceQueue call failed")
				continue
			}
			return
		}
	}
}
