package resmgr

import (
	"context"
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	cmn_recovery "code.uber.internal/infra/peloton/common/recovery"
	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"
	rmtask "code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

var (
	once     sync.Once
	recovery *recoveryHandler

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

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

/*
recoveryHandler performs recovery of jobs which are in non-terminated
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
*/
type recoveryHandler struct {
	sync.Mutex
	metrics         *Metrics
	jobStore        storage.JobStore
	taskStore       storage.TaskStore
	handler         *ServiceHandler
	config          Config
	nonRunningTasks []*resmgrsvc.EnqueueGangsRequest
	finished        chan bool //used for testing
}

// InitRecovery initializes the recoveryHandler
func InitRecovery(
	parent tally.Scope,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	handler *ServiceHandler,
	config Config,
) {
	once.Do(func() {
		recovery = &recoveryHandler{
			jobStore:  jobStore,
			taskStore: taskStore,
			handler:   handler,
			metrics:   NewMetrics(parent),
			config:    config,
			finished:  make(chan bool),
		}
	})
}

// GetRecoveryHandler returns the recovery handler
func GetRecoveryHandler() RecoveryHandler {
	if recovery == nil {
		log.Fatal(
			"Recovery handler is not initialized",
		)
	}
	return recovery
}

// Stop is a no-op for recovery handler
func (r *recoveryHandler) Stop() error {
	//no-op
	log.Info("Stopping recovery")
	return nil
}

// Start loads all the jobs and tasks which are not in terminal state
// and requeue them
func (r *recoveryHandler) Start() error {
	r.Lock()
	defer r.Unlock()

	ctx := context.Background()
	log.Info("Starting jobs recovery on startup")

	defer r.metrics.RecoveryTimer.Start().Stop()

	err := cmn_recovery.RecoverJobsByState(ctx, r.jobStore, jobStates, r.requeueTasksInRange)
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to recover running tasks")
		return err
	}

	log.Info("Recovery completed successfully for running tasks")
	r.metrics.RecoverySuccess.Inc(1)

	// We can start the recovery of non-running tasks now in the background
	log.Info("Recovery starting for non-running tasks")
	go r.recoverNonRunningTasks()

	return nil
}

// performs recovery of non-running tasks by enqueueing them in the resource
// manager handler
func (r *recoveryHandler) recoverNonRunningTasks() {
	defer close(r.finished)

	ctx := context.Background()
	successTasks, failedTasks := 0, 0
	for _, nr := range r.nonRunningTasks {
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

	r.metrics.RecoveryEnqueueSuccessCount.Inc(int64(successTasks))
	r.metrics.RecoveryEnqueueFailedCount.Inc(int64(failedTasks))
	log.Info("Recovery of non running tasks completed")
}

func (r *recoveryHandler) requeueTasksInRange(ctx context.Context,
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

func (r *recoveryHandler) addNonRunningTasks(notRunningTasks []*task.TaskInfo,
	jobConfig *job.JobConfig) {
	if len(notRunningTasks) == 0 {
		return
	}
	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   util.ConvertToResMgrGangs(notRunningTasks, jobConfig.GetSla()),
		ResPool: jobConfig.RespoolID,
	}
	log.WithField("request", request).Debug("Adding non running tasks")
	r.nonRunningTasks = append(r.nonRunningTasks, request)
}

func (r *recoveryHandler) addRunningTasks(
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

func (r *recoveryHandler) addTaskToTracker(
	taskInfo *task.TaskInfo,
	config *job.JobConfig,
	respool respool.ResPool) error {
	rmTask := util.ConvertTaskToResMgrTask(taskInfo, config.GetSla())
	err := rmtask.GetTracker().AddTask(
		rmTask,
		r.handler.GetStreamHandler(),
		respool,
		r.config.RmTaskConfig)
	if err != nil {
		return errors.Wrap(err, "unable to add running task to tracker")
	}

	if err = rmtask.GetTracker().AddResources(rmTask.Id); err != nil {
		return errors.Wrap(err, "could not add resources")
	}

	if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING {
		err = rmtask.GetTracker().GetTask(rmTask.Id).
			TransitTo(task.TaskState_RUNNING.String(), statemachine.WithReason("task recovered into running state"))
	} else if taskInfo.GetRuntime().GetState() == task.TaskState_LAUNCHED {
		err = rmtask.GetTracker().GetTask(rmTask.Id).
			TransitTo(task.TaskState_LAUNCHED.String(), statemachine.WithReason("task recovered into launched state"))
	}
	if err != nil {
		return errors.Wrap(err, "transition failed in task state machine")
	}
	return nil
}

func (r *recoveryHandler) loadTasksInRange(
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
