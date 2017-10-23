package resmgr

import (
	"context"
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

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
		job.JobState_PENDING,
		job.JobState_RUNNING,
		job.JobState_UNKNOWN,
		job.JobState_INITIALIZED,
	}
	// taskStatesToSkip represents the task states which need to be skipped when doing recovery
	taskStatesToSkip = map[task.TaskState]bool{
		task.TaskState_SUCCEEDED: true,
		task.TaskState_FAILED:    true,
		task.TaskState_KILLED:    true,
		task.TaskState_LOST:      true,
	}
)

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

// recoveryHandler performs recovery of jobs which are in non-terminated
// states and requeues the tasks in the pending queue.
// This is performed when the resource manager gains leadership
type recoveryHandler struct {
	sync.Mutex
	metrics   *Metrics
	jobStore  storage.JobStore
	taskStore storage.TaskStore
	handler   *ServiceHandler
	config    Config
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
	log.Infof("Stopping recovery")
	return nil
}

// Start loads all the jobs and tasks which are not in terminal state
// and requeue them
func (r *recoveryHandler) Start() error {
	r.Lock()
	defer r.Unlock()

	ctx := context.Background()
	log.Info("Starting jobs recovery on startup")
	jobsIDs, err := r.jobStore.GetJobsByStates(ctx, jobStates)
	if err != nil {
		r.metrics.RecoveryFail.Inc(1)
		log.WithError(err).Error("failed to read job IDs for recovery")
		return err
	}
	log.WithField("jobsIDs :", jobsIDs).Info("Resource Manager loading" +
		" jobsIDs from DB for recoveryHandler")
	var jobstring string
	var isError = false
	for _, jobID := range jobsIDs {
		info, err := r.jobStore.GetJob(ctx, &jobID)
		if err != nil {
			r.metrics.RecoveryFail.Inc(1)
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Error("failed to get job")
			return err
		}
		err = r.requeueJob(ctx, jobID.Value, info.Config)
		if err != nil {
			log.WithError(err).WithField("Not able to requeue jobStates ", jobID).Error()
			jobstring = jobstring + jobID.Value + " , "
			isError = true
		}
	}
	if isError {
		r.metrics.RecoveryFail.Inc(1)
		return errors.Errorf("failed to requeue jobsIDs %s", jobstring)
	}
	log.Info("Job recoveryHandler complete")
	r.metrics.RecoverySuccess.Inc(1)
	return nil
}

// Scans the tasks batch by batch, update / create task
// infos and put those task records into the queue. Even if some
// instances fail then recovery continues.
func (r *recoveryHandler) requeueJob(
	ctx context.Context,
	jobID string,
	jobConfig *job.JobConfig) error {

	// TODO optimize this function
	numSingleInstances := jobConfig.InstanceCount
	initialSingleInstance := uint32(0)
	var errString string
	var isError = false

	// Handle gang scheduled tasks if any
	minInstances := jobConfig.GetSla().GetMinimumRunningInstances()
	if minInstances > 1 {
		count, err := r.requeueTasksInRange(ctx, jobID, jobConfig, 0, minInstances)
		if err != nil {
			r.metrics.RecoveryFailCount.Inc(int64(count))
			log.WithFields(log.Fields{
				"JobID": jobID,
				"from":  0,
				"to":    minInstances}).Error("Failed to requeue gang tasks for")
			errString = errString + fmt.Sprintf("[ job %v in [%v, %v) ]", jobID, 0, minInstances)
			isError = true
		} else {
			r.metrics.RecoverySuccessCount.Inc(int64(count))
		}
		numSingleInstances -= minInstances
		initialSingleInstance += minInstances
	}

	// Handle singleton tasks
	if numSingleInstances > 0 {
		rangevar := numSingleInstances / RequeueBatchSize
		for i := initialSingleInstance; i <= rangevar; i++ {
			from := i * RequeueBatchSize
			to := util.Min((i+1)*RequeueBatchSize, numSingleInstances)
			count, err := r.requeueTasksInRange(ctx, jobID, jobConfig, from, to)
			if err != nil {
				r.metrics.RecoveryFailCount.Inc(int64(count))
				log.WithFields(log.Fields{
					"JobID": jobID,
					"from":  from,
					"to":    to}).Error("Failed to requeue tasks for")
				errString = errString + fmt.Sprintf("[ job %v in [%v, %v) ]",
					jobID, from, to)
				isError = true
			} else {
				r.metrics.RecoverySuccessCount.Inc(int64(count))
			}
		}
	}

	if isError {
		return errors.Errorf("Not able to requeue tasks %s", errString)
	}
	return nil
}

// returns the number of tasks which were attempted to reqeueue
func (r *recoveryHandler) requeueTasksInRange(
	ctx context.Context,
	jobID string,
	jobConfig *job.JobConfig,
	from, to uint32) (int, error) {

	notRunningTasks, runningTasks, err := r.loadTasksInRange(ctx, jobID, from, to)
	if err != nil {
		return len(notRunningTasks) + len(runningTasks), err
	}
	log.WithField("Count", len(notRunningTasks)).
		WithField("jobID", jobID).
		Debug("Tasks count to recover")
	resmgrTasks := util.ConvertToResMgrGangs(notRunningTasks, jobConfig.GetSla())

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   resmgrTasks,
		ResPool: jobConfig.RespoolID,
	}
	resp, err := r.handler.EnqueueGangs(ctx, request)
	if resp.GetError() != nil {
		return len(notRunningTasks), errors.Errorf("failed to requeue tasks %s",
			getEnqueueGangErrorMessage(resp.GetError()))
	}

	// enqueuing running tasks
	runningTasksLen, err := r.addRunningTasks(runningTasks, jobConfig)

	return len(notRunningTasks) + runningTasksLen, err
}

func (r *recoveryHandler) addRunningTasks(
	tasks []*task.TaskInfo,
	config *job.JobConfig) (int, error) {

	respool, err := respool.GetTree().Get(config.RespoolID)
	if err != nil {
		return 0, errors.Errorf("Respool %s does not exist",
			config.RespoolID.Value)
	}

	runningTasks := 0
	for _, taskInfo := range tasks {
		err = r.addTaskToTracker(taskInfo, config, respool)
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"job":      taskInfo.JobId,
				"instance": taskInfo.InstanceId,
			}).Error("couldn't add to tracker")
			continue
		}
		runningTasks++
	}
	return runningTasks, nil
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
		errors.Wrap(err, "Running Task can not be enqueued")
		return err
	}
	err = rmtask.GetTracker().AddResources(rmTask.Id)
	if err != nil {
		errors.Wrap(err, "could not add resources")
		return err
	}
	if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING {
		err = rmtask.GetTracker().GetTask(rmTask.Id).
			TransitTo(task.TaskState_RUNNING.String())
	} else if taskInfo.GetRuntime().GetState() == task.TaskState_LAUNCHED {
		// There is no Launched state in resmgr we have only launching
		// and after launching state is running.
		err = rmtask.GetTracker().GetTask(rmTask.Id).
			TransitTo(task.TaskState_LAUNCHING.String())
	}
	if err != nil {
		errors.Wrap(err, "Transition Failed")
		return err
	}
	return nil
}

func getEnqueueGangErrorMessage(err *resmgrsvc.EnqueueGangsResponse_Error) string {
	if err.GetFailure() != nil {
		return fmt.Sprintf("failed to enqueue tasks: %v", err.GetFailure().GetFailed())
	}
	if err.GetNoPermission() != nil {
		return fmt.Sprintf("permission denied:%s %s", err.GetNoPermission().GetId().GetValue(),
			err.GetNoPermission().GetMessage())
	}
	if err.GetNotFound() != nil {
		return fmt.Sprintf("resource pool not found:%s %s", err.GetNotFound().GetId().GetValue(),
			err.GetNotFound().GetMessage())
	}
	return ""
}

func (r *recoveryHandler) loadTasksInRange(
	ctx context.Context,
	jobID string,
	from, to uint32) ([]*task.TaskInfo, []*task.TaskInfo, error) {

	log.WithFields(log.Fields{
		"JobID": jobID,
		"from":  from,
		"to":    to,
	}).Info("Checking job instance range")

	if from > to {
		return nil, nil, fmt.Errorf("Invalid job instance range [%v, %v)", from, to)
	} else if from == to {
		return nil, nil, nil
	}

	pbJobID := &peloton.JobID{Value: jobID}
	var notRunningtasks []*task.TaskInfo
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
				"jobID":  jobID,
				"taskID": taskID,
				"state":  taskInfo.GetRuntime().GetState().String(),
			}).Debugf("found task for recovery")
			if taskInfo.GetRuntime().GetState() == task.TaskState_RUNNING ||
				taskInfo.GetRuntime().GetState() == task.TaskState_LAUNCHED {
				runningTasks = append(runningTasks, taskInfo)
			} else {
				notRunningtasks = append(notRunningtasks, taskInfo)
			}
		}
	}
	return notRunningtasks, runningTasks, err
}
