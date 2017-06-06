package resmgr

import (
	"context"
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

// RecoveryHandler defines the interface to
// be called by leader election callbacks.
type RecoveryHandler interface {
	Start() error
	Stop() error
}

var once sync.Once
var recovery *recoveryHandler

// recoveryHandler performs recovery of jobs which are in non-terminated
// states and requeues the tasks in the pending queue.
// This is performed when the resource manager gains leadership
type recoveryHandler struct {
	sync.Mutex
	jobStore  storage.JobStore
	taskStore storage.TaskStore
	handler   *ServiceHandler
}

// jobStates represents the states which need recovery
var jobStates = []job.JobState{
	job.JobState_PENDING,
	job.JobState_RUNNING,
	job.JobState_UNKNOWN,
	job.JobState_INITIALIZED,
}

// InitRecovery initializes the recoveryHandler
func InitRecovery(jobStore storage.JobStore,
	taskStore storage.TaskStore,
	handler *ServiceHandler) {
	once.Do(func() {
		recovery = &recoveryHandler{
			jobStore:  jobStore,
			taskStore: taskStore,
			handler:   handler,
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
		log.WithError(err).Error("failed to read job IDs for recovery")
		return err
	}
	log.WithField("jobsIDs :", jobsIDs).Info("Resource Manager loading" +
		" jobsIDs from DB for recoveryHandler")
	var jobstring string
	var isError = false
	for _, jobID := range jobsIDs {
		jobConfig, err := r.jobStore.GetJobConfig(ctx, &jobID)
		if err != nil {
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Error("failed to get jobconfig")
			return err
		}
		err = r.requeueJob(ctx, jobID.Value, jobConfig)
		if err != nil {
			log.WithError(err).WithField("Not able to requeue jobStates ", jobID).Error()
			jobstring = jobstring + jobID.Value + " , "
			isError = true
		}
	}
	if isError {
		return errors.Errorf("failed to requeue jobsIDs %s", jobstring)
	}
	log.Info("Job recoveryHandler complete")
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
	var err error
	numSingleInstances := jobConfig.InstanceCount
	initialSingleInstance := uint32(0)
	var errString string
	var isError = false

	if numSingleInstances > 0 {
		rangevar := numSingleInstances / RequeueBatchSize
		for i := initialSingleInstance; i <= rangevar; i++ {
			from := i * RequeueBatchSize
			to := util.Min((i+1)*RequeueBatchSize, numSingleInstances)
			err = r.requeueTasksInRange(ctx, jobID, jobConfig, from, to)

			if err != nil {
				log.Errorf("Failed to requeue tasks for job %v in [%v, %v)",
					jobID, from, to)
				errString = errString + fmt.Sprintf("[ job %v in [%v, %v) ]", jobID, from, to)
				isError = true
			}
		}
	}

	if isError {
		return errors.Errorf("Not able to requeue tasks %s", errString)
	}
	return nil
}

func (r *recoveryHandler) requeueTasksInRange(
	ctx context.Context,
	jobID string,
	jobConfig *job.JobConfig,
	from, to uint32) error {

	tasks, err := r.loadTasksInRange(ctx, jobID, from, to)
	if err != nil {
		return err
	}
	log.WithField("Count", len(tasks)).
		WithField("jobID", jobID).
		Debug("Tasks count to recover")
	resmgrTasks := util.ConvertToResMgrGangs(tasks, jobConfig)

	request := &resmgrsvc.EnqueueGangsRequest{
		Gangs:   resmgrTasks,
		ResPool: jobConfig.RespoolID,
	}
	resp, err := r.handler.EnqueueGangs(ctx, request)
	if resp.GetError() != nil {
		return errors.Errorf("failed to requeue tasks %s", getEnqueueGangErrorMessage(resp.GetError()))
	}
	return err
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
	from, to uint32) ([]*task.TaskInfo, error) {

	log.WithFields(log.Fields{
		"JobID": jobID,
		"from":  from,
		"to":    to,
	}).Info("Checking job instance range")

	if from > to {
		return nil, fmt.Errorf("Invalid job instance range [%v, %v)", from, to)
	} else if from == to {
		return nil, nil
	}

	pbJobID := &peloton.JobID{Value: jobID}
	var tasks []*task.TaskInfo
	taskInfoMap, err := r.taskStore.GetTasksForJobByRange(
		ctx,
		pbJobID,
		&task.InstanceRange{
			From: from,
			To:   to,
		})
	for _, taskInfo := range taskInfoMap {
		tasks = append(tasks, taskInfo)
	}
	return tasks, err
}
