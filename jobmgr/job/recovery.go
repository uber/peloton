package job

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	batchRows        = uint32(1000)
	recoveryInterval = 15 * time.Minute
)

// Recovery scans jobs and make sure that all tasks are created and
// properly sent to RM, when a job manager becomes leader. A job can have its tasks
// partially created, as the job manager instance can fail
type Recovery struct {
	jobStore         storage.JobStore
	taskStore        storage.TaskStore
	resmgrClient     resmgrsvc.ResourceManagerServiceYARPCClient
	lastRecoveryTime time.Time
	metrics          *RecoveryMetrics
}

// NewJobRecovery creates a JobStateValidator
func NewJobRecovery(
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient,
	parentScope tally.Scope) *Recovery {

	return &Recovery{
		jobStore:     jobStore,
		taskStore:    taskStore,
		resmgrClient: resmgrClient,
		metrics:      NewRecoveryMetrics(parentScope.SubScope("job_recovery")),
	}
}

// recoverJobs validates all jobs to make sure that all tasks
// are created and sent to RM, for jobs in INITIALIZED state
func (j *Recovery) recoverJobs(ctx context.Context) {
	if time.Since(j.lastRecoveryTime) < recoveryInterval {
		return
	}
	j.lastRecoveryTime = time.Now()
	jobStates := []job.JobState{
		job.JobState_INITIALIZED,
	}
	jobIDs, err := j.jobStore.GetJobsByStates(ctx, jobStates)
	if err != nil {
		log.WithError(err).
			WithField("states", jobStates).
			Error("Failed to GetJobsByStates")
	}
	for _, jobID := range jobIDs {
		err := j.recoverJob(ctx, &jobID)
		if err == nil {
			j.metrics.JobRecovered.Inc(1)
		} else {
			j.metrics.JobRecoverFailed.Inc(1)
		}
	}
}

// Make sure that all tasks created and queued to RM
func (j *Recovery) recoverJob(ctx context.Context, jobID *peloton.JobID) error {
	log.WithField("job_id", jobID.Value).Info("recovering job")

	jobConfig, err := j.jobStore.GetJobConfig(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to get jobConfig")
		return err
	}

	jobRuntime, err := j.jobStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to GetJobRuntime")
		return err
	}

	createTime, err := time.Parse(time.RFC3339Nano, jobRuntime.CreationTime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("create_time", jobRuntime.CreationTime).
			Error("Failed to Parse job create time")
		return err
	}
	// Only recover job that still in Initialized state after recoveryInterval
	// this is for avoiding collision with jobs being created right now
	if time.Since(createTime) < recoveryInterval {
		log.WithField("job_id", jobID).
			WithField("create_time", createTime).
			Info("Job created recently, skip")
		return nil
	}

	for batch := uint32(0); batch < jobConfig.InstanceCount/batchRows+1; batch++ {
		var tasksToRequeue []*pb_task.TaskInfo
		start := batch * batchRows
		end := util.Min((batch+1)*batchRows, jobConfig.InstanceCount)
		log.WithField("start", start).
			WithField("end", end).
			Debug("Validating task range")
		for i := start; i < end; i++ {
			t, err := j.taskStore.GetTaskForJob(ctx, jobID, i)
			if err != nil {
				_, ok := err.(*storage.TaskNotFoundError)
				if !ok {
					log.WithError(err).
						WithField("job_id", jobID.Value).
						WithField("id", i).
						Error("Failed to GetTaskForJob")
					continue
				}
				// Task does not exist in DB
				log.WithField("job_id", jobID.Value).
					WithField("task_instance", i).
					Info("Creating missing task")
				task, err := createTaskForJob(ctx, j.taskStore, jobID, i, jobConfig)
				if err != nil {
					log.WithError(err).
						WithField("job_id", jobID.Value).
						WithField("id", i).
						Error("Failed to CreateTask")
					j.metrics.TaskRecoverFailed.Inc(1)
					return err
				}
				j.metrics.TaskRecovered.Inc(1)
				tasksToRequeue = append(tasksToRequeue, task)
			} else {
				//Task exists, check if task is in initialized state, if yes then requeue to RM
				var taskState pb_task.TaskState
				for _, taskInfo := range t {
					taskState = taskInfo.Runtime.State
					// Task state in initialized need to be requeued to RM
					if taskState == pb_task.TaskState_INITIALIZED {
						tasksToRequeue = append(tasksToRequeue, taskInfo)
						log.WithField("job_id", jobID.Value).
							WithField("task_instance", i).
							Info("Requeue initialized task")
					}
				}
			}
		}

		if len(tasksToRequeue) > 0 {
			// requeue the tasks into resgmr
			// TODO: retry policy
			err := task.EnqueueGangs(context.Background(), tasksToRequeue, jobConfig, j.resmgrClient)
			if err != nil {
				log.WithError(err).
					WithField("job_id", jobID.Value).
					Error("Failed to EnqueueTasks to resmgr")
				j.metrics.TaskRequeueFailed.Inc(int64(len(tasksToRequeue)))
				return err
			}
			j.metrics.TaskRequeued.Inc(int64(len(tasksToRequeue)))
		}
	}
	jobRuntime.State = job.JobState_PENDING
	err = j.jobStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to UpdateJobRuntime")
		return err
	}
	return nil
}

func createTaskForJob(
	ctx context.Context,
	taskStore storage.TaskStore,
	jobID *peloton.JobID,
	id uint32,
	jobConfig *job.JobConfig) (*pb_task.TaskInfo, error) {
	taskInfo, err := task.CreateInitializingTask(jobID, id, jobConfig)
	if err != nil {
		return nil, err
	}

	if err := taskStore.CreateTask(ctx, jobID, id, taskInfo, jobConfig.OwningTeam); err != nil {
		log.WithError(err).
			WithField("job_id", jobID.Value).
			WithField("id", id).
			Error("Failed to CreateTask")
		return nil, err
	}

	return taskInfo, nil
}
