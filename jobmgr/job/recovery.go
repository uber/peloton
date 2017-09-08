package job

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"

	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"
)

const (
	batchRows = uint32(1000)
	// recoveryInterval represents the minimal time interval between two
	// attempts of jobs recovery.
	recoveryInterval = 15 * time.Minute
	// jobRecoveryGracePeriod represents the minimal age of any job to
	// be recovered. This prevents race between recovery and normal job state
	// transition at creation.
	jobRecoveryGracePeriod = 15 * time.Minute
)

// Recovery scans jobs and make sure that all tasks are created and
// properly sent to RM, when a job manager becomes leader. A job can have its tasks
// partially created, as the job manager instance can fail
type Recovery struct {
	trackedManager   tracked.Manager
	jobStore         storage.JobStore
	taskStore        storage.TaskStore
	lastRecoveryTime time.Time
	metrics          *RecoveryMetrics
}

// NewJobRecovery creates a JobStateValidator
func NewJobRecovery(
	trackedManager tracked.Manager,
	jobStore storage.JobStore,
	taskStore storage.TaskStore,
	parentScope tally.Scope) *Recovery {

	return &Recovery{
		trackedManager: trackedManager,
		jobStore:       jobStore,
		taskStore:      taskStore,
		metrics:        NewRecoveryMetrics(parentScope.SubScope("job_recovery")),
	}
}

// recoverJobs validates all jobs to make sure that all tasks
// are created and sent to RM, for jobs in INITIALIZED state
func (j *Recovery) recoverJobs(ctx context.Context) {
	startup := j.lastRecoveryTime.IsZero()
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
		err := j.recoverJob(ctx, &jobID, startup)
		if err == nil {
			j.metrics.JobRecovered.Inc(1)
		} else {
			j.metrics.JobRecoverFailed.Inc(1)
		}
	}
}

// Make sure that all tasks created and queued to RM
func (j *Recovery) recoverJob(ctx context.Context, jobID *peloton.JobID, startup bool) error {
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

	// If this not the first time we recover (startup), we check the age of the job
	// before trying to recover.
	if !startup {
		createTime, err := time.Parse(time.RFC3339Nano, jobRuntime.CreationTime)
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				WithField("create_time", jobRuntime.CreationTime).
				Error("Failed to Parse job create time")
			return err
		}
		// Only recover job that still in Initialized state after jobRecoveryGracePeriod
		// this is for avoiding collision with jobs being created right now
		if time.Since(createTime) < jobRecoveryGracePeriod {
			log.WithField("job_id", jobID).
				WithField("create_time", createTime).
				Info("Job created recently, skip")
			return nil
		}
	}

	for batch := uint32(0); batch < jobConfig.InstanceCount/batchRows+1; batch++ {
		start := batch * batchRows
		end := util.Min((batch+1)*batchRows, jobConfig.InstanceCount)
		log.WithField("start", start).
			WithField("end", end).
			Debug("Validating task range")
		for i := start; i < end; i++ {
			_, err := j.taskStore.GetTaskForJob(ctx, jobID, i)
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
				err := createTaskForJob(ctx, j.trackedManager, j.taskStore, jobID, i, jobConfig)
				if err != nil {
					log.WithError(err).
						WithField("job_id", jobID.Value).
						WithField("id", i).
						Error("Failed to CreateTask")
					j.metrics.TaskRecoverFailed.Inc(1)
					return err
				}
				j.metrics.TaskRecovered.Inc(1)
			}
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
	trackedManager tracked.Manager,
	taskStore storage.TaskStore,
	jobID *peloton.JobID,
	id uint32,
	jobConfig *job.JobConfig) error {
	taskInfo, err := task.CreateInitializingTask(jobID, id, jobConfig)
	if err != nil {
		return err
	}

	if err := taskStore.CreateTask(ctx, jobID, id, taskInfo, jobConfig.OwningTeam); err != nil {
		log.WithError(err).
			WithField("job_id", jobID.Value).
			WithField("id", id).
			Error("Failed to CreateTask")
		return err
	}

	trackedManager.SetTask(jobID, id, taskInfo.Runtime)

	return nil
}
