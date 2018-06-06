// Package recovery package can be used to do a fast resync of jobs and tasks in DB.
package recovery

import (
	"context"
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

const (
	// requeueTaskBatchSize defines the batch size of tasks to recover a
	// job upon leader fail-over
	requeueTaskBatchSize = uint32(1000)

	// requeueJobBatchSize defines the batch size of jobs to recover upon
	// leader fail-over
	requeueJobBatchSize = uint32(10)
)

// JobsBatch is used to track a batch of jobs.
type JobsBatch struct {
	jobs []peloton.JobID
}

// TasksBatch is used to track a batch of tasks in a job.
type TasksBatch struct {
	From uint32
	To   uint32
}

// RecoverBatchTasks is a function type which is used to recover a batch of tasks for a job.
type RecoverBatchTasks func(
	ctx context.Context,
	jobID string,
	jobConfig *job.JobConfig,
	jobRuntime *job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error)

func createTaskBatches(config *job.JobConfig) []TasksBatch {
	// check job config
	var batches []TasksBatch
	initialSingleInstance := uint32(0)
	numSingleInstances := config.InstanceCount
	minInstances := config.GetSla().GetMinimumRunningInstances()

	if minInstances > 1 {
		// gangs
		batches = append(batches, TasksBatch{
			0,
			minInstances,
		})
		numSingleInstances -= minInstances
		initialSingleInstance += minInstances
	}
	if numSingleInstances > 0 {
		rangevar := numSingleInstances / requeueTaskBatchSize
		for i := initialSingleInstance; i <= rangevar; i++ {
			From := i * requeueTaskBatchSize
			To := util.Min((i+1)*requeueTaskBatchSize, numSingleInstances)
			batches = append(batches, TasksBatch{
				From,
				To,
			})
		}
	}

	return batches
}

func createJobBatches(jobIDS []peloton.JobID) []JobsBatch {
	numJobs := uint32(len(jobIDS))
	rangevar := numJobs / requeueJobBatchSize
	initialSingleInstance := uint32(0)
	var batches []JobsBatch
	for i := initialSingleInstance; i <= rangevar; i++ {
		from := i * requeueJobBatchSize
		to := util.Min((i+1)*requeueJobBatchSize, numJobs)
		batches = append(batches, JobsBatch{
			jobIDS[from:to],
		})
	}
	return batches
}

func recoverJob(
	ctx context.Context,
	jobID string,
	jobConfig *job.JobConfig,
	jobRuntime *job.RuntimeInfo,
	f RecoverBatchTasks) error {
	finished := make(chan bool, 1)
	errChan := make(chan error, 1)

	taskBatches := createTaskBatches(jobConfig)
	var twg sync.WaitGroup
	// create goroutines for each batch of tasks in the job
	for _, batch := range taskBatches {
		twg.Add(1)
		go func(batch TasksBatch) {
			defer twg.Done()
			f(ctx, jobID, jobConfig, jobRuntime, batch, errChan)
		}(batch)
	}

	go func() {
		twg.Wait()
		close(finished)
	}()

	// wait for all goroutines to finish successfully or
	// exit early
	select {
	case <-finished:
	case err := <-errChan:
		if err != nil {
			return err
		}
	}

	log.WithField("job_id", jobID).Info("recovered job successfully")
	return nil
}

func recoverJobsBatch(
	ctx context.Context,
	jobStore storage.JobStore,
	batch JobsBatch,
	errChan chan<- error,
	f RecoverBatchTasks) {
	for _, jobID := range batch.jobs {
		jobRuntime, err := jobStore.GetJobRuntime(ctx, &jobID)
		if err != nil {
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Error("failed to load job runtime")
			// mv_jobs_by_state is a materialized view created on job_runtime table
			// The job ids here are queried on the materialized view by state.
			// There have been situations where job is deleted from job_runtime but
			// the materialized view does not get updated and the job still shows up.
			// so if you call GetJobRuntime for such a job, it will get a error.
			// In this case, we should log the job_id and skip to next job_id instead
			// of bailing out of the recovery code.

			// TODO (adityacb): create a recovery summary to be
			// returned at the end of this call.
			// That way, the caller has a better idea of recovery
			// stats and error counts and the caller can then
			// increment specific metrics.
			continue
		}

		// Do not process batch jobs in terminal state
		if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) && util.IsPelotonJobStateTerminal(jobRuntime.GetGoalState()) {
			continue
		}

		// Do not process terminated jobs with unknown goal state.
		// Older batch jobs created with code which did not set the goal
		// state will have goal state to be JobState_UNKNOWN.
		if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) && jobRuntime.GetGoalState() == job.JobState_UNKNOWN {
			continue
		}

		jobConfig, err := jobStore.GetJobConfig(ctx, &jobID)
		if err != nil {
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Error("Failed to load job config")
			errChan <- err
			return
		}

		err = recoverJob(ctx, jobID.Value, jobConfig, jobRuntime, f)
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				Error("Failed to recover job", jobID)
			errChan <- err
			return
		}
	}
}

// RecoverJobsByState is the handler to start a job recovery.
func RecoverJobsByState(
	ctx context.Context,
	jobStore storage.JobStore,
	jobStates []job.JobState,
	f RecoverBatchTasks) error {
	log.WithField("job_states", jobStates).Info("job states to recover")
	jobsIDs, err := jobStore.GetJobsByStates(ctx, jobStates)
	if err != nil {
		log.WithError(err).
			Error("failed to fetch jobs in recovery")
		return err
	}

	log.WithFields(log.Fields{
		"total_jobs":            len(jobsIDs),
		"job_ids":               jobsIDs,
		"job_states_to_recover": jobStates,
	}).Info("jobs to recover")

	jobBatches := createJobBatches(jobsIDs)
	var bwg sync.WaitGroup
	finished := make(chan bool)
	errChan := make(chan error, len(jobBatches))
	for _, batch := range jobBatches {
		bwg.Add(1)
		go func(batch JobsBatch) {
			defer bwg.Done()
			recoverJobsBatch(ctx, jobStore, batch, errChan, f)
		}(batch)
	}

	go func() {
		bwg.Wait()
		close(finished)
	}()

	// wait for all goroutines to finish successfully or
	// exit early
	select {
	case <-finished:
	case err := <-errChan:
		if err != nil {
			log.WithError(err).Error("recovery failed")
			return err
		}
	}
	return nil
}
