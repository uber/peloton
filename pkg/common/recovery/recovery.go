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

// Package recovery package can be used to do a fast resync of jobs and tasks in DB.
package recovery

import (
	"context"
	"sync"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/storage"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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
	configAddOn *models.ConfigAddOn,
	jobRuntime *job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error)

func createTaskBatches(config *job.JobConfig) []TasksBatch {
	// check job config
	var batches []TasksBatch
	initialSingleInstance := uint32(0)
	numSingleInstances := config.InstanceCount
	minInstances := config.GetSLA().GetMinimumRunningInstances()

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
	configAddOn *models.ConfigAddOn,
	jobRuntime *job.RuntimeInfo,
	f RecoverBatchTasks) error {
	finished := make(chan bool)
	errChan := make(chan error, 1)

	taskBatches := createTaskBatches(jobConfig)
	var twg sync.WaitGroup
	// create goroutines for each batch of tasks in the job
	for _, batch := range taskBatches {
		twg.Add(1)
		go func(batch TasksBatch) {
			defer twg.Done()
			f(ctx, jobID, jobConfig, configAddOn, jobRuntime, batch, errChan)
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
		jobRuntime, err := jobStore.GetJobRuntime(ctx, jobID.GetValue())
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

		// Do not process jobs in terminal state and have no update
		if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) &&
			util.IsPelotonJobStateTerminal(jobRuntime.GetGoalState()) &&
			len(jobRuntime.GetUpdateID().GetValue()) == 0 {
			continue
		}

		jobConfig, configAddOn, err := jobStore.GetJobConfig(ctx, jobID.GetValue())
		if err != nil {
			// config is not found and job state is uninitialized,
			// which means job is partially created and cannot be recovered.
			if yarpcerrors.IsNotFound(err) &&
				jobRuntime.GetState() == job.JobState_UNINITIALIZED {
				continue
			}

			log.WithField("job_id", jobID.Value).
				WithError(err).
				Error("Failed to load job config")
			errChan <- err
			return
		}

		err = recoverJob(ctx, jobID.Value, jobConfig, configAddOn, jobRuntime, f)
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				Error("Failed to recover job", jobID)
			errChan <- err
			return
		}
	}
}

// populateMissingActiveJobs will find out which jobIDs are present in
// materialzied view, but absent from active_jobs table and then add them to
// the active_jobs table.
func populateMissingActiveJobs(
	ctx context.Context,
	jobStore storage.JobStore,
	jobIDsFromMV []peloton.JobID,
	activeJobIDs []peloton.JobID,
	mtx *Metrics,
) {
	// get jobs that are in jobIDsFromMV but not in activeJobIDs
	// Add these jobs to active_jobs table

	jobIDsMap := make(map[string]bool)
	for _, jobID := range activeJobIDs {
		jobIDsMap[jobID.GetValue()] = true
	}
	// All jobs in jobIDsFromMV should be already present in jobIDsMap
	// If a job is not present, add it to active_jobs table.
	for _, jobID := range jobIDsFromMV {
		if _, ok := jobIDsMap[jobID.GetValue()]; !ok {
			// Just add the job to active_jobs table irrespective of job state
			// This code is for temporary migration and will be deleted in
			// subsequent releases, so keeping the logic simple.
			// adding terminal jobs to active_jobs table will result in them
			// getting deleted as part of active jobs cleanup action and will
			// not have any adverse effect on jobmgr
			log.WithField("job_id", jobID).
				Info("Add missing job to active_jobs")
			if err := jobStore.AddActiveJob(
				ctx, &peloton.JobID{Value: jobID.GetValue()}); err != nil {
				// Do not error out in recovery because this is not a critical
				// operation and we should continue with recovery despite this
				// error.
				log.WithField("job_id", jobID).
					WithError(err).Info("Failed to add job to active_jobs")
				mtx.activeJobsBackfillFail.Inc(1)
			} else {
				mtx.activeJobsBackfill.Inc(1)
			}
		}
	}
}

// getDereferencedJobIDsList dereferences the jobIDs list
func getDereferencedJobIDsList(jobIDs []*peloton.JobID) []peloton.JobID {
	result := []peloton.JobID{}
	for _, jobID := range jobIDs {
		result = append(result, *jobID)
	}
	return result
}

// RecoverJobsByState is the handler to start a job recovery.
func RecoverJobsByState(
	ctx context.Context,
	parentScope tally.Scope,
	jobStore storage.JobStore,
	jobStates []job.JobState,
	f RecoverBatchTasks,
	recoverFromActiveJobs,
	backfillFromMV bool,
) error {

	log.WithField("job_states", jobStates).Info("job states to recover")
	mtx := NewMetrics(parentScope.SubScope("recovery"))

	jobsIDs, err := jobStore.GetJobsByStates(ctx, jobStates)
	if err != nil {
		log.WithError(err).
			Error("failed to fetch jobs in recovery")
		return err
	}

	activeJobIDs, err := jobStore.GetActiveJobs(ctx)
	if err != nil {
		// Monitor logs to make sure you no longer see this log.
		// We will start returning error here once we switch recovery to use
		// active_jobs table.
		log.WithError(err).
			Error("GetActiveJobs failed")
	}
	activeJobIDsCopy := getDereferencedJobIDsList(activeJobIDs)

	mtx.activeJobsMV.Update(float64(len(jobsIDs)))
	mtx.activeJobs.Update(float64(len(activeJobIDsCopy)))

	if len(jobsIDs) != len(activeJobIDsCopy) {
		// Monitor logs to make sure you no longer see this log. Once we get
		// active_jobs populated by goalstate engine, we should never see this
		// log and at that time we are ready to switch recovery to use
		// active_jobs table

		log.WithFields(log.Fields{
			"total_jobs_from_mv": len(jobsIDs),
			"total_active_jobs":  len(activeJobIDs),
		}).Error("active_jobs not equal to jobs in mv_job_by_state")

		if backfillFromMV {
			// Backfill the missing jobs into active_jobs table.
			go populateMissingActiveJobs(
				ctx, jobStore, jobsIDs, activeJobIDsCopy, mtx)
		}
	}

	if recoverFromActiveJobs {
		// if this flag is set, recover from active_jobs list instead of MV
		//jobsIDs = getDereferencedJobIDsList(activeJobIDs)
		log.WithFields(log.Fields{
			"total_active_jobs": len(activeJobIDsCopy),
			"job_ids":           jobsIDs,
		}).Info("jobs to recover")
	} else {
		log.WithFields(log.Fields{
			"total_jobs":            len(jobsIDs),
			"job_ids":               jobsIDs,
			"job_states_to_recover": jobStates,
		}).Info("jobs to recover")
	}

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
		// If the last goroutine threw an error then both cases of the select
		// statement are satisfied. To ensure this error doesnt go uncaught, we
		// need to check the length of errChan here
		if len(errChan) != 0 {
			err = <-errChan
			log.WithError(err).Error("recovery failed")
			return err
		}
	case err := <-errChan:
		if err != nil {
			log.WithError(err).Error("recovery failed")
			return err
		}
	}
	return nil
}
