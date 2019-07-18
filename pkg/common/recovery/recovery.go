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
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

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

// jobRecoverySummary is used to track the skipped failure cases from
// recoverJobsBatch.
type jobRecoverySummary struct {
	missingJobRuntime    bool
	missingJobConfig     bool
	terminalRecoveredJob bool
}

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
	activeJobsOps ormobjects.ActiveJobsOps,
	jobConfigOps ormobjects.JobConfigOps,
	jobRuntimeOps ormobjects.JobRuntimeOps,
	batch JobsBatch,
	errChan chan<- error,
	summaryChan chan<- jobRecoverySummary,
	f RecoverBatchTasks) {

	var deleteWg sync.WaitGroup
	defer deleteWg.Wait()

	for _, jobID := range batch.jobs {
		jobRuntime, err := jobRuntimeOps.Get(ctx, &jobID)
		if err != nil {
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Info("failed to load job runtime")
			// mv_jobs_by_state is a materialized view created on job_runtime table
			// The job ids here are queried on the materialized view by state.
			// There have been situations where job is deleted from job_runtime but
			// the materialized view does not get updated and the job still shows up.
			// so if you get job_runtime for such a job, it will get a error.
			// In this case, we should log the job_id and skip to next job_id instead
			// of bailing out of the recovery code.

			summaryChan <- jobRecoverySummary{missingJobRuntime: true}

			if yarpcerrors.IsNotFound(err) {
				// Delete the job from active_jobs table and move on to the next
				// job for recovery
				deleteWg.Add(1)
				go func(jobID peloton.JobID) {
					defer deleteWg.Done()
					deleteFromActiveJobs(ctx, &jobID, activeJobsOps)
				}(jobID)
			}
			continue
		}

		jobConfig, configAddOn, err := jobConfigOps.Get(
			ctx,
			&jobID,
			jobRuntime.GetConfigurationVersion(),
		)
		if err != nil {
			log.WithField("job_id", jobID.Value).
				WithError(err).
				Info("Failed to load job config")
			// There have been situations where job is deleted from job_config
			// but still present in the job_runtime. So if you call GetJobConfig
			// here, it will get an error. In this case, we should log the
			// failure case and skip to next job_id instead of failing the
			// recovery code.

			summaryChan <- jobRecoverySummary{missingJobConfig: true}

			if yarpcerrors.IsNotFound(err) {
				// Delete the job from active_jobs table and move on to the next
				// job for recovery
				deleteWg.Add(1)
				go func(jobID peloton.JobID) {
					defer deleteWg.Done()
					deleteFromActiveJobs(ctx, &jobID, activeJobsOps)
				}(jobID)
			}
			continue
		}

		// Do not process jobs in terminal state and have no update
		if util.IsPelotonJobStateTerminal(jobRuntime.GetState()) &&
			util.IsPelotonJobStateTerminal(jobRuntime.GetGoalState()) {
			// Delete this job from active_jobs table ONLY if it is a terminal
			// BATCH job
			if jobConfig.GetType() == job.JobType_BATCH {
				summaryChan <- jobRecoverySummary{terminalRecoveredJob: true}
				log.WithField("job_id", jobID).
					Info("delete terminal batch job from active_jobs")
				deleteWg.Add(1)
				go func(jobID peloton.JobID) {
					defer deleteWg.Done()
					deleteFromActiveJobs(ctx, &jobID, activeJobsOps)
				}(jobID)
			}
			continue
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

// RecoverActiveJobs is the handler to start a job recovery.
func RecoverActiveJobs(
	ctx context.Context,
	parentScope tally.Scope,
	activeJobsOps ormobjects.ActiveJobsOps,
	jobConfigOps ormobjects.JobConfigOps,
	jobRuntimeOps ormobjects.JobRuntimeOps,
	f RecoverBatchTasks,
) error {

	mtx := NewMetrics(parentScope.SubScope("recovery"))

	activeJobIDs, err := activeJobsOps.GetAll(ctx)
	if err != nil {
		log.WithError(err).
			Error("GetActiveJobs failed")
		return err
	}

	activeJobIDsCopy := util.GetDereferencedJobIDsList(activeJobIDs)

	mtx.activeJobs.Update(float64(len(activeJobIDsCopy)))

	log.WithFields(log.Fields{
		"total_active_jobs": len(activeJobIDsCopy),
		"job_ids":           activeJobIDsCopy,
	}).Info("jobs to recover")

	jobBatches := createJobBatches(activeJobIDsCopy)
	var bwg sync.WaitGroup
	finished := make(chan bool)
	errChan := make(chan error, len(jobBatches))
	summaryChan := make(chan jobRecoverySummary, len(jobBatches))
	for _, batch := range jobBatches {
		bwg.Add(1)
		go func(batch JobsBatch) {
			defer bwg.Done()
			recoverJobsBatch(
				ctx,
				activeJobsOps,
				jobConfigOps,
				jobRuntimeOps,
				batch,
				errChan,
				summaryChan,
				f,
			)
		}(batch)
	}

	go func() {
		bwg.Wait()
		close(finished)
		close(summaryChan)
	}()

	handleRecoverySummary := func(summary jobRecoverySummary) {
		if summary.missingJobRuntime {
			mtx.missingJobRuntime.Inc(1)
		}
		if summary.missingJobConfig {
			mtx.missingJobConfig.Inc(1)
		}
		if summary.terminalRecoveredJob {
			mtx.terminalRecoveredJob.Inc(1)
		}
	}

	// wait for all goroutines to finish successfully or
	// exit early
	for {
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
			if len(summaryChan) != 0 {
				for summary := range summaryChan {
					handleRecoverySummary(summary)
				}
			}
			return nil
		case err := <-errChan:
			if err != nil {
				log.WithError(err).Error("recovery failed")
				return err
			}
		case result := <-summaryChan:
			handleRecoverySummary(result)
		}
	}
}

// deleteFromActiveJobs best effort deletes a job from the active_jobs
// There is no harm if this delete fails because we are not going to recover
// this job any way. So we should log a failure here and continue recovery
func deleteFromActiveJobs(
	ctx context.Context,
	jobID *peloton.JobID,
	activeJobsOps ormobjects.ActiveJobsOps,
) {
	if err := activeJobsOps.Delete(ctx, jobID); err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Info("DeleteActiveJob failed")
	}
}
