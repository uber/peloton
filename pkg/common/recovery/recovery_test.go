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

package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb_job "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/models"

	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

var (
	activeJobsOps  ormobjects.ActiveJobsOps
	jobConfigOps   ormobjects.JobConfigOps
	jobRuntimeOps  ormobjects.JobRuntimeOps
	receivedJobIDs []string
	count          int
)

var mutex = &sync.Mutex{}
var scope = tally.Scope(tally.NoopScope)

func init() {
	conf := ormobjects.GenerateTestCassandraConfig()
	ormobjects.MigrateSchema(conf)

	var err error
	if err != nil {
		log.Fatal(err)
	}

	ormStore, ormErr := ormobjects.NewCassandraStore(
		conf,
		scope,
	)
	if ormErr != nil {
		log.Fatal(ormErr)
	}

	activeJobsOps = ormobjects.NewActiveJobsOps(ormStore)
	jobConfigOps = ormobjects.NewJobConfigOps(ormStore)
	jobRuntimeOps = ormobjects.NewJobRuntimeOps(ormStore)
}

func createJobRuntime(
	ctx context.Context,
	jobID *peloton.JobID,
) error {
	now := time.Now()
	runtime := pb_job.RuntimeInfo{
		State:        pb_job.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    pb_job.JobState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: 1,
	}
	return jobRuntimeOps.Upsert(ctx, jobID, &runtime)
}

func createJobConfig(
	ctx context.Context,
	jobID *peloton.JobID,
) error {

	var sla = pb_job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 3,
		Preemptible:             false,
	}
	var taskConfig = pb_task.TaskConfig{
		Resource: &pb_task.ResourceConfig{
			CpuLimit:    0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
		},
	}

	now := time.Now()
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		SLA:           &sla,
		DefaultConfig: &taskConfig,
		InstanceCount: 2,
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
	}
	configAddOn := &models.ConfigAddOn{}

	return jobConfigOps.Create(ctx, jobID, &jobConfig, configAddOn, nil, 1)
}

func createJob(
	ctx context.Context,
	state pb_job.JobState,
	goalState pb_job.JobState,
) (*peloton.JobID, error) {
	var jobID = &peloton.JobID{Value: uuid.New()}

	if err := createJobRuntime(ctx, jobID); err != nil {
		return nil, err
	}

	if err := createJobConfig(ctx, jobID); err != nil {
		return nil, err
	}

	if err := activeJobsOps.Create(ctx, jobID); err != nil {
		return nil, err
	}

	jobRuntime, err := jobRuntimeOps.Get(ctx, jobID)
	if err != nil {
		return nil, err
	}

	jobRuntime.State = state
	jobRuntime.GoalState = goalState

	if err := jobRuntimeOps.Upsert(ctx, jobID, jobRuntime); err != nil {
		return nil, err
	}
	return jobID, nil
}

func recoverAllTask(
	ctx context.Context,
	jobID string,
	jobConfig *pb_job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *pb_job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error) {
	mutex.Lock()
	receivedJobIDs = append(receivedJobIDs, jobID)
	mutex.Unlock()
	return
}

func recoverFailTaskRandomly(
	ctx context.Context,
	jobID string,
	jobConfig *pb_job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *pb_job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error) {
	mutex.Lock()
	if count%9 == 0 {
		errChan <- errors.New("Task Recovery Failed")
	}
	count++
	mutex.Unlock()

	return
}

func TestJobRecoveryWithStore(t *testing.T) {
	var err error

	ctx := context.Background()

	pendingJobID, err := createJob(
		ctx,
		pb_job.JobState_PENDING,
		pb_job.JobState_SUCCEEDED,
	)
	assert.NoError(t, err)

	runningJobID, err := createJob(
		ctx,
		pb_job.JobState_RUNNING,
		pb_job.JobState_SUCCEEDED,
	)
	assert.NoError(t, err)

	receivedJobIDs = nil
	err = RecoverActiveJobs(
		ctx,
		scope,
		activeJobsOps,
		jobConfigOps,
		jobRuntimeOps,
		recoverAllTask)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(receivedJobIDs))

	// Delete these jobIDs from active jobs table to clear the state for
	// next tests
	err = activeJobsOps.Delete(ctx, pendingJobID)
	assert.NoError(t, err)
	err = activeJobsOps.Delete(ctx, runningJobID)
	assert.NoError(t, err)
}

func TestJobRecoveryMissingJobsWithStore(t *testing.T) {
	var err error

	ctx := context.Background()
	missingRuntimeID := &peloton.JobID{Value: uuid.New()}
	missingConfigID := &peloton.JobID{Value: uuid.New()}

	// Create only job config for this job
	err = createJobConfig(ctx, missingRuntimeID)
	assert.NoError(t, err)

	// Create only job runtime for this job
	err = createJobRuntime(ctx, missingConfigID)
	assert.NoError(t, err)

	// Add both jobs to active_jobs
	err = activeJobsOps.Create(ctx, missingConfigID)
	assert.NoError(t, err)
	err = activeJobsOps.Create(ctx, missingRuntimeID)
	assert.NoError(t, err)

	// Make sure active jobs contains these entries
	jobs, err := activeJobsOps.GetAll(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(jobs))

	receivedJobIDs = nil
	err = RecoverActiveJobs(
		ctx,
		scope,
		activeJobsOps,
		jobConfigOps,
		jobRuntimeOps,
		recoverAllTask)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(receivedJobIDs))

	// Make sure both jobs have been removed from active_jobs
	jobs, err = activeJobsOps.GetAll(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
}

// TestRecoveryMissingJobRuntime tests recovery when one of the active jobs
// doesn't exist in job_runtime
func TestRecoveryMissingJobRuntime(t *testing.T) {
	var missingJobID = &peloton.JobID{Value: uuid.New()}
	var pendingJobID = &peloton.JobID{Value: uuid.New()}
	var jobRuntime = pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}
	var jobConfig = pb_job.JobConfig{
		Type:          pb_job.JobType_BATCH,
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		InstanceCount: 2,
	}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)

	// missingJobID doesn't have job_runtime
	mockJobRuntimeOps.EXPECT().
		Get(ctx, missingJobID).
		Return(
			nil,
			yarpcerrors.NotFoundErrorf(
				"Cannot find job wth jobID %v", missingJobID.GetValue()),
		)
	mockJobRuntimeOps.EXPECT().
		Get(ctx, pendingJobID).
		Return(&jobRuntime, nil)

	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{missingJobID}, nil)

	mockActiveJobsOps.EXPECT().
		Delete(ctx, missingJobID).
		Return(nil)

	mockJobConfigOps.EXPECT().
		Get(ctx, pendingJobID, gomock.Any()).
		Return(&jobConfig, &models.ConfigAddOn{}, nil).AnyTimes()

	err := RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)
}

// TestRecoveryMissingJobConfig tests recovery when one of the active jobs
// doesn't exist in job_config
func TestRecoveryMissingJobConfig(t *testing.T) {
	var missingJobID = &peloton.JobID{Value: uuid.New()}
	var pendingJobID = &peloton.JobID{Value: uuid.New()}
	var err error
	var jobRuntime = pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}
	var jobConfig = pb_job.JobConfig{
		Type:          pb_job.JobType_BATCH,
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		InstanceCount: 2,
	}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)

	mockJobRuntimeOps.EXPECT().
		Get(ctx, missingJobID).
		Return(&jobRuntime, nil)

	mockJobRuntimeOps.EXPECT().
		Get(ctx, pendingJobID).
		Return(&jobRuntime, nil)

	// recoverJobsBatch should pass even if there is no job_id present in
	// job_config. It should just skip over to a new job.
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{missingJobID, pendingJobID}, nil)

	// missingJobID doesn't have job_config
	mockJobConfigOps.EXPECT().
		Get(ctx, missingJobID, gomock.Any()).
		Return(
			nil,
			&models.ConfigAddOn{},
			yarpcerrors.NotFoundErrorf(
				"Cannot find job wth jobID %v", missingJobID.GetValue()),
		)

	mockJobConfigOps.EXPECT().
		Get(ctx, pendingJobID, gomock.Any()).
		Return(&jobConfig, &models.ConfigAddOn{}, nil).AnyTimes()

	mockActiveJobsOps.EXPECT().Delete(ctx, missingJobID).Return(nil)

	err = RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)
}

// TestRecoveryTerminalJobs tests recovery when one of the active jobs
// is actually a terminal job and verifies that the job is deleted from the
// active_jobs table ONLY if it is a BATCH job
func TestRecoveryTerminalJobs(t *testing.T) {
	var err error
	var terminalJobID = &peloton.JobID{Value: uuid.New()}
	var nonTerminalJobID = &peloton.JobID{Value: uuid.New()}
	var jobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_SUCCEEDED,
		GoalState: pb_job.JobState_SUCCEEDED,
	}
	var nonTerminalJobRuntime = pb_job.RuntimeInfo{
		State:     pb_job.JobState_PENDING,
		GoalState: pb_job.JobState_SUCCEEDED,
	}

	var jobConfig = pb_job.JobConfig{
		Type:          pb_job.JobType_BATCH,
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		InstanceCount: 2,
	}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)

	mockJobRuntimeOps.EXPECT().
		Get(ctx, terminalJobID).
		Return(&jobRuntime, nil)
	mockJobRuntimeOps.EXPECT().
		Get(ctx, nonTerminalJobID).
		Return(&nonTerminalJobRuntime, nil)

	// recoverJobsBatch should pass even if the job to be recovered is terminal
	// and if it is a batch job, it should be delete from the active_jobs table
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{nonTerminalJobID, terminalJobID}, nil)

	mockJobConfigOps.EXPECT().
		Get(ctx, terminalJobID, gomock.Any()).
		Return(&jobConfig, &models.ConfigAddOn{}, nil)
	mockJobConfigOps.EXPECT().
		Get(ctx, nonTerminalJobID, gomock.Any()).
		Return(&jobConfig, &models.ConfigAddOn{}, nil)

	// Expect this call because this is a terminal BATCH job
	mockActiveJobsOps.EXPECT().Delete(ctx, terminalJobID).Return(nil)

	err = RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)

	// Same test for stateless job
	jobConfig = pb_job.JobConfig{
		Type: pb_job.JobType_SERVICE,
	}

	// recoverJobsBatch should pass even if the job to be recovered is terminal
	// and if it is a batch job, it should be delete from the active_jobs table
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{terminalJobID}, nil)

	mockJobRuntimeOps.EXPECT().
		Get(ctx, terminalJobID).
		Return(&jobRuntime, nil)

	mockJobConfigOps.EXPECT().
		Get(ctx, terminalJobID, gomock.Any()).
		Return(&jobConfig, &models.ConfigAddOn{}, nil)

	// This time, we don't expect a call to Delete.

	err = RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)

}

// TestRecoveryErrors tests RecoverActiveJobs errors
func TestRecoveryErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)

	// Test active_jobs GetAll error.
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return(nil, fmt.Errorf("Fake GetAll active jobs error"))
	err := RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.Error(t, err)
}

// TestRecoveryWithFailedJobBatches test will create 100 jobs, and eventually 10
// jobByBatches. recoverTaskRandomly will send error for recovering 11 jobs,
// so errChan will have at least 2 messages or max 10 messages if all job
// batches failed, here errChan will be full.
// TODO (varung): Add go routine leak test.
func TestRecoveryWithFailedJobBatches(t *testing.T) {
	var err error
	var jobID *peloton.JobID
	var activeJobs []*peloton.JobID

	count = 0
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		jobID, err = createJob(ctx, pb_job.JobState_PENDING, pb_job.JobState_SUCCEEDED)
		assert.NoError(t, err)
		activeJobs = append(activeJobs, jobID)
	}

	for i := 0; i < 50; i++ {
		jobID, err = createJob(ctx, pb_job.JobState_RUNNING, pb_job.JobState_SUCCEEDED)
		assert.NoError(t, err)
		activeJobs = append(activeJobs, jobID)
	}

	err = RecoverActiveJobs(
		ctx, scope, activeJobsOps, jobConfigOps, jobRuntimeOps, recoverFailTaskRandomly)
	assert.Error(t, err)

	for _, jobID := range activeJobs {
		// Delete these jobIDs from active jobs table to clear the state for
		// next tests
		err = activeJobsOps.Delete(ctx, jobID)
		assert.NoError(t, err)
	}
}

// TestDeleteActiveJobFailure simulates Delete failure and ensures
// that the recovery still goes through fine.
func TestDeleteActiveJobFailure(t *testing.T) {
	var missingJobID = &peloton.JobID{Value: uuid.New()}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)

	// recoverJobsBatch should pass even if there is no job_id present in
	// job_runtime. It should just skip over to a new job.
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{missingJobID}, nil)

	// missingJobID doesn't have job_runtime
	mockJobRuntimeOps.EXPECT().
		Get(ctx, missingJobID).
		Return(
			nil,
			yarpcerrors.NotFoundErrorf(
				"Cannot find job wth jobID %v", missingJobID.GetValue()),
		)

	mockActiveJobsOps.EXPECT().
		Delete(ctx, missingJobID).
		Return(fmt.Errorf("Delete active jobs error"))

	err := RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)
}

// TestDeleteOnlyOnNotFound simulates the case where cassandra error is not
// not found. In this case the job should not be deleted from active_jobs
func TestDeleteOnlyOnNotFound(t *testing.T) {
	var missingJobID = &peloton.JobID{Value: uuid.New()}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockJobRuntimeOps := objectmocks.NewMockJobRuntimeOps(ctrl)
	mockActiveJobsOps := objectmocks.NewMockActiveJobsOps(ctrl)
	mockJobConfigOps := objectmocks.NewMockJobConfigOps(ctrl)

	// recoverJobsBatch should pass even if there is no job_id present in
	// job_runtime. It should just skip over to a new job.
	mockActiveJobsOps.EXPECT().
		GetAll(ctx).
		Return([]*peloton.JobID{missingJobID}, nil)

	// missingJobID doesn't have job_runtime
	mockJobRuntimeOps.EXPECT().
		Get(ctx, missingJobID).
		Return(
			nil,
			yarpcerrors.InternalErrorf(
				"Cannot find job wth jobID %v", missingJobID.GetValue()),
		)

	// Delete should NOT be called in this case because the job
	// doesn't result in NotFound error from DB.

	err := RecoverActiveJobs(
		ctx,
		scope,
		mockActiveJobsOps,
		mockJobConfigOps,
		mockJobRuntimeOps,
		recoverAllTask,
	)
	assert.NoError(t, err)
}
