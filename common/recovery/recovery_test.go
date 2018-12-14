package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb_job "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pb_task "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/storage/cassandra"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

var (
	csStore              *cassandra.Store
	pendingJobID         *peloton.JobID
	runningJobID         *peloton.JobID
	receivedPendingJobID []string
	count                int
)

var mutex = &sync.Mutex{}

func init() {
	conf := cassandra.MigrateForTest()
	var err error
	csStore, err = cassandra.NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}

func createJob(ctx context.Context, state pb_job.JobState, goalState pb_job.JobState) (*peloton.JobID, error) {
	var jobID = &peloton.JobID{Value: uuid.New()}
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

	initialJobRuntime := pb_job.RuntimeInfo{
		State:        pb_job.JobState_INITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: jobConfig.GetChangeLog().GetVersion(),
	}

	err := csStore.CreateJobConfig(ctx, jobID, &jobConfig, configAddOn, 1, "gsg9")
	if err != nil {
		return nil, err
	}

	err = csStore.CreateJobRuntimeWithConfig(ctx, jobID, &initialJobRuntime, &jobConfig)
	if err != nil {
		return nil, err
	}

	jobRuntime, err := csStore.GetJobRuntime(ctx, jobID)
	if err != nil {
		return nil, err
	}

	jobRuntime.State = state
	jobRuntime.GoalState = goalState
	err = csStore.UpdateJobRuntime(ctx, jobID, jobRuntime)
	if err != nil {
		return nil, err
	}

	return jobID, nil
}

func createPartiallyCreatedJob(ctx context.Context, goalState pb_job.JobState) (*peloton.JobID, error) {
	var jobID = &peloton.JobID{Value: uuid.New()}
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

	initialJobRuntime := pb_job.RuntimeInfo{
		State:        pb_job.JobState_UNINITIALIZED,
		CreationTime: now.Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
		ConfigurationVersion: jobConfig.GetChangeLog().GetVersion(),
	}

	err := csStore.CreateJobRuntimeWithConfig(ctx, jobID, &initialJobRuntime, &jobConfig)
	if err != nil {
		return nil, err
	}

	return jobID, nil
}

func recoverPendingTask(
	ctx context.Context,
	jobID string,
	jobConfig *pb_job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *pb_job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error) {
	if jobID != pendingJobID.GetValue() {
		err := fmt.Errorf("Got the wrong job id")
		errChan <- err
	} else {
		mutex.Lock()
		receivedPendingJobID = append(receivedPendingJobID, jobID)
		mutex.Unlock()
	}

	return
}

func recoverRunningTask(
	ctx context.Context,
	jobID string,
	jobConfig *pb_job.JobConfig,
	configAddOn *models.ConfigAddOn,
	jobRuntime *pb_job.RuntimeInfo,
	batch TasksBatch,
	errChan chan<- error) {
	if jobID != runningJobID.GetValue() {
		err := fmt.Errorf("Got the wrong job id")
		errChan <- err
	} else {
		mutex.Lock()
		receivedPendingJobID = append(receivedPendingJobID, jobID)
		mutex.Unlock()
	}

	return
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
	receivedPendingJobID = append(receivedPendingJobID, jobID)
	mutex.Unlock()
	return
}

func recoverTaskRandomly(
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
	var jobStatesPending = []pb_job.JobState{
		pb_job.JobState_PENDING,
	}
	var jobStatesRunning = []pb_job.JobState{
		pb_job.JobState_RUNNING,
	}
	var jobStatesAll = []pb_job.JobState{
		pb_job.JobState_PENDING,
		pb_job.JobState_RUNNING,
		pb_job.JobState_FAILED,
	}

	ctx := context.Background()

	pendingJobID, err = createJob(ctx, pb_job.JobState_PENDING, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	runningJobID, err = createJob(ctx, pb_job.JobState_RUNNING, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	// this job should not be recovered
	_, err = createJob(ctx, pb_job.JobState_FAILED, pb_job.JobState_SUCCEEDED)
	assert.NoError(t, err)

	receivedPendingJobID = nil
	err = RecoverJobsByState(ctx, csStore, jobStatesPending, recoverPendingTask)
	assert.NoError(t, err)
	err = RecoverJobsByState(ctx, csStore, jobStatesRunning, recoverRunningTask)
	assert.NoError(t, err)
	err = RecoverJobsByState(ctx, csStore, jobStatesAll, recoverAllTask)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(receivedPendingJobID))
}

func TestRecoveryAfterJobDelete(t *testing.T) {
	var err error
	var jobStatesPending = []pb_job.JobState{
		pb_job.JobState_PENDING,
	}
	var jobRuntime = pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}
	var jobConfig = pb_job.JobConfig{
		Name:          "TestValidatorWithStore",
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "gsg9"},
		InstanceCount: 2,
	}
	var missingJobID = &peloton.JobID{Value: uuid.New()}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockJobStore := store_mocks.NewMockJobStore(ctrl)

	// recoverJobsBatch should pass even if there is no job_id present in job_runtime
	// it should just skip over to a new job. This test is specifically to test the
	// corner case where you deleted a job from job_runtime table but the materialized view
	// created on this table never got updated (this can happen because MV is
	// an experimental feature not supported by Cassandra)

	// mock GetJobsByStates to return missingJobID present in MV but
	// absent from job_runtime
	jobIDs := []peloton.JobID{*missingJobID, *pendingJobID}
	mockJobStore.EXPECT().
		GetJobsByStates(ctx, jobStatesPending).
		Return(jobIDs, nil).
		AnyTimes()

	mockJobStore.EXPECT().
		GetActiveJobs(ctx).
		Return([]*peloton.JobID{missingJobID, pendingJobID}, nil).
		AnyTimes()

	mockJobStore.EXPECT().
		GetJobRuntime(ctx, missingJobID).
		Return(nil, fmt.Errorf("Cannot find job wth jobID %v", missingJobID.GetValue())).
		AnyTimes()

	mockJobStore.EXPECT().
		GetJobRuntime(ctx, pendingJobID).
		Return(&jobRuntime, nil).
		AnyTimes()

	mockJobStore.EXPECT().
		GetJobConfig(ctx, pendingJobID).
		Return(&jobConfig, &models.ConfigAddOn{}, nil).
		AnyTimes()

	err = RecoverJobsByState(ctx, mockJobStore, jobStatesPending, recoverPendingTask)
	assert.NoError(t, err)

	mockJobStore.EXPECT().
		GetJobsByStates(ctx, jobStatesPending).
		Return([]peloton.JobID{}, nil).
		AnyTimes()
	// an error getting active_jobs should not result in recovery failure
	// because we do not use active_jobs for recovery just yet.
	mockJobStore.EXPECT().
		GetActiveJobs(ctx).
		Return([]*peloton.JobID{}, fmt.Errorf("")).
		AnyTimes()
	err = RecoverJobsByState(ctx, mockJobStore, jobStatesPending, recoverPendingTask)
	assert.NoError(t, err)
}

// TestRecoveryErrors tests RecoverJobsByState errors
func TestRecoveryErrors(t *testing.T) {
	jobStatesPending := []pb_job.JobState{
		pb_job.JobState_PENDING,
	}
	jobRuntime := pb_job.RuntimeInfo{
		State: pb_job.JobState_PENDING,
	}
	jobID := &peloton.JobID{Value: uuid.New()}

	ctrl := gomock.NewController(t)
	ctx := context.Background()
	mockJobStore := store_mocks.NewMockJobStore(ctrl)

	//Test GetJobsByStates error
	mockJobStore.EXPECT().
		GetJobsByStates(ctx, jobStatesPending).
		Return(nil, fmt.Errorf("Fake GetJobsByStates error"))
	err := RecoverJobsByState(ctx, mockJobStore, jobStatesPending, recoverPendingTask)
	assert.Error(t, err)

	// Test GetJobConfig error
	jobIDs := []peloton.JobID{*jobID}
	mockJobStore.EXPECT().
		GetJobsByStates(ctx, jobStatesPending).
		Return(jobIDs, nil)

	mockJobStore.EXPECT().
		GetActiveJobs(ctx).
		Return([]*peloton.JobID{jobID}, nil)

	mockJobStore.EXPECT().
		GetJobRuntime(ctx, jobID).
		Return(&jobRuntime, nil)

	mockJobStore.EXPECT().
		GetJobConfig(ctx, jobID).
		Return(nil, &models.ConfigAddOn{}, fmt.Errorf("Fake GetJobConfig error"))

	err = RecoverJobsByState(ctx, mockJobStore, jobStatesPending, recoverPendingTask)
	assert.Error(t, err)

	// Test active jobs != jobs in MV
	jobIDs = []peloton.JobID{*jobID}
	mockJobStore.EXPECT().
		GetJobsByStates(ctx, jobStatesPending).
		Return(jobIDs, nil)
	// even if active jobs returns an empty list, the recovery process should
	// move on without error
	mockJobStore.EXPECT().
		GetActiveJobs(ctx).
		Return([]*peloton.JobID{}, nil)

	mockJobStore.EXPECT().
		GetJobRuntime(ctx, jobID).
		Return(&jobRuntime, nil)

	mockJobStore.EXPECT().
		GetJobConfig(ctx, jobID).
		Return(nil, &models.ConfigAddOn{}, fmt.Errorf("Fake GetJobConfig error"))

	err = RecoverJobsByState(ctx, mockJobStore, jobStatesPending, recoverPendingTask)
	assert.Error(t, err)
}

// TestRecoveryWithFailedJobBatches test will create 100 jobs, and eventually 10 jobByBatches.
// recoverTaskRandomly will send error for recovering 11 jobs, so errChan will have at least 2 messages
// or max 10 messages if all job batches failed, here errChan will be full.
// TODO (varung): Add go routine leak test.
func TestRecoveryWithFailedJobBatches(t *testing.T) {
	var err error
	var jobStatesAll = []pb_job.JobState{
		pb_job.JobState_PENDING,
		pb_job.JobState_RUNNING,
		pb_job.JobState_FAILED,
	}
	count = 0

	ctx := context.Background()

	for i := 0; i < 50; i++ {
		_, err = createJob(ctx, pb_job.JobState_PENDING, pb_job.JobState_SUCCEEDED)
		assert.NoError(t, err)
	}

	for i := 0; i < 50; i++ {
		_, err = createJob(ctx, pb_job.JobState_RUNNING, pb_job.JobState_SUCCEEDED)
		assert.NoError(t, err)
	}

	err = RecoverJobsByState(ctx, csStore, jobStatesAll, recoverTaskRandomly)
	assert.Error(t, err)
}

func TestJobRecoveryWithUninitializedState(t *testing.T) {
	var err error

	ctx := context.Background()

	// these two jobs cannot be recovered
	_, err = createPartiallyCreatedJob(ctx, pb_job.JobState_RUNNING)
	assert.NoError(t, err)
	_, err = createPartiallyCreatedJob(ctx, pb_job.JobState_RUNNING)
	assert.NoError(t, err)

	// Although the state is UNINITIALIZED, config is persisted in db,
	// so this job can be recovered.
	_, err = createJob(ctx, pb_job.JobState_UNINITIALIZED, pb_job.JobState_RUNNING)
	assert.NoError(t, err)

	receivedPendingJobID = nil
	err = RecoverJobsByState(ctx, csStore, []pb_job.JobState{pb_job.JobState_UNINITIALIZED}, recoverAllTask)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(receivedPendingJobID))
}
