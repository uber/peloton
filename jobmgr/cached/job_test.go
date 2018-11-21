package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

var dbError = yarpcerrors.UnavailableErrorf("db error")

type JobTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	jobStore  *storemocks.MockJobStore
	taskStore *storemocks.MockTaskStore
	jobID     *peloton.JobID
	job       *job
	listeners []*FakeJobListener
}

func TestJob(t *testing.T) {
	suite.Run(t, new(JobTestSuite))
}

func (suite *JobTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.listeners = append(suite.listeners,
		new(FakeJobListener),
		new(FakeJobListener))
	suite.job = suite.initializeJob(suite.jobStore, suite.taskStore,
		suite.jobID)
}

func (suite *JobTestSuite) TearDownTest() {
	suite.listeners = nil
	suite.ctrl.Finish()
}

func (suite *JobTestSuite) initializeJob(
	jobStore *storemocks.MockJobStore,
	taskStore *storemocks.MockTaskStore,
	jobID *peloton.JobID) *job {
	j := &job{
		id: jobID,
		jobFactory: &jobFactory{
			mtx:       NewMetrics(tally.NoopScope),
			jobStore:  jobStore,
			taskStore: taskStore,
			running:   true,
			jobs:      map[string]*job{},
		},
		tasks: map[uint32]*task{},
		config: &cachedConfig{
			changeLog: &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			},
		},
		runtime: &pbjob.RuntimeInfo{ConfigurationVersion: 1},
	}
	for _, l := range suite.listeners {
		j.jobFactory.listeners = append(j.jobFactory.listeners, l)
	}
	j.jobFactory.jobs[j.id.GetValue()] = j
	return j
}

func initializeRuntimes(instanceCount uint32, state pbtask.TaskState) map[uint32]*pbtask.RuntimeInfo {
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: state,
		}
		runtimes[i] = runtime
	}
	return runtimes
}

func initializeCurrentRuntime(state pbtask.TaskState) *pbtask.RuntimeInfo {
	runtime := &pbtask.RuntimeInfo{
		State: state,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	return runtime
}

func initializeCurrentRuntimes(instanceCount uint32, state pbtask.TaskState) map[uint32]*pbtask.RuntimeInfo {
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: state,
			Revision: &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			},
		}
		runtimes[i] = runtime
	}
	return runtimes
}

func initializeDiffs(instanceCount uint32, state pbtask.TaskState) map[uint32]jobmgrcommon.RuntimeDiff {
	diffs := make(map[uint32]jobmgrcommon.RuntimeDiff)
	for i := uint32(0); i < instanceCount; i++ {
		diff := jobmgrcommon.RuntimeDiff{
			jobmgrcommon.StateField: state,
		}
		diffs[i] = diff
	}
	return diffs
}

// checkListeners verifies that listeners received the correct data
func (suite *JobTestSuite) checkListeners() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Equal(suite.jobID, l.jobID, msg)
		suite.Equal(suite.job.GetJobType(), l.jobType, msg)
		suite.Equal(suite.job.runtime, l.jobRuntime, msg)
	}
}

// checkListenersNotCalled verifies that listeners did not get invoked
func (suite *JobTestSuite) checkListenersNotCalled() {
	suite.NotZero(len(suite.listeners))
	for i, l := range suite.listeners {
		msg := fmt.Sprintf("Listener %d", i)
		suite.Nil(l.jobID, msg)
		suite.Nil(l.jobRuntime, msg)
	}
}

// TestJobFetchID tests fetching job ID.
func (suite *JobTestSuite) TestJobFetchID() {
	// Test fetching ID
	suite.Equal(suite.jobID, suite.job.ID())
}

// TestJobAddTask tests adding a task and recovering it as well
func (suite *JobTestSuite) TestJobAddTask() {
	instID := uint32(1)
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(runtime, nil)

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.NoError(err)
	suite.Equal(runtime, (t.(*task)).runtime)
	suite.Equal(t, suite.job.tasks[instID])
}

// TestJobAddTaskDBError tests returning error from DB while fetching the
// task runtime when adding a task and recovering it
func (suite *JobTestSuite) TestJobAddTaskDBError() {
	instID := uint32(1)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, fmt.Errorf("fake db error"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.Error(err)
	suite.EqualError(err, "fake db error")
	suite.Nil(t)
}

// TestJobAddTaskNotFound tests adding a task not in DB
func (suite *JobTestSuite) TestJobAddTaskNotFound() {
	instID := uint32(5)
	suite.job.config = nil
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 2,
		Type:          pbjob.JobType_SERVICE,
	}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, &models.ConfigAddOn{}, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, yarpcerrors.NotFoundErrorf("not found"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.Equal(err, InstanceIDExceedsInstanceCountError)
	suite.Nil(t)
}

// TestJobAddTaskJobConfigGetError tests returning error from DB during
// the fetch of job configuration when adding and recovering a task
func (suite *JobTestSuite) TestJobAddTaskJobConfigGetError() {
	instID := uint32(5)
	suite.job.config = nil
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, instID).
		Return(nil, yarpcerrors.NotFoundErrorf("not found"))
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, nil, fmt.Errorf("fake db error"))

	t, err := suite.job.AddTask(context.Background(), instID)
	suite.Error(err)
	suite.EqualError(err, "fake db error")
	suite.Nil(t)
}

// TestJobSetAndFetchConfigAndRuntime tests setting and fetching
// job configuration and runtime.
func (suite *JobTestSuite) TestJobSetAndFetchConfigAndRuntime() {
	// Test setting and fetching job config and runtime
	instanceCount := uint32(10)
	maxRunningInstances := uint32(2)
	maxRunningTime := uint32(5)
	jobConfig := &pbjob.JobConfig{
		SLA: &pbjob.SlaConfig{
			MaximumRunningInstances: maxRunningInstances,
			MaxRunningTime:          maxRunningTime,
		},
		InstanceCount: instanceCount,
		Type:          pbjob.JobType_BATCH,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}
	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_SUCCEEDED,
		UpdateID:             &peloton.UpdateID{Value: uuid.NewRandom().String()},
		ConfigurationVersion: jobConfig.GetChangeLog().GetVersion(),
	}
	jobInfo := &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}

	suite.job.config = nil
	suite.job.runtime = nil

	suite.job.Update(
		context.Background(),
		jobInfo,
		&models.ConfigAddOn{},
		UpdateCacheOnly)
	actJobRuntime, _ := suite.job.GetRuntime(context.Background())
	actJobConfig, _ := suite.job.GetConfig(context.Background())
	suite.Equal(jobRuntime, actJobRuntime)
	suite.Equal(instanceCount, actJobConfig.GetInstanceCount())
	suite.Equal(pbjob.JobType_BATCH, suite.job.GetJobType())
	suite.Equal(maxRunningInstances, actJobConfig.GetSLA().GetMaximumRunningInstances())
	suite.Equal(maxRunningTime, actJobConfig.GetSLA().GetMaxRunningTime())
	suite.Equal(pbjob.JobType_BATCH, actJobConfig.GetType())
	suite.Equal(jobConfig.RespoolID.Value, actJobConfig.GetRespoolID().Value)
	suite.Equal(jobRuntime.UpdateID.Value, actJobRuntime.GetUpdateID().Value)
	suite.checkListenersNotCalled()
}

// TestJobDBError tests DB errors during job operations.
func (suite *JobTestSuite) TestJobDBError() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	// Test db error in fetching job runtime
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(nil, dbError)
	// reset runtime to trigger load from db
	suite.job.runtime = nil
	actJobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.Error(err)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
		}, nil)
	// Test updating job runtime in DB and cache
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	actJobRuntime, err = suite.job.GetRuntime(context.Background())
	suite.Equal(jobRuntime.State, actJobRuntime.GetState())
	suite.Equal(jobRuntime.GoalState, actJobRuntime.GetGoalState())
	suite.NoError(err)
	suite.checkListeners()

	// Test error in DB while update job runtime
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(dbError)
	jobRuntime.State = pbjob.JobState_SUCCEEDED
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		UpdateCacheAndDB)
	suite.Error(err)
	suite.Nil(suite.job.runtime)
	suite.Nil(suite.job.config)
}

// TestJobUpdateRuntimeWithNoRuntimeCache tests update job which has
// no existing cache
func (suite *JobTestSuite) TestJobUpdateRuntimeWithNoCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
		}, nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)

	suite.job.runtime = nil
	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.checkListeners()
}

// TestJobUpdateRuntimeWithNoRuntimeCache tests update job which has
// existing cache
func (suite *JobTestSuite) TestJobUpdateRuntimeWithCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntime},
		nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeWithCache tests replace job runtime which has
// existing cache
func (suite *JobTestSuite) TestJobCompareAndSetRuntimeWithCache() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision:  revision,
	}

	jobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.GetState())
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GetGoalState())

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.GetState())
			suite.Equal(runtime.GoalState, jobRuntime.GetGoalState())
		}).
		Return(nil)

	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.GetState())
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GetGoalState())
	suite.Equal(suite.job.runtime.GetRevision().GetVersion(), revision.GetVersion()+1)
	suite.checkListeners()
	for _, l := range suite.listeners {
		l.Reset()
	}

	// second call with the same jobRuntime should fail due to concurrency check
	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobUpdateCompareAndSetRuntimeMixedUsage tests using CompareAndSetRuntime
// and update together
func (suite *JobTestSuite) TestJobUpdateCompareAndSetRuntimeMixedUsage() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	jobRuntimeUpdate := &pbjob.RuntimeInfo{
		State: pbjob.JobState_PENDING,
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision:  revision,
	}

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntimeUpdate.State)
		}).
		Return(nil)

	jobRuntime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	err = suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Runtime: jobRuntimeUpdate},
		nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	for _, l := range suite.listeners {
		l.Reset()
	}

	// should fail because jobRuntime is already outdated due to Update
	jobRuntime.State = pbjob.JobState_KILLED
	_, err = suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeUnexpectedVersionError tests replace job runtime
// which fails due to incorrect version number
func (suite *JobTestSuite) TestJobCompareAndSetRuntimeUnexpectedVersionError() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_KILLED,
		Revision: &peloton.ChangeLog{
			Version: 2,
		},
	}

	suite.job.runtime = &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.Error(err)
	suite.checkListenersNotCalled()
}

// TestJobCompareAndSetRuntimeNoCache tests replace job runtime which has
// no existing cache
func (suite *JobTestSuite) TestJobCompareAndSetRuntimeNoCache() {
	revision := &peloton.ChangeLog{
		Version: 1,
	}

	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_KILLED,
		Revision:  revision,
	}

	suite.job.runtime = nil

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
			Revision:  revision,
		}, nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)

	_, err := suite.job.CompareAndSetRuntime(context.Background(), jobRuntime)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.Equal(suite.job.runtime.GetRevision().GetVersion(), revision.GetVersion()+1)
	suite.checkListeners()
}

// TestJobUpdateConfig tests update job which new config
func (suite *JobTestSuite) TestJobUpdateConfig() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}

	suite.job.config = &cachedConfig{
		instanceCount: 5,
		changeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}
	suite.job.runtime = &pbjob.RuntimeInfo{
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
		ConfigurationVersion: suite.job.config.changeLog.Version,
	}

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(config.ChangeLog.Version, uint64(2))
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(nil)

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Config: jobConfig},
		&models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, jobConfig.InstanceCount)
	_, err = suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.checkListenersNotCalled()
	// update runtime to point to the new config
	err = suite.job.Update(context.Background(),
		&pbjob.JobInfo{Runtime: &pbjob.RuntimeInfo{
			ConfigurationVersion: 2,
		}}, nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetChangeLog().Version, uint64(2))
}

// TestJobUpdateConfig tests update job which new config
func (suite *JobTestSuite) TestJobUpdateConfigIncorectChangeLog() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: 3,
		},
	}

	suite.job.config = &cachedConfig{
		instanceCount: 5,
		changeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}

	err := suite.job.Update(
		context.Background(),
		&pbjob.JobInfo{Config: jobConfig},
		&models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.True(yarpcerrors.IsInvalidArgument(err))
	suite.EqualError(err,
		"code:invalid-argument message:invalid job configuration version")
	suite.NotEqual(suite.job.config.instanceCount, jobConfig.InstanceCount)
	suite.checkListenersNotCalled()
}

// TestJobUpdateRuntimeAndConfig tests update both runtime
// and config at the same time
func (suite *JobTestSuite) TestJobUpdateRuntimeAndConfig() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}

	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog:     &peloton.ChangeLog{Version: 1},
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_INITIALIZED,
			GoalState: pbjob.JobState_SUCCEEDED,
			Revision: &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			},
			ConfigurationVersion: 1,
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(jobConfig.InstanceCount, uint32(10))
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, jobRuntime.State)
			suite.Equal(runtime.GoalState, jobRuntime.GoalState)
		}).
		Return(nil)

	suite.job.runtime = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.checkListeners()
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
	suite.Equal(suite.job.config.instanceCount, jobConfig.InstanceCount)
}

// Test the case job update config when there is no config in cache,
// and later recover the config in cache which should not
// overwrite updated cache
func (suite *JobTestSuite) TestJobUpdateConfigAndRecoverConfig() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
}

// Test the case job update config when there is no config in cache,
// and later recover the runtime which should not interfere with each other
func (suite *JobTestSuite) TestJobUpdateConfigAndRecoverRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(1))
}

// Test the case job update config when there is no config in cache,
// and later recover the runtime and config
func (suite *JobTestSuite) TestJobUpdateConfigAndRecoverConfigPlusRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(*configAddOn, *addOn)
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))

	// Test 'ConfigAddOn cannot be nil error'
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, nil,
		UpdateCacheOnly)
	suite.Error(err)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the runtime in cache which should not
// overwrite updated cache
func (suite *JobTestSuite) TestJobUpdateRuntimeAndRecoverRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the config which should not interfere with each other
func (suite *JobTestSuite) TestJobUpdateRuntimeAndRecoverConfig() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// Test the case job update runtime when there is no runtime in cache,
// and later recover the runtime and config
func (suite *JobTestSuite) TestJobUpdateRuntimeAndRecoverConfigPlusRuntime() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
			suite.Equal(runtime.ConfigurationVersion, uint64(1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &updateRuntime,
	}, nil,
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

func (suite *JobTestSuite) TestJobUpdateRuntimePlusConfigAndRecover() {
	suite.job.config = nil
	suite.job.runtime = nil

	initialConfig := pbjob.JobConfig{
		Name:          "test_job",
		Type:          pbjob.JobType_BATCH,
		InstanceCount: 5,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}
	initialRuntime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	configAddOn := &models.ConfigAddOn{}
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, configAddOn, nil)
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
		}).
		Return(nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil

	updateRuntime := pbjob.RuntimeInfo{
		State: pbjob.JobState_SUCCEEDED,
	}

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &updatedConfig,
		Runtime: &updateRuntime,
	}, &models.ConfigAddOn{},
		UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover runtime only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, nil,
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover config only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover both config and runtime
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, &models.ConfigAddOn{},
		UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)
}

// TestJobCreate tests job create in cache and db
func (suite *JobTestSuite) TestJobCreate() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		Type:          pbjob.JobType_BATCH,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
	}

	createdBy := "test"
	configAddOn := &models.ConfigAddOn{
		SystemLabels: []*peloton.Label{
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobOwner),
				Value: createdBy,
			},
			{
				Key: fmt.Sprintf(
					common.SystemLabelKeyTemplate,
					common.SystemLabelPrefix,
					common.SystemLabelJobType),
				Value: jobConfig.Type.String(),
			},
		},
	}

	suite.jobStore.EXPECT().
		CreateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any(), gomock.Any(), createdBy).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, addOn *models.ConfigAddOn, version uint64, createBy string) {
			suite.Equal(version, uint64(1))
			suite.Equal(config.ChangeLog.Version, uint64(1))
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
			suite.Len(addOn.SystemLabels, len(configAddOn.SystemLabels))
			for i := 0; i < len(configAddOn.SystemLabels); i++ {
				suite.Equal(configAddOn.SystemLabels[i].GetKey(), addOn.SystemLabels[i].GetKey())
				suite.Equal(configAddOn.SystemLabels[i].GetValue(), addOn.SystemLabels[i].GetValue())
			}
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		AddActiveJob(gomock.Any(), suite.jobID).Return(nil)

	suite.jobStore.EXPECT().
		CreateJobRuntimeWithConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, initialRuntime *pbjob.RuntimeInfo, config *pbjob.JobConfig) {
			suite.Equal(initialRuntime.State, pbjob.JobState_UNINITIALIZED)
			suite.Equal(initialRuntime.GoalState, pbjob.JobState_SUCCEEDED)
			suite.Equal(initialRuntime.Revision.Version, uint64(1))
			suite.Equal(initialRuntime.ConfigurationVersion, config.ChangeLog.Version)
			suite.Equal(initialRuntime.TaskStats[pbjob.JobState_INITIALIZED.String()], jobConfig.InstanceCount)
			suite.Equal(len(initialRuntime.TaskStats), 1)

			suite.Equal(config.ChangeLog.Version, uint64(1))
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
		}).Return(nil)

	err := suite.job.Create(context.Background(), jobConfig, configAddOn, createdBy)
	suite.NoError(err)
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), uint32(10))
	suite.Equal(config.GetChangeLog().Version, uint64(1))
	suite.Equal(config.GetType(), pbjob.JobType_BATCH)
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetRevision().GetVersion(), uint64(1))
	suite.Equal(runtime.GetConfigurationVersion(), uint64(1))
	suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
	suite.Equal(runtime.GetGoalState(), pbjob.JobState_SUCCEEDED)
	suite.checkListeners()
}

// TestJobGetRuntimeRefillCache tests job would refill runtime cache
// if cache is missing
func (suite *JobTestSuite) TestJobGetRuntimeRefillCache() {
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_INITIALIZED,
		GoalState: pbjob.JobState_SUCCEEDED,
	}
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.job.id).
		Return(jobRuntime, nil)
	suite.job.runtime = nil
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetState(), jobRuntime.State)
	suite.Equal(runtime.GetGoalState(), jobRuntime.GoalState)
}

func (suite *JobTestSuite) TestJobGetConfigDBError() {
	suite.job.config = nil
	// Test the case there is no config cache and db returns err
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, nil, fmt.Errorf("test error"))

	config, err := suite.job.GetConfig(context.Background())
	suite.Error(err)
	suite.Nil(config)
}

func (suite *JobTestSuite) TestJobGetConfigSuccess() {
	suite.job.config = nil
	// Test the case there is no config cache and db returns no err
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
		ChangeLog: &peloton.ChangeLog{
			Version: suite.job.runtime.ConfigurationVersion,
		},
	}

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, &models.ConfigAddOn{}, nil)

	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), jobConfig.GetInstanceCount())
	suite.Nil(config.GetSLA())

	// Test the case there is config cache after the first call to
	// GetConfig
	config, err = suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), jobConfig.GetInstanceCount())
	suite.Nil(config.GetSLA())
}

func (suite *JobTestSuite) TestJobIsControllerTask() {
	tests := []struct {
		config         *pbjob.JobConfig
		expectedResult bool
	}{
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: false},
		},
			false},
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: true},
		},
			true},
		{&pbjob.JobConfig{
			DefaultConfig: &pbtask.TaskConfig{Controller: false},
			InstanceConfig: map[uint32]*pbtask.TaskConfig{
				0: {Controller: true},
			},
		},
			true},
	}

	for index, test := range tests {
		suite.job.config = nil

		suite.jobStore.EXPECT().
			GetJobConfig(gomock.Any(), suite.jobID).
			Return(test.config, &models.ConfigAddOn{}, nil)

		config, err := suite.job.GetConfig(context.Background())
		suite.NoError(err)
		suite.Equal(HasControllerTask(config), test.expectedResult, "test:%d fails", index)
	}
}

// TestJobSetJobUpdateTime tests update the task update time coming from mesos.
func (suite *JobTestSuite) TestJobSetJobUpdateTime() {
	// Test setting and fetching job update time
	updateTime := float64(time.Now().UnixNano())
	suite.job.SetTaskUpdateTime(&updateTime)
	suite.Equal(updateTime, suite.job.GetFirstTaskUpdateTime())
	suite.Equal(updateTime, suite.job.GetLastTaskUpdateTime())
}

// TestJobCreateTasks tests creating task runtimes in cache and DB
func (suite *JobTestSuite) TestJobCreateTasks() {
	instanceCount := uint32(10)
	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)

	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			CreateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				"peloton",
				gomock.Any()).
			Do(func(
				ctx context.Context,
				jobID *peloton.JobID,
				instanceID uint32,
				runtime *pbtask.RuntimeInfo,
				owner string,
				jobType pbjob.JobType) {
				suite.Equal(pbtask.TaskState_INITIALIZED, runtime.GetState())
				suite.Equal(uint64(1), runtime.GetRevision().GetVersion())
			}).Return(nil)
	}

	err := suite.job.CreateTasks(context.Background(), runtimes, "peloton")
	suite.NoError(err)

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		suite.NotNil(tt)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.NotNil(actRuntime)
		suite.Equal(pbtask.TaskState_INITIALIZED, actRuntime.GetState())
		suite.Equal(uint64(1), actRuntime.GetRevision().GetVersion())
	}
}

// TestJobCreateTasksWithDBError tests getting DB error while creating task runtimes
func (suite *JobTestSuite) TestJobCreateTasksWithDBError() {
	instanceCount := uint32(10)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_INITIALIZED)
	for i := uint32(0); i < instanceCount; i++ {
		suite.taskStore.EXPECT().
			CreateTaskRuntime(gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				"peloton",
				pbjob.JobType_BATCH).
			Return(dbError)
	}

	err := suite.job.CreateTasks(context.Background(), runtimes, "peloton")
	suite.Error(err)
}

// TestSetGetTasksInJobInCacheSingle tests setting and getting single task in job in cache.
func (suite *JobTestSuite) TestSetGetTasksInJobInCacheSingle() {
	instanceCount := uint32(10)
	runtime := pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
		Revision: &peloton.ChangeLog{
			Version: 1,
		},
	}

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		suite.job.ReplaceTasks(map[uint32]*pbtask.RuntimeInfo{i: &runtime}, false)
	}
	suite.Equal(instanceCount, uint32(len(suite.job.tasks)))

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.Equal(runtime, *actRuntime)
	}
}

// TestSetGetTasksInJobInCacheBlock tests setting and getting tasks as a chunk in a job in cache.
func (suite *JobTestSuite) TestSetGetTasksInJobInCacheBlock() {
	instanceCount := uint32(10)
	// Test updating tasks in one call in cache
	runtimes := initializeCurrentRuntimes(instanceCount, pbtask.TaskState_SUCCEEDED)
	suite.job.ReplaceTasks(runtimes, false)

	// Validate the state of the tasks in cache is correct
	for instID, runtime := range runtimes {
		tt := suite.job.GetTask(instID)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.Equal(runtime, actRuntime)
	}
}

// TestTasksGetAllTasks tests getting all tasks.
func (suite *JobTestSuite) TestTasksGetAllTasks() {
	instanceCount := uint32(10)
	runtimes := initializeCurrentRuntimes(instanceCount, pbtask.TaskState_RUNNING)
	suite.job.ReplaceTasks(runtimes, false)

	// Test get all tasks
	ttMap := suite.job.GetAllTasks()
	suite.Equal(instanceCount, uint32(len(ttMap)))
}

// TestPartialJobCheck checks the partial job check.
func (suite *JobTestSuite) TestPartialJobCheck() {
	instanceCount := uint32(10)

	runtimes := initializeCurrentRuntimes(instanceCount, pbtask.TaskState_RUNNING)
	suite.job.ReplaceTasks(runtimes, false)

	// Test partial job check
	suite.job.config.instanceCount = 20
	suite.True(suite.job.IsPartiallyCreated(suite.job.config))
}

// TestPatchTasks_SetGetTasksSingle tests setting and getting single task in job in cache.
func (suite *JobTestSuite) TestPatchTasks_SetGetTasksSingle() {
	instanceCount := uint32(10)
	RuntimeDiff := jobmgrcommon.RuntimeDiff{
		jobmgrcommon.StateField: pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			suite.jobID,
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil).Times(int(instanceCount))
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			Revision: &peloton.ChangeLog{Version: 1},
			State:    pbtask.TaskState_INITIALIZED,
		}, nil).Times(int(instanceCount))

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		suite.job.PatchTasks(context.Background(), map[uint32]jobmgrcommon.RuntimeDiff{i: RuntimeDiff})
	}
	suite.Equal(instanceCount, uint32(len(suite.job.tasks)))

	// Validate the state of the tasks in cache is correct
	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.GetTask(i)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.Equal(pbtask.TaskState_RUNNING, actRuntime.State)
	}
}

// TestReplaceTasks tests setting and getting tasks as a chunk in a job in cache.
func (suite *JobTestSuite) TestReplaceTasks() {
	instanceCount := uint32(10)
	// Test updating tasks in one call in cache
	runtimes := initializeCurrentRuntimes(instanceCount, pbtask.TaskState_SUCCEEDED)
	suite.job.ReplaceTasks(runtimes, false)

	// Validate the state of the tasks in cache is correct
	for instID, runtime := range runtimes {
		tt := suite.job.GetTask(instID)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.Equal(runtime, actRuntime)
	}
}

//
// TestPatchTasks_SetGetTasksMultiple tests patching runtime in multiple
func (suite *JobTestSuite) TestPatchTasksSetGetTasksMultiple() {
	instanceCount := uint32(10)

	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				gomock.Any()).Return(nil)
	}

	err := suite.job.PatchTasks(context.Background(), diffs)
	suite.NoError(err)

	// Validate the state of the tasks in cache is correct
	for instID := range diffs {
		tt := suite.job.GetTask(instID)
		suite.NotNil(tt)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.NotNil(actRuntime)
		suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
		suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
	}
}

// TestPatchTasks_DBError tests getting DB error during update task runtimes.
func (suite *JobTestSuite) TestPatchTasksDBError() {
	instanceCount := uint32(10)
	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.addTaskToJobMap(i)
		tt.runtime = &pbtask.RuntimeInfo{
			State: pbtask.TaskState_LAUNCHED,
		}
		// Simulate fake DB error
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(
				gomock.Any(),
				suite.jobID,
				i,
				gomock.Any(),
				gomock.Any()).
			Return(dbError)
	}
	err := suite.job.PatchTasks(context.Background(), diffs)
	suite.Error(err)
}

// TestPatchTasks_SingleTask tests updating task runtime of a single task in DB.
func (suite *JobTestSuite) TestPatchTasksSingleTask() {
	diffs := initializeDiffs(1, pbtask.TaskState_RUNNING)
	oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
	tt := suite.job.addTaskToJobMap(0)
	tt.runtime = oldRuntime

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any()).
		Return(nil)
	err := suite.job.PatchTasks(context.Background(), diffs)
	suite.NoError(err)

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, _ := att.GetRunTime(context.Background())
	suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
	suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
}

// TestJobUpdateResourceUsage tests updating the resource usage for job
func (suite *JobTestSuite) TestJobUpdateResourceUsage() {
	taskResourceUsage := map[string]float64{
		common.CPU:    float64(1),
		common.GPU:    float64(0),
		common.MEMORY: float64(1)}

	suite.job.resourceUsage = map[string]float64{
		common.CPU:    float64(10),
		common.GPU:    float64(2),
		common.MEMORY: float64(10)}

	suite.job.UpdateResourceUsage(taskResourceUsage)
	updatedResourceUsage := map[string]float64{
		common.CPU:    float64(11),
		common.GPU:    float64(2),
		common.MEMORY: float64(11)}
	suite.Equal(updatedResourceUsage, suite.job.GetResourceUsage())
}

// TestRecalculateResourceUsage tests recalculating the resource usage for job
// on recovery
func (suite *JobTestSuite) TestRecalculateResourceUsage() {
	var oldRuntime *pbtask.RuntimeInfo

	instanceCount := uint32(5)
	initialResourceMap := map[string]float64{
		common.CPU:    float64(5),
		common.GPU:    float64(0),
		common.MEMORY: float64(5)}

	// Make sure initial resource map points to expected values
	suite.job.resourceUsage = initialResourceMap
	suite.Equal(initialResourceMap, suite.job.GetResourceUsage())

	// Add 5 tasks to the job of which 2 are terminal and have valid
	// resource usage.
	for i := uint32(0); i < instanceCount; i++ {
		if i%2 == 0 {
			// initialize two terminal tasks
			oldRuntime = initializeCurrentRuntime(pbtask.TaskState_RUNNING)
		} else {
			oldRuntime = initializeCurrentRuntime(pbtask.TaskState_SUCCEEDED)
			oldRuntime.ResourceUsage = map[string]float64{
				common.CPU:    float64(3),
				common.GPU:    float64(0),
				common.MEMORY: float64(3)}
		}
		suite.job.addTaskToJobMap(i)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
	}
	suite.job.RecalculateResourceUsage(context.Background())

	// initial resource map for this job should be reset and recalculated
	// by adding resource usage of the two terminal tasks
	updatedResourceUsage := map[string]float64{
		common.CPU:    float64(6),
		common.GPU:    float64(0),
		common.MEMORY: float64(6)}
	suite.Equal(updatedResourceUsage, suite.job.GetResourceUsage())
}

// TestRecalculateResourceUsageError tests DB error in recalculating the
// resource usage for job on recovery
func (suite *JobTestSuite) TestRecalculateResourceUsageError() {
	initialResourceMap := map[string]float64{
		common.CPU:    float64(5),
		common.GPU:    float64(0),
		common.MEMORY: float64(5)}
	suite.job.resourceUsage = initialResourceMap

	suite.job.addTaskToJobMap(uint32(0))
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, uint32(0)).
		Return(nil, dbError)

	// This will reset the resource usage map to 0 and try to update it with
	// terminal task resource usage. But on getting error in GetTaskRuntime,
	// it will log the error and move on. So the resource usage will stay at 0.
	suite.job.RecalculateResourceUsage(context.Background())
	suite.Equal(createEmptyResourceUsageMap(), suite.job.GetResourceUsage())
}

func (suite *JobTestSuite) TestJobRemoveTask() {
	suite.job.addTaskToJobMap(0)
	suite.NotNil(suite.job.GetTask(0))
	suite.job.RemoveTask(0)
	suite.Nil(suite.job.GetTask(0))
}

// TestJobCompareAndSetConfigSuccess tests the success case of
// CompareAndSetConfig
func (suite *JobTestSuite) TestJobCompareAndSetConfigSuccess() {
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)

	newConfig, err := suite.job.CompareAndSetConfig(
		context.Background(),
		&pbjob.JobConfig{
			InstanceCount: 100,
			ChangeLog: &peloton.ChangeLog{
				Version: suite.job.config.changeLog.Version,
			},
		}, &models.ConfigAddOn{},
	)
	suite.NoError(err)
	suite.Equal(newConfig.GetInstanceCount(), uint32(100))
}

// TestJobCompareAndSetConfigInvalidConfigVersion tests CompareAndSetConfig
// fails due to invalid version
func (suite *JobTestSuite) TestJobCompareAndSetConfigInvalidConfigVersion() {
	newConfig, err := suite.job.CompareAndSetConfig(
		context.Background(),
		&pbjob.JobConfig{
			InstanceCount: 100,
			ChangeLog: &peloton.ChangeLog{
				Version: suite.job.config.changeLog.Version - 1,
			},
		}, &models.ConfigAddOn{},
	)
	suite.Error(err)
	suite.Nil(newConfig)
}
