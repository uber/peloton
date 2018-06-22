package cached

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type JobTestSuite struct {
	suite.Suite

	ctrl      *gomock.Controller
	jobStore  *storemocks.MockJobStore
	taskStore *storemocks.MockTaskStore
	jobID     *peloton.JobID
	job       *job
	runtimes  map[uint32]*pbtask.RuntimeInfo
}

func TestJob(t *testing.T) {
	suite.Run(t, new(JobTestSuite))
}

func (suite *JobTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.job = initializeJob(suite.jobStore, suite.taskStore, suite.jobID)
}

func (suite *JobTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func initializeJob(jobStore *storemocks.MockJobStore, taskStore *storemocks.MockTaskStore, jobID *peloton.JobID) *job {
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

func initializeDiffs(instanceCount uint32, state pbtask.TaskState) map[uint32]map[string]interface{} {
	diffs := make(map[uint32]map[string]interface{})
	for i := uint32(0); i < instanceCount; i++ {
		diff := map[string]interface{}{
			"State": state,
		}
		diffs[i] = diff
	}
	return diffs
}

// TestJobFetchID tests fetching job ID.
func (suite *JobTestSuite) TestJobFetchID() {
	// Test fetching ID
	suite.Equal(suite.jobID, suite.job.ID())
}

// TestJobSetAndFetchConfigAndRuntime tests setting and fetching
// job configuration and runtime.
func (suite *JobTestSuite) TestJobSetAndFetchConfigAndRuntime() {
	// Test setting and fetching job config and runtime
	instanceCount := uint32(10)
	maxRunningInstances := uint32(2)
	maxRunningTime := uint32(5)
	jobRuntime := &pbjob.RuntimeInfo{
		State:     pbjob.JobState_RUNNING,
		GoalState: pbjob.JobState_SUCCEEDED,
	}
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
	jobInfo := &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}

	suite.job.config = nil
	suite.job.runtime = nil

	suite.job.Update(context.Background(), jobInfo, UpdateCacheOnly)
	actJobRuntime, _ := suite.job.GetRuntime(context.Background())
	actJobConfig, _ := suite.job.GetConfig(context.Background())
	suite.Equal(jobRuntime, actJobRuntime)
	suite.Equal(instanceCount, actJobConfig.GetInstanceCount())
	suite.Equal(pbjob.JobType_BATCH, suite.job.GetJobType())
	suite.Equal(maxRunningInstances, actJobConfig.GetSLA().GetMaximumRunningInstances())
	suite.Equal(maxRunningTime, actJobConfig.GetSLA().GetMaxRunningTime())
	suite.Equal(pbjob.JobType_BATCH, actJobConfig.GetType())
	suite.Equal(jobConfig.RespoolID.Value, actJobConfig.GetRespoolID().Value)
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
		Return(nil, fmt.Errorf("fake db error"))
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
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheAndDB)
	suite.NoError(err)
	actJobRuntime, err = suite.job.GetRuntime(context.Background())
	suite.Equal(jobRuntime.State, actJobRuntime.State)
	suite.Equal(jobRuntime.GoalState, actJobRuntime.GoalState)
	suite.NoError(err)

	// Test error in DB while update job runtime
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(fmt.Errorf("fake db error"))
	jobRuntime.State = pbjob.JobState_SUCCEEDED
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheAndDB)
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

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
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

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{Runtime: jobRuntime}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.State, jobRuntime.State)
	suite.Equal(suite.job.runtime.GoalState, jobRuntime.GoalState)
}

// TestJobUpdateConfig tests update job which new config
func (suite *JobTestSuite) TestJobUpdateConfig() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
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
	}

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(config.ChangeLog.Version, uint64(2))
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{Config: jobConfig}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, jobConfig.InstanceCount)
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.Revision.Version, uint64(2))
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetChangeLog().Version, uint64(2))
}

// TestJobUpdateConfig_RuntimeUpdateFailureRetry tests update to job config,
// with config db write success but runtime db write error.
// An retry upon failure should succeed if db stops returning error.
func (suite *JobTestSuite) TestJobUpdateConfig_RuntimeUpdateFailureRetry() {
	// the config object stored in db
	var latestJobConfigInDB *pbjob.JobConfig

	suite.job.runtime = nil
	suite.job.config = nil

	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
	}

	jobConfigInDB := &pbjob.JobConfig{
		InstanceCount: 5,
		ChangeLog: &peloton.ChangeLog{
			Version: 1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), gomock.Any()).
		Return(jobConfigInDB, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(jobConfigInDB.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.InstanceCount, uint32(10))
			suite.Equal(config.ChangeLog.Version, uint64(2))
			latestJobConfigInDB = config
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		}, nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(fmt.Errorf("test updateJobRuntime err"))

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{Config: jobConfig}, UpdateCacheAndDB)
	suite.Error(err)

	// retry the update should succeed
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), gomock.Any()).
		Return(jobConfigInDB, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(latestJobConfigInDB.ChangeLog.Version, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			Revision: &peloton.ChangeLog{
				Version: 1,
			},
		}, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.InstanceCount, uint32(10))
			// changeLog version should be the max config version in db + 1
			suite.Equal(config.ChangeLog.Version, uint64(latestJobConfigInDB.ChangeLog.Version+1))
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(latestJobConfigInDB.ChangeLog.Version+1))
			suite.Equal(runtime.Revision.Version, uint64(2))
		}).
		Return(nil)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{Config: jobConfig}, UpdateCacheAndDB)
	suite.NoError(err)
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
		}, nil)

	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(suite.job.config.changeLog.Version, nil)

	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, jobConfig *pbjob.JobConfig) {
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

	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: jobRuntime,
		Config:  jobConfig,
	}, UpdateCacheAndDB)
	suite.NoError(err)
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
	runtime := pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		ConfigurationVersion: 1,
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(time.Now().UnixNano()),
			UpdatedAt: uint64(time.Now().UnixNano()),
			Version:   1,
		},
	}

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, nil)
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&runtime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
		}).
		Return(nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, nil)
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
		}).
		Return(nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, nil)
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
		}).
		Return(nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
			suite.Equal(config.ChangeLog.Version, uint64(2))
			suite.Equal(config.InstanceCount, uint32(10))
		}).
		Return(nil)
	updatedConfig := initialConfig
	updatedConfig.InstanceCount = 10
	updatedConfig.ChangeLog = nil
	err := suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &updatedConfig,
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
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
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
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
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, UpdateCacheOnly)
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
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(1))
	suite.Equal(suite.job.runtime.Revision.Version, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
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

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(&initialConfig, nil)
	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), suite.jobID).
		Return(&initialRuntime, nil)
	suite.jobStore.EXPECT().
		GetMaxJobConfigVersion(gomock.Any(), suite.jobID).
		Return(initialConfig.ChangeLog.Version, nil)
	suite.jobStore.EXPECT().
		UpdateJobRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, runtime *pbjob.RuntimeInfo) {
			suite.Equal(runtime.ConfigurationVersion, uint64(2))
			suite.Equal(runtime.State, pbjob.JobState_SUCCEEDED)
		}).
		Return(nil)
	suite.jobStore.EXPECT().
		UpdateJobConfig(gomock.Any(), suite.jobID, gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig) {
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
	}, UpdateCacheAndDB)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover runtime only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover config only
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config: &initialConfig,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
	suite.Equal(suite.job.runtime.State, pbjob.JobState_SUCCEEDED)

	// recover both config and runtime
	err = suite.job.Update(context.Background(), &pbjob.JobInfo{
		Config:  &initialConfig,
		Runtime: &initialRuntime,
	}, UpdateCacheOnly)
	suite.NoError(err)
	suite.Equal(suite.job.config.instanceCount, uint32(10))
	suite.Equal(suite.job.runtime.ConfigurationVersion, uint64(2))
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
	suite.jobStore.EXPECT().
		CreateJobConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any(), createdBy).
		Do(func(_ context.Context, _ *peloton.JobID, config *pbjob.JobConfig, version uint64, createBy string) {
			suite.Equal(version, uint64(1))
			suite.Equal(config.ChangeLog.Version, uint64(1))
			suite.Equal(config.InstanceCount, jobConfig.InstanceCount)
			suite.Equal(config.Type, jobConfig.Type)
		}).
		Return(nil)

	suite.jobStore.EXPECT().
		CreateJobRuntimeWithConfig(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, _ *peloton.JobID, initialRuntime *pbjob.RuntimeInfo, config *pbjob.JobConfig) {
			suite.Equal(initialRuntime.State, pbjob.JobState_INITIALIZED)
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

	err := suite.job.Create(context.Background(), jobConfig, createdBy)
	suite.NoError(err)
	config, err := suite.job.GetConfig(context.Background())
	suite.NoError(err)
	suite.Equal(config.GetInstanceCount(), uint32(10))
	suite.Equal(config.GetChangeLog().Version, uint64(1))
	suite.Equal(suite.job.GetJobType(), pbjob.JobType_BATCH)
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.GetRevision().GetVersion(), uint64(1))
	suite.Equal(runtime.GetConfigurationVersion(), uint64(1))
	suite.Equal(runtime.GetState(), pbjob.JobState_INITIALIZED)
	suite.Equal(runtime.GetGoalState(), pbjob.JobState_SUCCEEDED)
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
	runtime, err := suite.job.GetRuntime(context.Background())
	suite.NoError(err)
	suite.Equal(runtime.State, jobRuntime.State)
	suite.Equal(runtime.GoalState, jobRuntime.GoalState)
}

func (suite *JobTestSuite) TestJobGetConfig() {
	suite.job.config = nil
	// Test the case there is no config cache and db returns err
	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("test error"))

	config, err := suite.job.GetConfig(context.Background())
	suite.Error(err)

	// Test the case there is no config cache and db returns no err
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
		RespoolID:     &peloton.ResourcePoolID{Value: uuid.NewRandom().String()},
	}

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), suite.jobID).
		Return(jobConfig, nil)

	config, err = suite.job.GetConfig(context.Background())
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
			CreateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any(), "peloton").
			Do(func(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *pbtask.RuntimeInfo, owner string) {
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
			CreateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any(), "peloton").
			Return(fmt.Errorf("fake db error"))
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

// TestTasksUpdateInDB tests updating task runtimes in DB.
func (suite *JobTestSuite) TestTasksUpdateInDB() {
	instanceCount := uint32(10)

	runtimes := initializeRuntimes(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any()).Return(nil)
	}

	// Update task runtimes in DB and cache
	err := suite.job.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	suite.NoError(err)

	// Validate the state of the tasks in cache is correct
	for instID := range runtimes {
		tt := suite.job.GetTask(instID)
		suite.NotNil(tt)
		actRuntime, _ := tt.GetRunTime(context.Background())
		suite.NotNil(actRuntime)
		suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
		suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
	}
}

// TestTasksUpdateDBError tests getting DB error during update task runtimes.
func (suite *JobTestSuite) TestTasksUpdateDBError() {
	instanceCount := uint32(10)
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	oldRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_LAUNCHED,
	}
	runtimes := make(map[uint32]*pbtask.RuntimeInfo)

	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.AddTask(i).(*task)
		tt.runtime = oldRuntime
		runtimes[i] = runtime
		// Simulate fake DB error
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any()).
			Return(fmt.Errorf("fake db error"))
	}
	err := suite.job.UpdateTasks(context.Background(), runtimes, UpdateCacheAndDB)
	suite.Error(err)
}

// TestTasksUpdateRuntimeSingleTask tests updating task runtime of a single task in DB.
func (suite *JobTestSuite) TestTasksUpdateRuntimeSingleTask() {
	runtime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
	tt := suite.job.AddTask(0).(*task)
	tt.runtime = oldRuntime

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	err := suite.job.UpdateTasks(context.Background(), map[uint32]*pbtask.RuntimeInfo{0: runtime}, UpdateCacheAndDB)
	suite.NoError(err)

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, _ := att.GetRunTime(context.Background())
	suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
	suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
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
	runtimeDiff := map[string]interface{}{
		"State": pbtask.TaskState_RUNNING,
	}

	suite.taskStore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), suite.jobID, gomock.Any(), gomock.Any()).
		Return(nil).Times(int(instanceCount))
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), suite.jobID, gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			Revision: &peloton.ChangeLog{Version: 1},
			State:    pbtask.TaskState_INITIALIZED,
		}, nil).Times(int(instanceCount))

	// Test updating tasks one at a time in cache
	for i := uint32(0); i < instanceCount; i++ {
		suite.job.PatchTasks(context.Background(), map[uint32]map[string]interface{}{i: runtimeDiff})
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
func (suite *JobTestSuite) TestPatchTasks_SetGetTasksMultiple() {
	instanceCount := uint32(10)

	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
		suite.taskStore.EXPECT().
			GetTaskRuntime(gomock.Any(), suite.jobID, i).Return(oldRuntime, nil)
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any()).Return(nil)
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
func (suite *JobTestSuite) TestPatchTasks_DBError() {
	instanceCount := uint32(10)
	diffs := initializeDiffs(instanceCount, pbtask.TaskState_RUNNING)

	for i := uint32(0); i < instanceCount; i++ {
		tt := suite.job.AddTask(i).(*task)
		tt.runtime = &pbtask.RuntimeInfo{
			State: pbtask.TaskState_LAUNCHED,
		}
		// Simulate fake DB error
		suite.taskStore.EXPECT().
			UpdateTaskRuntime(gomock.Any(), suite.jobID, i, gomock.Any()).
			Return(fmt.Errorf("fake db error"))
	}
	err := suite.job.PatchTasks(context.Background(), diffs)
	suite.Error(err)
}

// TestPatchTasks_SingleTask tests updating task runtime of a single task in DB.
func (suite *JobTestSuite) TestPatchTasks_SingleTask() {
	diffs := initializeDiffs(1, pbtask.TaskState_RUNNING)
	oldRuntime := initializeCurrentRuntime(pbtask.TaskState_LAUNCHED)
	tt := suite.job.AddTask(0).(*task)
	tt.runtime = oldRuntime

	// Update task runtime of only one task
	suite.taskStore.EXPECT().
		UpdateTaskRuntime(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	err := suite.job.PatchTasks(context.Background(), diffs)
	suite.NoError(err)

	// Validate the state of the task in cache is correct
	att := suite.job.GetTask(0)
	actRuntime, _ := att.GetRunTime(context.Background())
	suite.Equal(pbtask.TaskState_RUNNING, actRuntime.GetState())
	suite.Equal(uint64(2), actRuntime.GetRevision().GetVersion())
}
