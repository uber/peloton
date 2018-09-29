package handler

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type HandlerCacheTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	jobStore    *storemocks.MockJobStore
	jobFactory  *cachedmocks.MockJobFactory
	cachedJob   *cachedmocks.MockJob
	jobID       *peloton.JobID
	runtimeInfo *job.RuntimeInfo
	config      *job.JobConfig
}

func TestHandlerCache(t *testing.T) {
	suite.Run(t, new(HandlerCacheTestSuite))
}

func (suite *HandlerCacheTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.jobID = &peloton.JobID{Value: uuid.New()}
	suite.runtimeInfo = &job.RuntimeInfo{
		State: job.JobState_RUNNING,
	}
	suite.config = &job.JobConfig{
		InstanceCount: 10,
	}
}

func (suite *HandlerCacheTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheHit() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(suite.runtimeInfo, nil)
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.NoError(err)
	suite.Equal(runtime.GetState(), suite.runtimeInfo.GetState())
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheHitWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("cache err"))
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.Error(err)
	suite.Nil(runtime)
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheMiss() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobStore.EXPECT().GetJobRuntime(gomock.Any(), suite.jobID).
		Return(suite.runtimeInfo, nil)
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.NoError(err)
	suite.Equal(runtime.GetState(), suite.runtimeInfo.GetState())
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheMissWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobStore.EXPECT().GetJobRuntime(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("db error"))
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.Error(err)
	suite.Nil(runtime)
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheHit() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).
		Return(suite.config, nil)
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.NoError(err)
	suite.Equal(suite.config.GetInstanceCount(), config.GetInstanceCount())
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheHitWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).
		Return(nil, fmt.Errorf("cache err"))
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.Error(err)
	suite.Nil(config)
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheMiss() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobStore.EXPECT().GetJobConfig(gomock.Any(), suite.jobID).
		Return(suite.config, &models.ConfigAddOn{}, nil)
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.NoError(err)
	suite.Equal(suite.config.GetInstanceCount(), config.GetInstanceCount())
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheMissWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobStore.EXPECT().GetJobConfig(gomock.Any(), suite.jobID).
		Return(nil, nil, fmt.Errorf("db error"))
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobStore)
	suite.Error(err)
	suite.Nil(config)
}
