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

package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/models"

	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type HandlerCacheTestSuite struct {
	suite.Suite

	ctrl          *gomock.Controller
	jobConfigOps  *objectmocks.MockJobConfigOps
	jobRuntimeOps *objectmocks.MockJobRuntimeOps
	jobFactory    *cachedmocks.MockJobFactory
	cachedJob     *cachedmocks.MockJob
	jobID         *peloton.JobID
	runtimeInfo   *job.RuntimeInfo
	config        *job.JobConfig
}

func TestHandlerCache(t *testing.T) {
	suite.Run(t, new(HandlerCacheTestSuite))
}

func (suite *HandlerCacheTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.jobConfigOps = objectmocks.NewMockJobConfigOps(suite.ctrl)
	suite.jobRuntimeOps = objectmocks.NewMockJobRuntimeOps(suite.ctrl)
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
		context.Background(),
		suite.jobID,
		suite.jobFactory,
		suite.jobRuntimeOps)
	suite.NoError(err)
	suite.Equal(runtime.GetState(), suite.runtimeInfo.GetState())
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheHitWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("cache err"))
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(),
		suite.jobID,
		suite.jobFactory,
		suite.jobRuntimeOps)
	suite.Error(err)
	suite.Nil(runtime)
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheMiss() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobRuntimeOps.EXPECT().Get(gomock.Any(), suite.jobID).
		Return(suite.runtimeInfo, nil)
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(),
		suite.jobID,
		suite.jobFactory,
		suite.jobRuntimeOps)
	suite.NoError(err)
	suite.Equal(runtime.GetState(), suite.runtimeInfo.GetState())
}

func (suite *HandlerCacheTestSuite) TestGetJobRuntime_CacheMissWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobRuntimeOps.EXPECT().Get(gomock.Any(), suite.jobID).
		Return(nil, fmt.Errorf("db error"))
	runtime, err := GetJobRuntimeWithoutFillingCache(
		context.Background(),
		suite.jobID,
		suite.jobFactory,
		suite.jobRuntimeOps)
	suite.Error(err)
	suite.Nil(runtime)
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheHit() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).
		Return(suite.config, nil)
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobConfigOps)
	suite.NoError(err)
	suite.Equal(suite.config.GetInstanceCount(), config.GetInstanceCount())
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheHitWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(suite.cachedJob)
	suite.cachedJob.EXPECT().GetConfig(gomock.Any()).
		Return(nil, fmt.Errorf("cache err"))
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobConfigOps)
	suite.Error(err)
	suite.Nil(config)
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheMiss() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobConfigOps.EXPECT().GetCurrentVersion(gomock.Any(), suite.jobID).
		Return(suite.config, &models.ConfigAddOn{}, nil)
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobConfigOps)
	suite.NoError(err)
	suite.Equal(suite.config.GetInstanceCount(), config.GetInstanceCount())
}

func (suite *HandlerCacheTestSuite) TestGetJobConfig_CacheMissWithErr() {
	suite.jobFactory.EXPECT().GetJob(suite.jobID).Return(nil)
	suite.jobConfigOps.EXPECT().GetCurrentVersion(gomock.Any(), suite.jobID).
		Return(nil, nil, fmt.Errorf("db error"))
	config, err := GetJobConfigWithoutFillingCache(
		context.Background(), suite.jobID, suite.jobFactory, suite.jobConfigOps)
	suite.Error(err)
	suite.Nil(config)
}
