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

package podsvc

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/models"

	leadermocks "github.com/uber/peloton/pkg/common/leader/mocks"
	"github.com/uber/peloton/pkg/common/util"
	cachedmocks "github.com/uber/peloton/pkg/jobmgr/cached/mocks"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
	goalstatemocks "github.com/uber/peloton/pkg/jobmgr/goalstate/mocks"
	logmanagermocks "github.com/uber/peloton/pkg/jobmgr/logmanager/mocks"
	storemocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobID      = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testInstanceID = 1
	testPodName    = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1"
	testPodID      = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"
	testPrevPodID  = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
	testRunID      = 3
)

type podHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl                *gomock.Controller
	cachedJob           *cachedmocks.MockJob
	cachedTask          *cachedmocks.MockTask
	jobFactory          *cachedmocks.MockJobFactory
	candidate           *leadermocks.MockCandidate
	podStore            *storemocks.MockTaskStore
	mockedPodEventsOps  *objectmocks.MockPodEventsOps
	mockTaskConfigV2Ops *objectmocks.MockTaskConfigV2Ops
	goalStateDriver     *goalstatemocks.MockDriver
	frameworkInfoStore  *storemocks.MockFrameworkInfoStore
	hostmgrClient       *hostmocks.MockInternalHostServiceYARPCClient
	logmanager          *logmanagermocks.MockLogManager
	mesosAgentWorkDir   string
}

func (suite *podHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.podStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.candidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.frameworkInfoStore = storemocks.NewMockFrameworkInfoStore(suite.ctrl)
	suite.hostmgrClient = hostmocks.NewMockInternalHostServiceYARPCClient(suite.ctrl)
	suite.logmanager = logmanagermocks.NewMockLogManager(suite.ctrl)
	suite.mesosAgentWorkDir = "test"
	suite.mockedPodEventsOps = objectmocks.NewMockPodEventsOps(suite.ctrl)
	suite.mockTaskConfigV2Ops = objectmocks.NewMockTaskConfigV2Ops(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:         suite.jobFactory,
		candidate:          suite.candidate,
		podStore:           suite.podStore,
		podEventsOps:       suite.mockedPodEventsOps,
		taskConfigV2Ops:    suite.mockTaskConfigV2Ops,
		goalStateDriver:    suite.goalStateDriver,
		frameworkInfoStore: suite.frameworkInfoStore,
		hostMgrClient:      suite.hostmgrClient,
		logManager:         suite.logmanager,
		mesosAgentWorkDir:  suite.mesosAgentWorkDir,
	}
}

func (suite *podHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// GetPodCacheSuccess test the success case of get pod cache
func (suite *podHandlerTestSuite) TestGetPodCacheSuccess() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(testInstanceID)).
		Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_KILLED,
			Healthy:   pbtask.HealthState_HEALTHY,
		}, nil)

	suite.cachedTask.EXPECT().
		GetLabels(gomock.Any()).
		Return(nil, nil)

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.NoError(err)
	suite.NotNil(resp.GetStatus())
	suite.Equal(resp.GetStatus().GetState(), pod.PodState_POD_STATE_RUNNING)
	suite.Equal(resp.GetStatus().GetDesiredState(), pod.PodState_POD_STATE_KILLED)
	suite.Equal(resp.GetStatus().GetContainersStatus()[0].GetHealthy().GetState(), pod.HealthState_HEALTH_STATE_HEALTHY)
}

// TestGetPodCacheInvalidPodName test the case of getting cache
// with invalid pod name
func (suite *podHandlerTestSuite) TestGetPodCacheInvalidPodName() {
	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: "invalid-name"},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestGetPodCacheNoJobCache tests the case of getting cache
// when the corresponding job cache does not exist
func (suite *podHandlerTestSuite) TestGetPodCacheNoJobCache() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(testInstanceID)).
		Return(nil)

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestGetPodCacheNoTaskCache tests the case of getting cache
// when cachedTask fail to get runtime
func (suite *podHandlerTestSuite) TestGetPodCacheNoTaskCache() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(testInstanceID)).
		Return(nil)

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestGetPodCacheFailToGetRuntime tests the case of getting cache
// when the corresponding task cache does not exist
func (suite *podHandlerTestSuite) TestGetPodCacheFailToGetRuntime() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(testInstanceID)).
		Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.UnavailableErrorf("test error"))

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsUnavailable(err))
}

// TestGetPodCacheFailToGetLabels tests the case of getting error in
// fetching labels from the cache
func (suite *podHandlerTestSuite) TestGetPodCacheFailToGetLabels() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetTask(uint32(testInstanceID)).
		Return(suite.cachedTask)

	suite.cachedTask.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_KILLED,
			Healthy:   pbtask.HealthState_HEALTHY,
		}, nil)

	suite.cachedTask.EXPECT().
		GetLabels(gomock.Any()).
		Return(nil, yarpcerrors.UnavailableErrorf("test error"))

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsUnavailable(err))
}

// TestRefreshPodSuccess tests the success case of refreshing pod
func (suite *podHandlerTestSuite) TestRefreshPodSuccess() {
	taskInfo := &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		},
	}
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[testInstanceID] = taskInfo
	pelotonJobID := &peloton.JobID{Value: testJobID}

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.podStore.EXPECT().
			GetTaskForJob(gomock.Any(), testJobID, uint32(testInstanceID)).
			Return(taskInfos, nil),

		suite.jobFactory.EXPECT().
			AddJob(pelotonJobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			ReplaceTasks(gomock.Any(), true).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(pelotonJobID, uint32(testInstanceID), gomock.Any()).
			Return(),

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_SERVICE),

		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(pbjob.JobType_SERVICE).
			Return(time.Second),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(pelotonJobID, gomock.Any()).
			Return(),
	)

	resp, err := suite.handler.RefreshPod(context.Background(),
		&svc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestRefreshPodNonLeader tests calling refresh pod
// on non-leader jobmgr
func (suite *podHandlerTestSuite) TestRefreshPodNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.RefreshPod(context.Background(),
		&svc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsUnavailable(err))
}

// TestRefreshPodInvalidPodName tests the failure case of refreshing pod
// due to invalid pod name
func (suite *podHandlerTestSuite) TestRefreshPodInvalidPodName() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	resp, err := suite.handler.RefreshPod(context.Background(),
		&svc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: "invalid-name"},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestRefreshPodFailToGetTaskRuntime tests the failure
// case of refreshing pod, due to error while getting task info
func (suite *podHandlerTestSuite) TestRefreshPodFailToGetTask() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.podStore.EXPECT().
		GetTaskForJob(gomock.Any(), testJobID, uint32(testInstanceID)).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshPod(context.Background(),
		&svc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})

	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
}

// TestRefreshPodFailToReplaceTasks tests the failure case of
// replacing tasks
func (suite *podHandlerTestSuite) TestRefreshPodFailToReplaceTasks() {
	taskInfo := &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		},
	}
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[testInstanceID] = taskInfo
	pelotonJobID := &peloton.JobID{Value: testJobID}

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.podStore.EXPECT().
			GetTaskForJob(gomock.Any(), testJobID, uint32(testInstanceID)).
			Return(taskInfos, nil),

		suite.jobFactory.EXPECT().
			AddJob(pelotonJobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			ReplaceTasks(gomock.Any(), true).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	resp, err := suite.handler.RefreshPod(context.Background(),
		&svc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInternal(err))
}

// TestStartPodSuccess tests the case of start pod successfully
func (suite *podHandlerTestSuite) TestStartPodSuccess() {
	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&pbjob.JobConfig{
				Type: pbjob.JobType_SERVICE,
			}, nil),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:     pbjob.JobState_RUNNING,
				GoalState: pbjob.JobState_RUNNING,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), &pbjob.RuntimeInfo{
				State:     pbjob.JobState_PENDING,
				GoalState: pbjob.JobState_RUNNING,
			}).Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_PENDING,
			GoalState: pbjob.JobState_RUNNING,
		}, nil),

		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(testInstanceID)).
			Return(suite.cachedTask, nil),

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				&peloton.JobID{Value: testJobID},
				uint32(testInstanceID),
				uint64(1)).
			Return(&pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled: true,
				},
			}, nil, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetTask(
				gomock.Any(),
				uint32(testInstanceID),
				gomock.Any(),
				false,
			).Return(nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(&peloton.JobID{Value: testJobID}, uint32(testInstanceID), gomock.Any()).
			Return(),

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_SERVICE),

		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(pbjob.JobType_SERVICE).
			Return(time.Second),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()).
			Return(),
	)

	resp, err := suite.handler.StartPod(context.Background(), &svc.StartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestStartPodNonLeaderFailure tests start pod failure case
// as the candidate is non-leader
func (suite *podHandlerTestSuite) TestStartPodNonLeaderFailure() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.StartPod(context.Background(), &svc.StartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestStartPodSuccessWithRuntimeUnexpectedVersionError tests the case that
// pod start can succeed in the case of job runtime update has
// UnexpectedVersionError
func (suite *podHandlerTestSuite) TestStartPodSuccessWithJobRuntimeUnexpectedVersionError() {
	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&pbjob.JobConfig{
				Type: pbjob.JobType_SERVICE,
			}, nil),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:     pbjob.JobState_RUNNING,
				GoalState: pbjob.JobState_RUNNING,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), &pbjob.RuntimeInfo{
				State:     pbjob.JobState_PENDING,
				GoalState: pbjob.JobState_RUNNING,
			}).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:     pbjob.JobState_RUNNING,
				GoalState: pbjob.JobState_RUNNING,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), &pbjob.RuntimeInfo{
				State:     pbjob.JobState_PENDING,
				GoalState: pbjob.JobState_RUNNING,
			}).Return(nil, nil),

		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(testInstanceID)).
			Return(suite.cachedTask, nil),

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				&peloton.JobID{Value: testJobID},
				uint32(testInstanceID),
				uint64(1)).
			Return(&pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled: true,
				},
			}, nil, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetTask(
				gomock.Any(),
				uint32(testInstanceID),
				gomock.Any(),
				false,
			).Return(nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(&peloton.JobID{Value: testJobID}, uint32(testInstanceID), gomock.Any()).
			Return(),

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_SERVICE),

		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(pbjob.JobType_SERVICE).
			Return(time.Second),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()).
			Return(),
	)

	resp, err := suite.handler.StartPod(context.Background(), &svc.StartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestStartPodSuccessWithRuntimeUnexpectedVersionError tests the case that
// pod start can succeed in the case of pod runtime update has
// UnexpectedVersionError
func (suite *podHandlerTestSuite) TestStartPodSuccessWithPodRuntimeUnexpectedVersionError() {
	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&pbjob.JobConfig{
				Type: pbjob.JobType_SERVICE,
			}, nil),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:     pbjob.JobState_RUNNING,
				GoalState: pbjob.JobState_RUNNING,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), &pbjob.RuntimeInfo{
				State:     pbjob.JobState_PENDING,
				GoalState: pbjob.JobState_RUNNING,
			}).Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_PENDING,
			GoalState: pbjob.JobState_RUNNING,
		}, nil),

		suite.cachedJob.EXPECT().
			AddTask(gomock.Any(), uint32(testInstanceID)).
			Return(suite.cachedTask, nil),

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				&peloton.JobID{Value: testJobID},
				uint32(testInstanceID),
				uint64(1)).
			Return(&pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled: true,
				},
			}, nil, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetTask(
				gomock.Any(),
				uint32(testInstanceID),
				gomock.Any(),
				false,
			).Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedTask.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				&peloton.JobID{Value: testJobID},
				uint32(testInstanceID),
				uint64(1)).
			Return(&pbtask.TaskConfig{
				HealthCheck: &pbtask.HealthCheckConfig{
					Enabled: true,
				},
			}, nil, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetTask(
				gomock.Any(),
				uint32(testInstanceID),
				gomock.Any(),
				false,
			).Return(nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(&peloton.JobID{Value: testJobID}, uint32(testInstanceID), gomock.Any()).
			Return(),

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_SERVICE),

		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(pbjob.JobType_SERVICE).
			Return(time.Second),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()).
			Return(),
	)

	resp, err := suite.handler.StartPod(context.Background(), &svc.StartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestStartPodFailToSetJobRuntime tests the case of pod start failure
// due to fail to set job runtime
func (suite *podHandlerTestSuite) TestStartPodFailToSetJobRuntime() {
	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID}).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			GetConfig(gomock.Any()).
			Return(&pbjob.JobConfig{
				Type: pbjob.JobType_SERVICE,
			}, nil),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				State:     pbjob.JobState_RUNNING,
				GoalState: pbjob.JobState_RUNNING,
			}, nil),

		suite.cachedJob.EXPECT().
			CompareAndSetRuntime(gomock.Any(), &pbjob.RuntimeInfo{
				State:     pbjob.JobState_PENDING,
				GoalState: pbjob.JobState_RUNNING,
			}).Return(nil, yarpcerrors.InternalErrorf("test error")),

		suite.cachedJob.EXPECT().
			GetJobType().
			Return(pbjob.JobType_SERVICE),

		suite.goalStateDriver.EXPECT().
			JobRuntimeDuration(pbjob.JobType_SERVICE).
			Return(time.Second),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any()).
			Return(),
	)

	resp, err := suite.handler.StartPod(context.Background(), &svc.StartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestStopPodSuccess tests the success case of stopping a pod
func (suite *podHandlerTestSuite) TestStopPodSuccess() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := testPodID
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField:   pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:     "Task stop API request",
		jobmgrcommon.ReasonField:      "",
		jobmgrcommon.DesiredHostField: "",

		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.StopPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	response, err := suite.handler.StopPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}

// TestStopPodAlreadyStoppedPod tests calling stop pod
// on an already stopped pod
func (suite *podHandlerTestSuite) TestStopPodAlreadyStoppedPod() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := testPodID
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		GoalState: pbtask.TaskState_KILLED,
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),
	)

	request := &svc.StopPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	response, err := suite.handler.StopPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}

// TestStopPodNonLeader tests calling stop pod
// on non-leader jobmgr
func (suite *podHandlerTestSuite) TestStopPodNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.StopPod(context.Background(),
		&svc.StopPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsUnavailable(err))
}

// TestStopPodInvalidPodName tests the case of
// stopping pod with invalid pod name
func (suite *podHandlerTestSuite) TestStopPodInvalidPodName() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	resp, err := suite.handler.StopPod(context.Background(),
		&svc.StopPodRequest{
			PodName: &v1alphapeloton.PodName{Value: "invalid-name"},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestStopPodGetTaskRuntimeFailure tests failure of
// stopping pod due to GetTaskRuntime failure
func (suite *podHandlerTestSuite) TestStopPodGetTaskRuntimeFailure() {
	jobID := &peloton.JobID{Value: testJobID}
	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(nil, yarpcerrors.NotFoundErrorf("test error")),
	)

	resp, err := suite.handler.StopPod(context.Background(),
		&svc.StopPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestStopPodPatchTasksFailure tests failure of
// stopping pod due to PatchTasks failure
func (suite *podHandlerTestSuite) TestStopPodPatchTasksFailure() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := ""
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField:   pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:     "Task stop API request",
		jobmgrcommon.ReasonField:      "",
		jobmgrcommon.DesiredHostField: "",
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, nil, yarpcerrors.InternalErrorf("test error")),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.StopPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	}
	response, err := suite.handler.StopPod(context.Background(), request)
	suite.Error(err)
	suite.NotNil(response)
}

// TestStopPodPodNotInCache tests the failure case of stopping a
// pod when the pod is not present in the cache
func (suite *podHandlerTestSuite) TestStopPodPodNotInCache() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := testPodID
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.GoalStateField:   pbtask.TaskState_KILLED,
		jobmgrcommon.MessageField:     "Task stop API request",
		jobmgrcommon.ReasonField:      "",
		jobmgrcommon.DesiredHostField: "",

		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, []uint32{uint32(testInstanceID)}, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.StopPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	_, err := suite.handler.StopPod(context.Background(), request)
	suite.Equal(_errPodNotInCache, err)
}

// TestRestartPodSuccess tests the success case of restarting a pod
func (suite *podHandlerTestSuite) TestRestartPodSuccess() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := testPodID
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: util.CreateMesosTaskID(
			jobID, uint32(testInstanceID), uint64(testRunID)+1),
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.RestartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	}
	response, err := suite.handler.RestartPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}

// TestRestartPodViolatingSLA tests the case of restarting pod
// in SLA aware pattern that would violate SLA
func (suite *podHandlerTestSuite) TestRestartPodViolatingSLA() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := testPodID
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: util.CreateMesosTaskID(
			jobID, uint32(testInstanceID), uint64(testRunID)+1),
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, nil, nil),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.RestartPodRequest{
		PodName:  &v1alphapeloton.PodName{Value: testPodName},
		CheckSla: true,
	}
	response, err := suite.handler.RestartPod(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestRestartPodNonLeader tests calling restart pod
// on non-leader jobmgr
func (suite *podHandlerTestSuite) TestRestartPodNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.RestartPod(context.Background(),
		&svc.RestartPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsUnavailable(err))
}

// TestRestartPodInvalidPodName tests the case of restarting pod
// with invalid pod name
func (suite *podHandlerTestSuite) TestRestartPodInvalidPodName() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	resp, err := suite.handler.RestartPod(context.Background(),
		&svc.RestartPodRequest{
			PodName: &v1alphapeloton.PodName{Value: "invalid-name"},
		})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestRestartPodGetTaskRuntimeFailure tests failure of
// restarting pod due to GetTaskRuntime failure
func (suite *podHandlerTestSuite) TestRestartPodGetTaskRuntimeFailure() {
	jobID := &peloton.JobID{Value: testJobID}
	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(nil, yarpcerrors.NotFoundErrorf("test error")),
	)

	resp, err := suite.handler.RestartPod(context.Background(),
		&svc.RestartPodRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRestartPodPatchTasksFailure tests failure of
// restarting pod due to PatchTasks failure
func (suite *podHandlerTestSuite) TestRestartPodPatchTasksFailure() {
	jobID := &peloton.JobID{Value: testJobID}
	mesosTaskID := ""
	taskRuntimeInfo := &pbtask.RuntimeInfo{
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
	}
	runtimeDiff := make(map[uint32]jobmgrcommon.RuntimeDiff)
	runtimeDiff[uint32(testInstanceID)] = jobmgrcommon.RuntimeDiff{
		jobmgrcommon.DesiredMesosTaskIDField: util.CreateMesosTaskID(
			jobID, uint32(testInstanceID), 1),
		jobmgrcommon.TerminationStatusField: &pbtask.TerminationStatus{
			Reason: pbtask.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART,
		},
	}

	suite.cachedJob.EXPECT().
		ID().
		Return(jobID).
		AnyTimes()

	suite.cachedTask.EXPECT().
		ID().
		Return(uint32(testInstanceID)).
		AnyTimes()

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.jobFactory.EXPECT().
			AddJob(&peloton.JobID{Value: testJobID}).
			Return(suite.cachedJob),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), jobID, uint32(testInstanceID)).
			Return(taskRuntimeInfo, nil),

		suite.cachedJob.EXPECT().
			PatchTasks(gomock.Any(), runtimeDiff, false).
			Return(nil, nil, yarpcerrors.InternalErrorf("test error")),

		suite.goalStateDriver.EXPECT().
			EnqueueTask(jobID, uint32(testInstanceID), gomock.Any()),
	)

	request := &svc.RestartPodRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
	}
	response, err := suite.handler.RestartPod(context.Background(), request)
	suite.Error(err)
	suite.NotNil(response)
}

// TestGetPodSuccess tests the success case of getting pod info
func (suite *podHandlerTestSuite) TestGetPodSuccess() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}
	pelotonJob := &peloton.JobID{Value: testJobID}
	var configVersion uint64 = 1
	testLabels := []*peloton.Label{
		{
			Key:   "testKey",
			Value: "testValue",
		},
	}
	testPorts := []*pbtask.PortConfig{
		{
			Name:  "port name",
			Value: 8080,
		},
	}
	testConstraint := &pbtask.Constraint{
		Type: pbtask.Constraint_LABEL_CONSTRAINT,
		LabelConstraint: &pbtask.LabelConstraint{
			Kind: pbtask.LabelConstraint_TASK,
		},
	}
	mesosTaskID := testPodID
	prevMesosTaskID := testPrevPodID

	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			PrevPodId: &v1alphapeloton.PodID{
				Value: prevMesosTaskID,
			},
			Timestamp:    "2019-01-03T22:14:58Z",
			Message:      "",
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     "peloton-host-0",
			Version: &v1alphapeloton.EntityVersion{
				Value: "1-0-0",
			},
		},
	}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
			Return(
				&pbtask.RuntimeInfo{
					State:         pbtask.TaskState_RUNNING,
					GoalState:     pbtask.TaskState_RUNNING,
					ConfigVersion: configVersion,
					PrevMesosTaskId: &mesos.TaskID{
						Value: &prevMesosTaskID,
					},
				}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			).Return(
			&pbtask.TaskConfig{
				Name:       testPodName,
				Labels:     testLabels,
				Ports:      testPorts,
				Constraint: testConstraint,
			}, &models.ConfigAddOn{},
			nil,
		),

		suite.podStore.EXPECT().
			GetPodEvents(
				gomock.Any(),
				testJobID,
				uint32(testInstanceID),
				testPrevPodID,
			).Return(events, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			),

		suite.podStore.EXPECT().
			GetPodEvents(
				gomock.Any(),
				testJobID,
				uint32(testInstanceID),
				testPrevPodID,
			).Return(nil, nil),
	)

	response, err := suite.handler.GetPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(request.GetPodName(), response.GetCurrent().GetSpec().GetPodName())
	suite.NotEmpty(response.GetPrevious())
	for _, info := range response.GetPrevious() {
		suite.Equal(request.GetPodName(), info.GetSpec().GetPodName())
	}
}

// TestGetPodCurrentOnly tests the success case of getting pod info with
// limit set to 1
func (suite *podHandlerTestSuite) TestGetPodCurrentOnly() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		Limit: uint32(1),
	}
	pelotonJob := &peloton.JobID{Value: testJobID}
	var configVersion uint64 = 1
	testLabels := []*peloton.Label{
		{
			Key:   "testKey",
			Value: "testValue",
		},
	}
	testPorts := []*pbtask.PortConfig{
		{
			Name:  "port name",
			Value: 8080,
		},
	}
	testConstraint := &pbtask.Constraint{
		Type: pbtask.Constraint_LABEL_CONSTRAINT,
		LabelConstraint: &pbtask.LabelConstraint{
			Kind: pbtask.LabelConstraint_TASK,
		},
	}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
			Return(
				&pbtask.RuntimeInfo{
					State:         pbtask.TaskState_RUNNING,
					GoalState:     pbtask.TaskState_RUNNING,
					ConfigVersion: configVersion,
				}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			).Return(
			&pbtask.TaskConfig{
				Name:       testPodName,
				Labels:     testLabels,
				Ports:      testPorts,
				Constraint: testConstraint,
			}, &models.ConfigAddOn{},
			nil,
		),
	)

	response, err := suite.handler.GetPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(request.GetPodName(), response.GetCurrent().GetSpec().GetPodName())
	suite.Empty(response.GetPrevious())
}

// TestGetPodSuccessLimit tests the success case of getting pod info with a limit
func (suite *podHandlerTestSuite) TestGetPodSuccessLimit() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		Limit: 2,
	}
	pelotonJob := &peloton.JobID{Value: testJobID}
	var configVersion uint64 = 1
	testLabels := []*peloton.Label{
		{
			Key:   "testKey",
			Value: "testValue",
		},
	}
	testPorts := []*pbtask.PortConfig{
		{
			Name:  "port name",
			Value: 8080,
		},
	}
	testConstraint := &pbtask.Constraint{
		Type: pbtask.Constraint_LABEL_CONSTRAINT,
		LabelConstraint: &pbtask.LabelConstraint{
			Kind: pbtask.LabelConstraint_TASK,
		},
	}
	mesosTaskID := testPodID
	prevMesosTaskID := testPrevPodID
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			PrevPodId: &v1alphapeloton.PodID{
				Value: prevMesosTaskID,
			},
			Timestamp:    "2019-01-03T22:14:58Z",
			Message:      "",
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     "peloton-host-0",
			Version: &v1alphapeloton.EntityVersion{
				Value: "1-0-0",
			},
		},
	}

	prevEvents := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			PrevPodId: &v1alphapeloton.PodID{
				Value: prevMesosTaskID,
			},
			Timestamp:    "2019-01-03T22:14:58Z",
			Message:      "",
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     "peloton-host-0",
			Version: &v1alphapeloton.EntityVersion{
				Value: "1-0-0",
			},
		},
	}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
			Return(
				&pbtask.RuntimeInfo{
					State:         pbtask.TaskState_RUNNING,
					GoalState:     pbtask.TaskState_RUNNING,
					ConfigVersion: configVersion,
					PrevMesosTaskId: &mesos.TaskID{
						Value: &prevMesosTaskID,
					},
				}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			).Return(
			&pbtask.TaskConfig{
				Name:       testPodName,
				Labels:     testLabels,
				Ports:      testPorts,
				Constraint: testConstraint,
			}, &models.ConfigAddOn{},
			nil,
		),

		suite.podStore.EXPECT().
			GetPodEvents(
				gomock.Any(),
				testJobID,
				uint32(testInstanceID),
				testPrevPodID,
			).Return(events, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			),

		suite.podStore.EXPECT().
			GetPodEvents(
				gomock.Any(),
				testJobID,
				uint32(testInstanceID),
				testPrevPodID,
			).Return(prevEvents, nil),
	)

	response, err := suite.handler.GetPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(request.GetPodName(), response.GetCurrent().GetSpec().GetPodName())
	suite.NotEmpty(response.GetPrevious())
	for _, info := range response.GetPrevious() {
		suite.Equal(request.GetPodName(), info.GetSpec().GetPodName())
	}
}

// TestGetPodInvalidPodName tests PodName
// parse error while getting pod info
func (suite *podHandlerTestSuite) TestGetPodInvalidPodName() {
	request := &svc.GetPodRequest{}
	_, err := suite.handler.GetPod(context.Background(), request)
	suite.Error(err)
}

// TestGetPodTaskRuntimeFailure tests GetPod failure due to
// error while getting task runtime.
func (suite *podHandlerTestSuite) TestGetPodTaskRuntimeFailure() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}
	pelotonJob := &peloton.JobID{Value: testJobID}

	suite.podStore.EXPECT().
		GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetPod(context.Background(), request)
	suite.Error(err)
}

// TestGetPodTaskRuntimeFailure tests GetPod failure due to
// error while getting task config.
func (suite *podHandlerTestSuite) TestGetPodTaskConfigFailure() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}
	pelotonJob := &peloton.JobID{Value: testJobID}
	var configVersion uint64 = 1

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
			Return(
				&pbtask.RuntimeInfo{
					State:         pbtask.TaskState_RUNNING,
					GoalState:     pbtask.TaskState_RUNNING,
					ConfigVersion: configVersion,
				}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			).Return(nil, nil, yarpcerrors.InternalErrorf("test error")),
	)

	_, err := suite.handler.GetPod(context.Background(), request)
	suite.Error(err)
}

// TestGetPodFailureToGetPreviousPodEvents tests failure to get pod info due to
// error while getting events for previous runs of the pod
func (suite *podHandlerTestSuite) TestGetPodFailureToGetPreviousPodEvents() {
	request := &svc.GetPodRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}
	pelotonJob := &peloton.JobID{Value: testJobID}
	var configVersion uint64 = 1
	prevMesosTaskID := testPrevPodID

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJob, uint32(testInstanceID)).
			Return(
				&pbtask.RuntimeInfo{
					State:         pbtask.TaskState_RUNNING,
					GoalState:     pbtask.TaskState_RUNNING,
					ConfigVersion: configVersion,
					PrevMesosTaskId: &mesos.TaskID{
						Value: &prevMesosTaskID,
					},
				}, nil),

		suite.mockTaskConfigV2Ops.EXPECT().
			GetTaskConfig(
				gomock.Any(),
				pelotonJob,
				uint32(testInstanceID),
				configVersion,
			).Return(
			&pbtask.TaskConfig{
				Name: testPodName,
			}, &models.ConfigAddOn{},
			nil,
		),

		suite.podStore.EXPECT().
			GetPodEvents(
				gomock.Any(),
				testJobID,
				uint32(testInstanceID),
				testPrevPodID,
			).Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	_, err := suite.handler.GetPod(context.Background(), request)
	suite.Error(err)
}

// TestServiceHandler_GetPodEvents tests getting pod events for a given pod
func (suite *podHandlerTestSuite) TestGetPodEvents() {
	request := &svc.GetPodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	mesosTaskID := testPodID
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			Timestamp:    "2019-01-03T22:14:58Z",
			Message:      "",
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     "peloton-host-0",
		},
	}

	suite.podStore.EXPECT().
		GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
		Return(events, nil)
	response, err := suite.handler.GetPodEvents(context.Background(), request)
	suite.NoError(err)
	suite.Equal(events, response.GetEvents())
}

// TestGetPodEventsPodNameParseError tests PodName parse error
// while getting pod events for a given pod
func (suite *podHandlerTestSuite) TestGetPodEventsPodNameParseError() {
	request := &svc.GetPodEventsRequest{}
	_, err := suite.handler.GetPodEvents(context.Background(), request)
	suite.Error(err)
}

// TestGetPodEventsStoreError tests store error
// while getting pod events for a given pod
func (suite *podHandlerTestSuite) TestGetPodEventsStoreError() {
	request := &svc.GetPodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}
	suite.podStore.EXPECT().
		GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
		Return(nil, fmt.Errorf("fake GetPodEvents error"))
	_, err := suite.handler.GetPodEvents(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxSuccess tests the success case of browsing pod sandbox
func (suite *podHandlerTestSuite) TestBrowsePodSandboxSuccess() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: testPodID,
		},
	}

	hostname := "hostname"
	frameworkID := "testFramework"
	agentPID := "slave(1)@1.2.3.4:9090"
	agentIP := "1.2.3.4"
	agentPort := "9090"
	agentID := "agentID"
	agents := []*mesosmaster.Response_GetAgents_Agent{
		{
			Pid: &agentPID,
		},
	}
	mesosTaskID := testPodID
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     hostname,
			AgentId:      agentID,
		},
	}

	logPaths := []string{}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), testPodID).
			Return(events, nil),

		suite.frameworkInfoStore.EXPECT().
			GetFrameworkID(gomock.Any(), _frameworkName).
			Return(frameworkID, nil),

		suite.hostmgrClient.EXPECT().
			GetMesosAgentInfo(
				gomock.Any(),
				&hostsvc.GetMesosAgentInfoRequest{Hostname: hostname},
			).Return(&hostsvc.GetMesosAgentInfoResponse{Agents: agents}, nil),

		suite.logmanager.EXPECT().
			ListSandboxFilesPaths(
				suite.mesosAgentWorkDir,
				frameworkID,
				agentIP,
				agentPort,
				agentID,
				testPodID,
			).Return(logPaths, nil),

		suite.hostmgrClient.EXPECT().GetMesosMasterHostPort(
			gomock.Any(),
			&hostsvc.MesosMasterHostPortRequest{},
		).Return(&hostsvc.MesosMasterHostPortResponse{}, nil),
	)
	response, err := suite.handler.BrowsePodSandbox(context.Background(), request)

	suite.NoError(err)
	suite.Equal(agentIP, response.GetHostname())
	suite.Equal(agentPort, response.GetPort())
	suite.Equal(logPaths, response.GetPaths())
	suite.Empty(response.GetMesosMasterHostname())
	suite.Empty(response.GetMesosMasterPort())
}

// TestBrowsePodSandboxFailureInvalidPodName tests BrowsePodSandbox failure
// due to invalid podname
func (suite *podHandlerTestSuite) TestBrowsePodSandboxFailureInvalidPodName() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: "InvalidPodName",
		},
	}

	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxGetPodEventsFailure tests BrowsePodSandbox failure
// due to GetPodEvents failure
func (suite *podHandlerTestSuite) TestBrowsePodSandboxGetPodEventsFailure() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	suite.podStore.EXPECT().
		GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
		Return(nil, yarpcerrors.NotFoundErrorf("test error"))
	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxAbort tests BrowsePodSandbox failure with aborted error
func (suite *podHandlerTestSuite) TestBrowsePodSandboxAbort() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	suite.podStore.EXPECT().
		GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
		Return(nil, nil)
	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxGetFrameworkIDFailure tests BrowsePodSandbox failure
// due to error while getting framework id
func (suite *podHandlerTestSuite) TestBrowsePodSandboxGetFrameworkIDFailure() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	mesosTaskID := testPodID
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     "hostname",
			AgentId:      "agentId",
		},
	}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
			Return(events, nil),

		suite.frameworkInfoStore.EXPECT().
			GetFrameworkID(gomock.Any(), _frameworkName).
			Return("", yarpcerrors.NotFoundErrorf("test error")),
	)

	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)

	// Test error due to empty framework id
	gomock.InOrder(
		suite.podStore.EXPECT().
			GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), "").
			Return(events, nil),

		suite.frameworkInfoStore.EXPECT().
			GetFrameworkID(gomock.Any(), _frameworkName).
			Return("", nil),
	)

	_, err = suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxListSandboxFilesPathsFailure tests BrowsePodSandbox
// failure due to LogManager.ListSandboxFilesPaths error
func (suite *podHandlerTestSuite) TestBrowsePodSandboxListSandboxFilesPathsFailure() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: testPodID,
		},
	}

	hostname := "hostname"
	frameworkID := "testFramework"
	agentID := "agentID"
	agents := []*mesosmaster.Response_GetAgents_Agent{}
	mesosTaskID := testPodID

	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     hostname,
			AgentId:      agentID,
		},
	}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), testPodID).
			Return(events, nil),

		suite.frameworkInfoStore.EXPECT().
			GetFrameworkID(gomock.Any(), _frameworkName).
			Return(frameworkID, nil),

		suite.hostmgrClient.EXPECT().
			GetMesosAgentInfo(
				gomock.Any(),
				&hostsvc.GetMesosAgentInfoRequest{Hostname: hostname},
			).Return(&hostsvc.GetMesosAgentInfoResponse{Agents: agents}, nil),

		suite.logmanager.EXPECT().
			ListSandboxFilesPaths(
				suite.mesosAgentWorkDir,
				frameworkID,
				gomock.Any(),
				gomock.Any(),
				agentID,
				testPodID,
			).Return(nil, yarpcerrors.NotFoundErrorf("test error")),
	)

	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestBrowsePodSandboxGetMesosMasterHostPortFailure tests BrowsePodSandbox
// failure due to HostMgrClient.GetMesosMasterHostPort error
func (suite *podHandlerTestSuite) TestBrowsePodSandboxGetMesosMasterHostPortFailure() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: testPodID,
		},
	}

	hostname := "hostname"
	frameworkID := "testFramework"
	agentPID := "slave(1)@1.2.3.4:9090"
	agentIP := "1.2.3.4"
	agentPort := "9090"
	agentID := "agentID"
	agents := []*mesosmaster.Response_GetAgents_Agent{
		{
			Pid: &agentPID,
		},
	}
	mesosTaskID := testPodID

	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: mesosTaskID,
			},
			ActualState:  pod.PodState_POD_STATE_RUNNING.String(),
			DesiredState: pod.PodState_POD_STATE_RUNNING.String(),
			Hostname:     hostname,
			AgentId:      agentID,
		},
	}
	logPaths := []string{}

	gomock.InOrder(
		suite.podStore.EXPECT().
			GetPodEvents(gomock.Any(), testJobID, uint32(testInstanceID), testPodID).
			Return(events, nil),

		suite.frameworkInfoStore.EXPECT().
			GetFrameworkID(gomock.Any(), _frameworkName).
			Return(frameworkID, nil),

		suite.hostmgrClient.EXPECT().
			GetMesosAgentInfo(
				gomock.Any(),
				&hostsvc.GetMesosAgentInfoRequest{Hostname: hostname},
			).Return(&hostsvc.GetMesosAgentInfoResponse{Agents: agents}, nil),

		suite.logmanager.EXPECT().
			ListSandboxFilesPaths(
				suite.mesosAgentWorkDir,
				frameworkID,
				agentIP,
				agentPort,
				agentID,
				testPodID,
			).Return(logPaths, nil),

		suite.hostmgrClient.EXPECT().GetMesosMasterHostPort(
			gomock.Any(),
			&hostsvc.MesosMasterHostPortRequest{},
		).Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	_, err := suite.handler.BrowsePodSandbox(context.Background(), request)
	suite.Error(err)
}

// TestDeletePodEventsSuccess tests the success case of deleting pod events
func (suite *podHandlerTestSuite) TestDeletePodEventsSuccess() {
	request := &svc.DeletePodEventsRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
		PodId:   &v1alphapeloton.PodID{Value: testPodID},
	}

	suite.podStore.EXPECT().
		DeletePodEvents(
			gomock.Any(),
			testJobID,
			uint32(testInstanceID),
			uint64(testRunID),
			uint64(testRunID)+1,
		).Return(nil)

	response, err := suite.handler.DeletePodEvents(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}

// TestDeletePodEventsFailureInvalidPodName tests
// DeletePodEvents failure due to invalid podname
func (suite *podHandlerTestSuite) TestDeletePodEventsFailureInvalidPodName() {
	request := &svc.DeletePodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: "InvalidPodName",
		},
		PodId: &v1alphapeloton.PodID{
			Value: testPodID,
		},
	}

	_, err := suite.handler.DeletePodEvents(context.Background(), request)
	suite.Error(err)
}

// TestDeletePodEventsFailureInvalidPodID tests
// DeletePodEvents failure due to invalid pod-id
func (suite *podHandlerTestSuite) TestDeletePodEventsFailureInvalidPodID() {
	request := &svc.DeletePodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: "invalid-pod-id",
		},
	}

	_, err := suite.handler.DeletePodEvents(context.Background(), request)
	suite.Error(err)
}

// TestDeletePodEventsStoreError tests
// DeletePodEvents failure due to store error
func (suite *podHandlerTestSuite) TestDeletePodEventsStoreError() {
	request := &svc.DeletePodEventsRequest{
		PodName: &v1alphapeloton.PodName{Value: testPodName},
		PodId:   &v1alphapeloton.PodID{Value: testPodID},
	}

	suite.podStore.EXPECT().
		DeletePodEvents(
			gomock.Any(),
			testJobID,
			uint32(testInstanceID),
			uint64(testRunID),
			uint64(testRunID)+1,
		).Return(yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.DeletePodEvents(context.Background(), request)
	suite.Error(err)
}

func TestPodServiceHandler(t *testing.T) {
	suite.Run(t, new(podHandlerTestSuite))
}
