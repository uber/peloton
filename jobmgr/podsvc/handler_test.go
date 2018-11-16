package podsvc

import (
	"context"
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesosmaster "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmocks "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	logmanagermocks "code.uber.internal/infra/peloton/jobmgr/logmanager/mocks"
	leadermocks "code.uber.internal/infra/peloton/leader/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/util"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobID      = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testInstanceID = 1
	testPodName    = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1"
	testPodID      = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-3"
	testRunID      = 3
)

type podHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl               *gomock.Controller
	cachedJob          *cachedmocks.MockJob
	cachedTask         *cachedmocks.MockTask
	jobFactory         *cachedmocks.MockJobFactory
	candidate          *leadermocks.MockCandidate
	podStore           *storemocks.MockTaskStore
	goalStateDriver    *goalstatemocks.MockDriver
	frameworkInfoStore *storemocks.MockFrameworkInfoStore
	hostmgrClient      *hostmocks.MockInternalHostServiceYARPCClient
	logmanager         *logmanagermocks.MockLogManager
	mesosAgentWorkDir  string
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
	suite.handler = &serviceHandler{
		jobFactory:         suite.jobFactory,
		candidate:          suite.candidate,
		podStore:           suite.podStore,
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
		GetRunTime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_KILLED,
			Healthy:   pbtask.HealthState_HEALTHY,
		}, nil)

	resp, err := suite.handler.GetPodCache(context.Background(),
		&svc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: testPodName},
		})
	suite.NoError(err)
	suite.NotNil(resp.GetStatus())
	suite.Equal(resp.GetStatus().GetState(), pod.PodState_POD_STATE_RUNNING)
	suite.Equal(resp.GetStatus().GetDesiredState(), pod.PodState_POD_STATE_KILLED)
	suite.Equal(resp.GetStatus().GetHealthy(), pod.HealthState_HEALTH_STATE_HEALTHY)
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
		GetRunTime(gomock.Any()).
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
	taskRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	pelotonJobID := &peloton.JobID{Value: testJobID}

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJobID, uint32(testInstanceID)).
			Return(taskRuntime, nil),

		suite.jobFactory.EXPECT().
			AddJob(pelotonJobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			ReplaceTasks(map[uint32]*pbtask.RuntimeInfo{
				testInstanceID: taskRuntime,
			}, true).
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
// case of refreshing pod, due to error while getting task runtime
func (suite *podHandlerTestSuite) TestRefreshPodFailToGetTaskRuntime() {
	pelotonJobID := &peloton.JobID{Value: testJobID}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.podStore.EXPECT().
		GetTaskRuntime(gomock.Any(), pelotonJobID, uint32(testInstanceID)).
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
	taskRuntime := &pbtask.RuntimeInfo{
		State: pbtask.TaskState_RUNNING,
	}
	pelotonJobID := &peloton.JobID{Value: testJobID}

	gomock.InOrder(
		suite.candidate.EXPECT().
			IsLeader().
			Return(true),

		suite.podStore.EXPECT().
			GetTaskRuntime(gomock.Any(), pelotonJobID, uint32(testInstanceID)).
			Return(taskRuntime, nil),

		suite.jobFactory.EXPECT().
			AddJob(pelotonJobID).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			ReplaceTasks(map[uint32]*pbtask.RuntimeInfo{
				testInstanceID: taskRuntime,
			}, true).
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
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.podStore.EXPECT().
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

		suite.cachedTask.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any(), pbjob.JobType_SERVICE).
			Return(nil, nil),

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
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.podStore.EXPECT().
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

		suite.cachedTask.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any(), pbjob.JobType_SERVICE).
			Return(nil, nil),

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
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.podStore.EXPECT().
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

		suite.cachedTask.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any(), pbjob.JobType_SERVICE).
			Return(nil, jobmgrcommon.UnexpectedVersionError),

		suite.cachedTask.EXPECT().
			GetRunTime(gomock.Any()).
			Return(&pbtask.RuntimeInfo{
				State:         pbtask.TaskState_KILLED,
				GoalState:     pbtask.TaskState_KILLED,
				ConfigVersion: 1,
			}, nil),

		suite.podStore.EXPECT().
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

		suite.cachedTask.EXPECT().
			CompareAndSetRuntime(gomock.Any(), gomock.Any(), pbjob.JobType_SERVICE).
			Return(nil, nil),

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

// TestStartPodFailToSetJobRuntime tests the case of pod failure
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

func TestPodServiceHandler(t *testing.T) {
	suite.Run(t, new(podHandlerTestSuite))
}

func (suite *podHandlerTestSuite) TestStopPod() {
	request := &svc.StopPodRequest{}
	response, err := suite.handler.StopPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
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
			PatchTasks(gomock.Any(), runtimeDiff).
			Return(nil),

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
			PatchTasks(gomock.Any(), runtimeDiff).
			Return(yarpcerrors.InternalErrorf("test error")),

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

func (suite *podHandlerTestSuite) TestGetPod() {
	request := &svc.GetPodRequest{}
	response, err := suite.handler.GetPod(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}

// TestServiceHandler_GetPodEvents tests getting pod events for a given pod
func (suite *podHandlerTestSuite) TestGetPodEvents() {
	request := &svc.GetPodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: testPodID,
			},
			ActualState:  "STARTING",
			DesiredState: "RUNNING",
			PrevPodId: &v1alphapeloton.PodID{
				Value: "0",
			},
		},
	}
	response := &svc.GetPodEventsResponse{
		Events: events,
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
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: testPodID,
			},
			ActualState: pbtask.TaskState_RUNNING.String(),
			Hostname:    hostname,
			AgentId:     agentID,
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

// TestBrowsePodSandboxFailureInvalidPodName test BrowsePodSandbox failure
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

// TestBrowsePodSandboxGetPodEventsFailure test BrowsePodSandbox failure
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

// TestBrowsePodSandboxGetFrameworkIDFailure test BrowsePodSandbox failure
// due to error while getting framework id
func (suite *podHandlerTestSuite) TestBrowsePodSandboxGetFrameworkIDFailure() {
	request := &svc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: testPodName,
		},
	}

	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: testPodID,
			},
			ActualState: pbtask.TaskState_RUNNING.String(),
			Hostname:    "hostname",
			AgentId:     "agentID",
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
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: testPodID,
			},
			ActualState: pbtask.TaskState_RUNNING.String(),
			Hostname:    hostname,
			AgentId:     agentID,
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
	events := []*pod.PodEvent{
		{
			PodId: &v1alphapeloton.PodID{
				Value: testPodID,
			},
			ActualState: pbtask.TaskState_RUNNING.String(),
			Hostname:    hostname,
			AgentId:     agentID,
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

func (suite *podHandlerTestSuite) TestDeletePodEvents() {
	request := &svc.DeletePodEventsRequest{}
	response, err := suite.handler.DeletePodEvents(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
}
