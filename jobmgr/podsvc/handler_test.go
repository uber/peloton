package podsvc

import (
	"context"
	"testing"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobID      = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testInstanceID = 1
	testPodName    = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1"
)

type podHandlerTestSuite struct {
	suite.Suite
	ctrl       *gomock.Controller
	handler    *serviceHandler
	cachedJob  *cachedmocks.MockJob
	cachedTask *cachedmocks.MockTask
	jobFactory *cachedmocks.MockJobFactory
}

func (suite *podHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedTask = cachedmocks.NewMockTask(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory: suite.jobFactory,
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
			PodName: &v1alphapeloton.PodName{Value: "run-name"},
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

func TestPodServiceHandler(t *testing.T) {
	suite.Run(t, new(podHandlerTestSuite))
}
