package cli

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const testPodName = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1"

type podActionsTestSuite struct {
	suite.Suite
	ctx    context.Context
	client Client

	ctrl      *gomock.Controller
	podClient *mocks.MockPodServiceYARPCClient
}

func (suite *podActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.podClient = mocks.NewMockPodServiceYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:      false,
		podClient:  suite.podClient,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
}

func (suite *podActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestClientPodGetCacheSuccess test the success case of getting cache
func (suite *podActionsTestSuite) TestClientPodGetCacheSuccess() {
	suite.podClient.EXPECT().
		GetPodCache(gomock.Any(), gomock.Any()).
		Return(&podsvc.GetPodCacheResponse{
			Status: &pod.PodStatus{
				State: pod.PodState_POD_STATE_RUNNING,
			},
		}, nil)

	suite.NoError(suite.client.PodGetCache(testPodName))
}

// TestClientPodGetCacheSuccess test the failure case of getting cache
func (suite *podActionsTestSuite) TestClientPodGetCacheFail() {
	suite.podClient.EXPECT().
		GetPodCache(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodGetCache(testPodName))
}

func TestPodActions(t *testing.T) {
	suite.Run(t, new(podActionsTestSuite))
}
