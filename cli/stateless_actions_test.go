package cli

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type statelessActionsTestSuite struct {
	suite.Suite
	ctx    context.Context
	client Client

	ctrl            *gomock.Controller
	statelessClient *mocks.MockJobServiceYARPCClient
}

func (suite *statelessActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.statelessClient = mocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:           false,
		statelessClient: suite.statelessClient,
		dispatcher:      nil,
		ctx:             suite.ctx,
	}
}

func (suite *statelessActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *statelessActionsTestSuite) TestStatelessGetCacheActionSuccess() {
	suite.statelessClient.EXPECT().
		GetJobCache(suite.ctx, &svc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(&svc.GetJobCacheResponse{}, nil)

	suite.NoError(suite.client.StatelessGetCacheAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessGetCacheActionError() {
	suite.statelessClient.EXPECT().
		GetJobCache(suite.ctx, &svc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessGetCacheAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessRefreshAction() {
	suite.statelessClient.EXPECT().
		RefreshJob(suite.ctx, &svc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(&svc.RefreshJobResponse{}, nil)

	suite.NoError(suite.client.StatelessRefreshAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessRefreshActionError() {
	suite.statelessClient.EXPECT().
		RefreshJob(suite.ctx, &svc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRefreshAction(testJobID))
}

func TestStatelessActions(t *testing.T) {
	suite.Run(t, new(statelessActionsTestSuite))
}
