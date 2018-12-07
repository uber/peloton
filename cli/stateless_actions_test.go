package cli

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testStatelessSpecConfig = "../example/stateless/testspec.yaml"
)

type statelessActionsTestSuite struct {
	suite.Suite
	ctx    context.Context
	client Client

	ctrl            *gomock.Controller
	statelessClient *mocks.MockJobServiceYARPCClient
	resClient       *respoolmocks.MockResourceManagerYARPCClient
}

func (suite *statelessActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.statelessClient = mocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.resClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:           false,
		statelessClient: suite.statelessClient,
		resClient:       suite.resClient,
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

func (suite *statelessActionsTestSuite) TestStatelessReplaceJobActionSuccess() {
	batchSize := uint32(1)
	respoolPath := "/testPath"
	entityVersion := "1-1"
	override := false
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	rollbackOnFailure := false
	startPaused := true

	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: respoolPath,
			},
		}).
		Return(&respool.LookupResponse{
			Id: &peloton.ResourcePoolID{Value: uuid.New()},
		}, nil)

	suite.statelessClient.EXPECT().
		ReplaceJob(gomock.Any(), gomock.Any()).
		Return(&svc.ReplaceJobResponse{
			Version: &v1alphapeloton.EntityVersion{Value: "2-2"},
		}, nil)

	suite.NoError(suite.client.StatelessReplaceJobAction(
		testJobID,
		testStatelessSpecConfig,
		batchSize,
		respoolPath,
		entityVersion,
		override,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		rollbackOnFailure,
		startPaused,
	))
}

// TestStatelessReplaceJobActionLookupResourcePoolIDFail tests the failure case of replace
// job due to look up resource pool fails
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobActionLookupResourcePoolIDFail() {
	batchSize := uint32(1)
	respoolPath := "/testPath"
	entityVersion := "1-1"
	override := false
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	rollbackOnFailure := false
	startPaused := true

	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: respoolPath,
			},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessReplaceJobAction(
		testJobID,
		testStatelessSpecConfig,
		batchSize,
		respoolPath,
		entityVersion,
		override,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		rollbackOnFailure,
		startPaused,
	))
}

func TestStatelessActions(t *testing.T) {
	suite.Run(t, new(statelessActionsTestSuite))
}
