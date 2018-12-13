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

func (suite *statelessActionsTestSuite) TestStatelessWorkflowPauseAction() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		PauseJobWorkflow(suite.ctx, &svc.PauseJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(&svc.PauseJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowPauseAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowPauseActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		PauseJobWorkflow(suite.ctx, &svc.PauseJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowPauseAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowResumeAction() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &svc.ResumeJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(&svc.ResumeJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowResumeAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowResumeActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &svc.ResumeJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowResumeAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowAbortAction() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		AbortJobWorkflow(suite.ctx, &svc.AbortJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(&svc.AbortJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowAbortAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowAbortActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	suite.statelessClient.EXPECT().
		AbortJobWorkflow(suite.ctx, &svc.AbortJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowAbortAction(testJobID, entityVersion.GetValue()))
}

func (suite *statelessActionsTestSuite) TestStatelessQueryActionSuccess() {
	suite.statelessClient.EXPECT().
		QueryJobs(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, req *svc.QueryJobsRequest) {
			spec := req.GetSpec()
			suite.Equal(spec.GetPagination().GetLimit(), uint32(10))
			suite.Equal(spec.GetPagination().GetMaxLimit(), uint32(0))
			suite.Equal(spec.GetPagination().GetOffset(), uint32(0))
			suite.Equal(spec.GetRespool().GetValue(), "/testPath")
			suite.Equal(spec.GetName(), "test1")
			suite.Equal(spec.GetOwner(), "owner1")
			suite.Equal(spec.GetLabels()[0].GetKey(), "k1")
			suite.Equal(spec.GetLabels()[0].GetValue(), "v1")
			suite.Equal(spec.GetLabels()[1].GetKey(), "k2")
			suite.Equal(spec.GetLabels()[1].GetValue(), "v2")
			suite.Equal(spec.GetKeywords()[0], "key1")
			suite.Equal(spec.GetKeywords()[1], "key2")
		}).
		Return(&svc.QueryJobsResponse{}, nil)

	err := suite.client.StatelessQueryAction(
		"k1=v1,k2=v2",
		"/testPath",
		"key1,key2",
		"JOB_STATE_RUNNING,JOB_STATE_SUCCEEDED",
		"owner1",
		"test1",
		1,
		10,
		0,
		0,
		"creation_time",
		"ASC",
	)
	suite.NoError(err)
}

func (suite *statelessActionsTestSuite) TestStatelessQueryActionWrongLabelFormatFailure() {
	err := suite.client.StatelessQueryAction(
		"k1,k2",
		"/testPath",
		"key1,key2",
		"JOB_STATE_RUNNING,JOB_STATE_SUCCEEDED",
		"owner1",
		"test1",
		1,
		10,
		0,
		0,
		"creation_time",
		"ASC",
	)
	suite.Error(err)
}

func (suite *statelessActionsTestSuite) TestStatelessQueryActionWrongSortOrderFailure() {
	err := suite.client.StatelessQueryAction(
		"k1=v1,k2=v2",
		"/testPath",
		"key1,key2",
		"JOB_STATE_RUNNING,JOB_STATE_SUCCEEDED",
		"owner1",
		"test1",
		1,
		10,
		0,
		0,
		"creation_time",
		"Descent",
	)
	suite.Error(err)
}

func (suite *statelessActionsTestSuite) TestStatelessQueryActionError() {
	suite.statelessClient.EXPECT().
		QueryJobs(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, req *svc.QueryJobsRequest) {
			spec := req.GetSpec()
			suite.Equal(spec.GetPagination().GetLimit(), uint32(10))
			suite.Equal(spec.GetPagination().GetMaxLimit(), uint32(0))
			suite.Equal(spec.GetPagination().GetOffset(), uint32(0))
			suite.Equal(spec.GetRespool().GetValue(), "/testPath")
			suite.Equal(spec.GetName(), "test1")
			suite.Equal(spec.GetOwner(), "owner1")
			suite.Equal(spec.GetLabels()[0].GetKey(), "k1")
			suite.Equal(spec.GetLabels()[0].GetValue(), "v1")
			suite.Equal(spec.GetLabels()[1].GetKey(), "k2")
			suite.Equal(spec.GetLabels()[1].GetValue(), "v2")
			suite.Equal(spec.GetKeywords()[0], "key1")
			suite.Equal(spec.GetKeywords()[1], "key2")
		}).
		Return(&svc.QueryJobsResponse{}, yarpcerrors.InternalErrorf("test error"))

	err := suite.client.StatelessQueryAction(
		"k1=v1,k2=v2",
		"/testPath",
		"key1,key2",
		"JOB_STATE_RUNNING,JOB_STATE_SUCCEEDED",
		"owner1",
		"test1",
		1,
		10,
		0,
		0,
		"creation_time",
		"ASC",
	)
	suite.Error(err)
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
