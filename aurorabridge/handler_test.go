package aurorabridge

import (
	"context"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	jobmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/thrift/aurora/api"
	"code.uber.internal/infra/peloton/aurorabridge/atop"
	"code.uber.internal/infra/peloton/aurorabridge/fixture"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

type ServiceHandlerTestSuite struct {
	suite.Suite

	ctx context.Context

	ctrl      *gomock.Controller
	jobClient *jobmocks.MockJobServiceYARPCClient

	respoolID *peloton.ResourcePoolID

	handler *ServiceHandler
}

func (suite *ServiceHandlerTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.ctrl = gomock.NewController(suite.T())
	suite.jobClient = jobmocks.NewMockJobServiceYARPCClient(suite.ctrl)

	suite.respoolID = fixture.PelotonResourcePoolID()

	suite.handler = NewServiceHandler(
		tally.NoopScope,
		suite.jobClient,
		suite.respoolID,
	)
}

func (suite *ServiceHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestServiceHandler(t *testing.T) {
	suite.Run(t, &ServiceHandlerTestSuite{})
}

// Ensures StartJobUpdate creates jobs which don't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobSuccess() {
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)
	newv := fixture.PelotonEntityVersion()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.jobClient.EXPECT().
		CreateJob(suite.ctx, gomock.Any()).
		Return(&statelesssvc.CreateJobResponse{
			Version: newv,
		}, nil)

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
	suite.Equal(newv.String(), result.GetKey().GetID())
}

// Ensures StartJobUpdate returns an INVALID_REQUEST error if there is a conflict
// when trying to create a job which doesn't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobConflict() {
	req := fixture.AuroraJobUpdateRequest()
	name := atop.NewJobName(req.GetTaskConfig().GetJob())

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.jobClient.EXPECT().
		CreateJob(suite.ctx, gomock.Any()).
		Return(nil, yarpcerrors.AlreadyExistsErrorf(""))

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures StartJobUpdate replaces jobs which already exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_ReplaceJobSuccess() {
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)
	curv := fixture.PelotonEntityVersion()
	newv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(&statelesssvc.GetJobIDFromJobNameResponse{
			JobId: []*peloton.JobID{id},
		}, nil)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Status: &stateless.JobStatus{
					Version: curv,
				},
			},
		}, nil)

	suite.jobClient.EXPECT().
		ReplaceJob(suite.ctx, gomock.Any()).
		Return(&statelesssvc.ReplaceJobResponse{
			Version: newv,
		}, nil)

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
	suite.Equal(newv.String(), result.GetKey().GetID())
}

// Ensures StartJobUpdate returns an INVALID_REQUEST error if there is a conflict
// when trying to replace a job which has changed version.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_ReplaceJobConflict() {
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)
	curv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(&statelesssvc.GetJobIDFromJobNameResponse{
			JobId: []*peloton.JobID{id},
		}, nil)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Status: &stateless.JobStatus{
					Version: curv,
				},
			},
		}, nil)

	suite.jobClient.EXPECT().
		ReplaceJob(suite.ctx, gomock.Any()).
		Return(nil, yarpcerrors.AbortedErrorf(""))

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}
