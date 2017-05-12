package cli

import (
	"context"
	"errors"
	"io/ioutil"
	"testing"

	client_mocks "code.uber.internal/infra/peloton/vendor_mocks/go.uber.org/yarpc/encoding/json/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc"
	"gopkg.in/yaml.v2"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

const testJobConfig = "../example/testjob.yaml"

type jobActionsTestSuite struct {
	suite.Suite
	mockCtrl       *gomock.Controller
	mockBaseClient *client_mocks.MockClient
	ctx            context.Context
}

func (suite *jobActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockBaseClient = client_mocks.NewMockClient(suite.mockCtrl)
	suite.ctx = context.Background()
}

func (suite *jobActionsTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
	suite.ctx.Done()
}

func (suite *jobActionsTestSuite) getConfig() *job.JobConfig {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(testJobConfig)
	suite.NoError(err)
	err = yaml.Unmarshal(buffer, &jobConfig)
	suite.NoError(err)
	return &jobConfig
}

func (suite *jobActionsTestSuite) TestClient_JobCreateAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockBaseClient,
		jobClient:  suite.mockBaseClient,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
	id := uuid.New()
	path := "/a/b/c/d"
	config := suite.getConfig()
	config.RespoolID = &respool.ResourcePoolID{
		Value: id,
	}

	tt := []struct {
		jobID                 string
		jobCreateRequest      *job.CreateRequest
		jobCreateResponse     *job.CreateResponse
		respoolLookupRequest  *respool.LookupRequest
		respoolLookupResponse *respool.LookupResponse
		createError           error
	}{
		{
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				JobId: &peloton.JobID{
					Value: uuid.New(),
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &respool.ResourcePoolID{
					Value: id,
				},
			},
			createError: nil,
		},
		{
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				JobId: &peloton.JobID{
					Value: uuid.New(),
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &respool.ResourcePoolID{
					Value: id,
				},
			},
			createError: errors.New("unable to create job"),
		},
		{
			jobID: id,
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				JobId: &peloton.JobID{
					Value: id,
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &respool.ResourcePoolID{
					Value: id,
				},
			},
			createError: nil,
		},
	}

	for _, t := range tt {
		suite.withMockResourcePoolLookup(t.respoolLookupRequest, t.respoolLookupResponse)
		suite.withMockJobCreateResponse(t.jobCreateRequest, t.jobCreateResponse, t.createError)

		err := c.JobCreateAction(t.jobID, path, testJobConfig)
		if t.createError != nil {
			suite.EqualError(err, t.createError.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *jobActionsTestSuite) TestClient_JobUpdateAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockBaseClient,
		jobClient:  suite.mockBaseClient,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
	id := uuid.New()
	config := suite.getConfig()
	tt := []struct {
		jobID             string
		jobUpdateRequest  *job.UpdateRequest
		jobUpdateResponse *job.UpdateResponse
		updateError       error
	}{
		{
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{
				Message: "50 instances added",
			},
		},
		{
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{},
			updateError:       errors.New("unable to update job"),
		},
	}

	for _, t := range tt {
		suite.withMockJobUpdateResponse(t.jobUpdateRequest, t.jobUpdateResponse, t.updateError)
		err := c.JobUpdateAction(t.jobID, testJobConfig)
		if t.updateError != nil {
			suite.EqualError(err, t.updateError.Error())
		} else {
			suite.NoError(err)
		}
	}
}

func (suite *jobActionsTestSuite) withMockJobUpdateResponse(
	req *job.UpdateRequest,
	resp *job.UpdateResponse,
	err error,
) {
	suite.mockBaseClient.EXPECT().Call(
		suite.ctx,
		gomock.Eq(
			yarpc.NewReqMeta().Procedure("JobManager.Update"),
		),
		gomock.Eq(req),
		gomock.Eq(&job.UpdateResponse{}),
	).Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
		o := resBodyOut.(*job.UpdateResponse)
		*o = *resp
	}).Return(nil, err)
}

func (suite *jobActionsTestSuite) withMockJobCreateResponse(
	req *job.CreateRequest,
	resp *job.CreateResponse,
	err error,
) {
	suite.mockBaseClient.EXPECT().Call(
		suite.ctx,
		gomock.Eq(
			yarpc.NewReqMeta().Procedure("JobManager.Create"),
		),
		gomock.Eq(req),
		gomock.Eq(&job.CreateResponse{}),
	).Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
		o := resBodyOut.(*job.CreateResponse)
		*o = *resp
	}).Return(nil, err)
}

func (suite *jobActionsTestSuite) withMockResourcePoolLookup(
	req *respool.LookupRequest,
	resp *respool.LookupResponse,
) {
	suite.mockBaseClient.EXPECT().Call(
		suite.ctx,
		gomock.Eq(
			yarpc.NewReqMeta().Procedure("ResourceManager.LookupResourcePoolID"),
		),
		gomock.Eq(req),
		gomock.Eq(&respool.LookupResponse{}),
	).Do(func(_ context.Context, _ yarpc.CallReqMeta, _ interface{}, resBodyOut interface{}) {
		o := resBodyOut.(*respool.LookupResponse)
		*o = *resp
	}).Return(nil, nil)
}

func TestJobActions(t *testing.T) {
	suite.Run(t, new(jobActionsTestSuite))
}
