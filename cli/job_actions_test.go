package cli

import (
	"context"
	"errors"
	"io/ioutil"
	"testing"

	job_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/job/mocks"
	respool_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/respool/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
)

const (
	testJobConfig = "../example/testjob.yaml"
	testJobID     = "481d565e-28da-457d-8434-f6bb7faa0e95"
)

type jobActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockJob     *job_mocks.MockJobManagerYarpcClient
	mockRespool *respool_mocks.MockResourceManagerYarpcClient
	ctx         context.Context
}

func (suite *jobActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJob = job_mocks.NewMockJobManagerYarpcClient(suite.mockCtrl)
	suite.mockRespool = respool_mocks.NewMockResourceManagerYarpcClient(suite.mockCtrl)
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
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
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
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
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
	suite.mockJob.EXPECT().Update(suite.ctx, gomock.Eq(req)).Return(resp, err)
}

func (suite *jobActionsTestSuite) withMockJobCreateResponse(
	req *job.CreateRequest,
	resp *job.CreateResponse,
	err error,
) {
	suite.mockJob.EXPECT().
		Create(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

func (suite *jobActionsTestSuite) withMockResourcePoolLookup(
	req *respool.LookupRequest,
	resp *respool.LookupResponse,
) {
	suite.mockRespool.EXPECT().
		LookupResourcePoolID(suite.ctx, gomock.Eq(req)).
		Return(resp, nil)
}

func TestJobActions(t *testing.T) {
	suite.Run(t, new(jobActionsTestSuite))
}

func (suite *jobActionsTestSuite) TestClient_JobQueryAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	suite.mockJob.EXPECT().Query(gomock.Any(), &job.QueryRequest{
		Spec: &job.QuerySpec{
			Keywords: []string{"keyword"},
			Labels: []*peloton.Label{{
				Key:   "key",
				Value: "value",
			}},
		},
	}).Return(nil, nil)

	suite.NoError(c.JobQueryAction("key:value", "", "keyword,"))
}

func (suite *jobActionsTestSuite) TestClient_JobGetAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(nil, nil)

	suite.NoError(c.JobGetAction(testJobID))
}

func (suite *jobActionsTestSuite) TestClient_JobStatusAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(nil, nil)

	suite.NoError(c.JobStatusAction(testJobID))
}
