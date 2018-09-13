package cli

import (
	"context"
	"errors"
	"io/ioutil"
	"testing"
	"time"

	jobmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job/mocks"
	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool/mocks"
	taskmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task/mocks"

	pberrors "code.uber.internal/infra/peloton/.gen/peloton/api/v0/errors"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	jobmgrtask "code.uber.internal/infra/peloton/jobmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
	"gopkg.in/yaml.v2"
)

const (
	testJobConfig  = "../example/testjob.yaml"
	testJobID      = "481d565e-28da-457d-8434-f6bb7faa0e95"
	testSecretPath = "/tmp/secret"
	testSecretStr  = "my-test-secret"
)

type jobActionsTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	mockJob     *jobmocks.MockJobManagerYARPCClient
	mockTask    *taskmocks.MockTaskManagerYARPCClient
	mockRespool *respoolmocks.MockResourceManagerYARPCClient
	ctx         context.Context
}

func (suite *jobActionsTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJob = jobmocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
	suite.mockTask = taskmocks.NewMockTaskManagerYARPCClient(suite.mockCtrl)
	suite.mockRespool = respoolmocks.NewMockResourceManagerYARPCClient(
		suite.mockCtrl)
	suite.ctx = context.Background()
}

func TestJobActions(t *testing.T) {
	suite.Run(t, new(jobActionsTestSuite))
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

// TestClientJobCreateAction tests creating a job
func (suite *jobActionsTestSuite) TestClientJobCreateAction() {
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
	config.RespoolID = &peloton.ResourcePoolID{
		Value: id,
	}

	tt := []struct {
		debug                 bool
		jobID                 string
		jobCreateRequest      *job.CreateRequest
		jobCreateResponse     *job.CreateResponse
		respoolLookupRequest  *respool.LookupRequest
		respoolLookupResponse *respool.LookupResponse
		createError           error
		respoolError          error
		secretPath            string
		secret                []byte
	}{
		{
			// happy path
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		// json print
		{
			debug: true,
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// missing job id
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				JobId: nil,
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// job exists error
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					AlreadyExists: &job.JobAlreadyExists{
						Id: &peloton.JobID{
							Value: testJobID,
						},
						Message: "already exists",
					},
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// invalid config error
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					InvalidConfig: &job.InvalidJobConfig{
						Id: &peloton.JobID{
							Value: testJobID,
						},
						Message: "bad configuration",
					},
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// invalid job-id error
			jobID: "",
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: "",
				},
				Config: config,
			},
			jobCreateResponse: &job.CreateResponse{
				Error: &job.CreateResponse_Error{
					InvalidJobId: &job.InvalidJobId{
						Id: &peloton.JobID{
							Value: testJobID,
						},
						Message: "bad job-id",
					},
				},
			},
			respoolLookupRequest: &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: path,
				},
			},
			respoolLookupResponse: &respool.LookupResponse{
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// resource pool look up error
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: errors.New("unable to lookup resource pool"),
		},
		{
			// no resource pool id returned
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
				Id: nil,
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// job create error
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  errors.New("unable to create job"),
			respoolError: nil,
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
		},
		{
			// happy path with secrets
			jobID: id,
			jobCreateRequest: &job.CreateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
				Secrets: []*peloton.Secret{
					jobmgrtask.CreateSecretProto(
						"", testSecretPath, []byte(testSecretStr)),
				},
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
				Id: &peloton.ResourcePoolID{
					Value: id,
				},
			},
			createError:  nil,
			respoolError: nil,
			// set secretPath and secretStr explicitly here,
			// for the rest of the tests, this will default to ""
			secretPath: testSecretPath,
			secret:     []byte(testSecretStr),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.withMockResourcePoolLookup(
			t.respoolLookupRequest,
			t.respoolLookupResponse,
			t.respoolError,
		)

		if t.respoolError == nil && t.respoolLookupResponse.Id != nil {
			suite.withMockJobCreateResponse(
				t.jobCreateRequest,
				t.jobCreateResponse,
				t.createError,
			)
		}

		err := c.JobCreateAction(t.jobID, path, testJobConfig, t.secretPath, t.secret)
		if t.createError != nil {
			suite.EqualError(err, t.createError.Error())
		} else if t.respoolError != nil {
			suite.EqualError(err, t.respoolError.Error())
		} else if t.respoolLookupResponse.Id == nil {
			suite.Error(err)
		} else {
			suite.NoError(err)
		}
	}
}

// TestClientJobUpdateAction tests updating a job
func (suite *jobActionsTestSuite) TestClientJobUpdateAction() {
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
		debug             bool
		jobID             string
		jobUpdateRequest  *job.UpdateRequest
		jobUpdateResponse *job.UpdateResponse
		updateError       error
		secretPath        string
		secret            []byte
	}{
		{
			// happy path
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{
				Id: &peloton.JobID{
					Value: id,
				},
				Message: "50 instances added",
			},
		},
		{
			// json
			debug: true,
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{
				Id: &peloton.JobID{
					Value: id,
				},
				Message: "50 instances added",
			},
		},
		{
			// job not found
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{
				Error: &job.UpdateResponse_Error{
					JobNotFound: &job.JobNotFound{
						Id: &peloton.JobID{
							Value: id,
						},
						Message: "job not found",
					},
				},
			},
		},
		{
			// bad config
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
			},
			jobUpdateResponse: &job.UpdateResponse{
				Error: &job.UpdateResponse_Error{
					InvalidConfig: &job.InvalidJobConfig{
						Id: &peloton.JobID{
							Value: id,
						},
						Message: "invalid configuration",
					},
				},
			},
		},
		{
			// update error
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
		{
			// with secrets
			jobID: id,
			jobUpdateRequest: &job.UpdateRequest{
				Id: &peloton.JobID{
					Value: id,
				},
				Config: config,
				Secrets: []*peloton.Secret{
					jobmgrtask.CreateSecretProto(
						"", testSecretPath, []byte(testSecretStr)),
				},
			},
			jobUpdateResponse: &job.UpdateResponse{},
			updateError:       nil,
			// set secretPath and secretStr explicitly here,
			// for the rest of the tests, this will default to ""
			secretPath: testSecretPath,
			secret:     []byte(testSecretStr),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.withMockJobUpdateResponse(t.jobUpdateRequest, t.jobUpdateResponse, t.updateError)
		err := c.JobUpdateAction(t.jobID, testJobConfig, t.secretPath, t.secret)
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
	err error,
) {
	suite.mockRespool.EXPECT().
		LookupResourcePoolID(suite.ctx, gomock.Eq(req)).
		Return(resp, err)
}

// TestClientJobQueryAction tests job query
func (suite *jobActionsTestSuite) TestClientJobQueryAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &job.QueryResponse{
		Results: []*job.JobSummary{
			{
				Id: &peloton.JobID{
					Value: testJobID,
				},
				Name:          "test",
				OwningTeam:    "test",
				InstanceCount: 10,
				Runtime: &job.RuntimeInfo{
					State:          job.JobState_RUNNING,
					CreationTime:   time.Now().UTC().Format(time.RFC3339Nano),
					CompletionTime: "",
					TaskStats: map[string]uint32{
						"RUNNING": 10,
					},
				},
			},
			{
				Id: &peloton.JobID{
					Value: testJobID,
				},
				Name:          "test",
				OwningTeam:    "test",
				InstanceCount: 10,
				Runtime: &job.RuntimeInfo{
					State:          job.JobState_SUCCEEDED,
					CreationTime:   time.Now().UTC().Format(time.RFC3339Nano),
					CompletionTime: time.Now().UTC().Format(time.RFC3339Nano),
					TaskStats: map[string]uint32{
						"RUNNING": 10,
					},
				},
			},
		},
	}

	suite.mockJob.EXPECT().Query(gomock.Any(), &job.QueryRequest{
		Spec: &job.QuerySpec{
			Keywords: []string{"keyword"},
			Labels: []*peloton.Label{{
				Key:   "key",
				Value: "value",
			}},
			JobStates: []job.JobState{
				job.JobState_RUNNING,
			},
			Owner: "test_owner",
			Name:  "test_name",
			Pagination: &query.PaginationSpec{
				Limit:  10,
				Offset: 0,
				OrderBy: []*query.OrderBy{
					{
						Order:    query.OrderBy_DESC,
						Property: &query.PropertyPath{Value: "creation_time"},
					},
				},
				MaxLimit: 100,
			},
		},
		SummaryOnly: true,
	}).Return(resp, nil)

	suite.NoError(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "DESC",
	))
	suite.Error(c.JobQueryAction(
		"key=value1,value2", "", "keyword,", "RUNNING",
		"test_owner", "test_name", 0, 10, 100, 0, "creation_time", "DESC",
	))
	suite.Error(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "RANDOM",
	))

	c.Debug = true
	suite.mockJob.EXPECT().
		Query(gomock.Any(), gomock.Any()).
		Return(resp, nil)
	suite.NoError(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "DESC",
	))
}

// TestClientJobQueryActionWithRespoolError tests job query
// with error in resource pool lookup
func (suite *jobActionsTestSuite) TestClientJobQueryActionWithRespoolError() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	path := "/respool"
	req := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{
			Value: path,
		},
	}

	suite.mockRespool.EXPECT().
		LookupResourcePoolID(suite.ctx, gomock.Eq(req)).
		Return(nil, errors.New("unable to get resource pool"))

	suite.Error(c.JobQueryAction(
		"key=value", path, "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "DESC",
	))
}

// TestClientJobQueryActionWithError tests with job query
// request returning error
func (suite *jobActionsTestSuite) TestClientJobQueryActionWithError() {
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
			JobStates: []job.JobState{
				job.JobState_RUNNING,
			},
			Owner: "test_owner",
			Name:  "test_name",
			Pagination: &query.PaginationSpec{
				Limit:  10,
				Offset: 0,
				OrderBy: []*query.OrderBy{
					{
						Order:    query.OrderBy_ASC,
						Property: &query.PropertyPath{Value: "creation_time"},
					},
				},
				MaxLimit: 100,
			},
		},
		SummaryOnly: true,
	}).Return(nil, errors.New("unable to query jobs"))

	suite.Error(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "ASC",
	))
}

// TestClientJobQueryActionResponseError tests error in query response
func (suite *jobActionsTestSuite) TestClientJobQueryActionResponseError() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &job.QueryResponse{
		Error: &job.QueryResponse_Error{
			Err: &pberrors.UnknownError{
				Message: "unknown error",
			},
		},
	}

	suite.mockJob.EXPECT().Query(gomock.Any(), &job.QueryRequest{
		Spec: &job.QuerySpec{
			Keywords: []string{"keyword"},
			Labels: []*peloton.Label{{
				Key:   "key",
				Value: "value",
			}},
			JobStates: []job.JobState{
				job.JobState_RUNNING,
			},
			Owner: "test_owner",
			Name:  "test_name",
			Pagination: &query.PaginationSpec{
				Limit:  10,
				Offset: 0,
				OrderBy: []*query.OrderBy{
					{
						Order:    query.OrderBy_DESC,
						Property: &query.PropertyPath{Value: "creation_time"},
					},
				},
				MaxLimit: 100,
			},
		},
		SummaryOnly: true,
	}).Return(resp, nil)

	suite.NoError(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 0, 10, 100, 0, "creation_time", "DESC",
	))
}

// TestClientJobQueryActionWithDays tests job query with days
func (suite *jobActionsTestSuite) TestClientJobQueryActionWithDays() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	suite.mockJob.EXPECT().
		Query(gomock.Any(), gomock.Any()).
		Return(nil, nil)
	suite.NoError(c.JobQueryAction(
		"key=value", "", "keyword,", "RUNNING", "test_owner",
		"test_name", 5, 10, 100, 0, "creation_time", "DESC",
	))
}

// TestClientJobGetAction tests job get
func (suite *jobActionsTestSuite) TestClientJobGetAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	tt := []struct {
		debug    bool
		req      *job.GetRequest
		resp     *job.GetResponse
		getError error
	}{
		{
			// happy path
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp: &job.GetResponse{
				JobInfo: &job.JobInfo{
					Id: &peloton.JobID{
						Value: testJobID,
					},
					Runtime: &job.RuntimeInfo{
						State: job.JobState_RUNNING,
					},
				},
			},
			getError: nil,
		},
		{
			// json
			debug: true,
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp: &job.GetResponse{
				JobInfo: &job.JobInfo{
					Id: &peloton.JobID{
						Value: testJobID,
					},
					Runtime: &job.RuntimeInfo{
						State: job.JobState_RUNNING,
					},
				},
			},
			getError: nil,
		},
		{
			// did not find job
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp:     nil,
			getError: nil,
		},
		{
			// error
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp:     nil,
			getError: errors.New("unable to get job"),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockJob.EXPECT().
			Get(gomock.Any(), t.req).
			Return(t.resp, t.getError)

		if t.getError != nil {
			suite.Error(c.JobGetAction(testJobID))
		} else {
			suite.NoError(c.JobGetAction(testJobID))
		}
	}
}

// TestClientJobStatusAction tests fetching job status
func (suite *jobActionsTestSuite) TestClientJobStatusAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	tt := []struct {
		debug    bool
		req      *job.GetRequest
		resp     *job.GetResponse
		getError error
	}{
		{
			// happy path
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp: &job.GetResponse{
				JobInfo: &job.JobInfo{
					Id: &peloton.JobID{
						Value: testJobID,
					},
					Runtime: &job.RuntimeInfo{
						State: job.JobState_RUNNING,
					},
				},
			},
			getError: nil,
		},
		{
			// json
			debug: true,
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp: &job.GetResponse{
				JobInfo: &job.JobInfo{
					Id: &peloton.JobID{
						Value: testJobID,
					},
					Runtime: &job.RuntimeInfo{
						State: job.JobState_RUNNING,
					},
				},
			},
			getError: nil,
		},
		{
			// did not find job
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp:     nil,
			getError: nil,
		},
		{
			// error
			req: &job.GetRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			resp:     nil,
			getError: errors.New("unable to get job status"),
		},
	}

	for _, t := range tt {
		c.Debug = t.debug
		suite.mockJob.EXPECT().
			Get(gomock.Any(), t.req).
			Return(t.resp, t.getError)
		if t.getError != nil {
			suite.Error(c.JobStatusAction(testJobID))
		} else {
			suite.NoError(c.JobStatusAction(testJobID))
		}
	}
}

// TestClientJobGetCacheAction tests fetching job in cache
func (suite *jobActionsTestSuite) TestClientJobGetCacheAction() {
	c := Client{
		Debug:      false,
		resClient:  suite.mockRespool,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	tt := []struct {
		req *job.GetCacheRequest
		err error
	}{
		{
			req: &job.GetCacheRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			err: nil,
		},
		{
			req: &job.GetCacheRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			err: errors.New("unable to fetch job cache"),
		},
	}

	for _, t := range tt {
		suite.mockJob.EXPECT().
			GetCache(gomock.Any(), t.req).
			Return(nil, t.err)

		if t.err != nil {
			suite.Error(c.JobGetCacheAction(testJobID))
		} else {
			suite.NoError(c.JobGetCacheAction(testJobID))
		}
	}
}

// TestClientJobRefreshAction tests refreshing a job
func (suite *jobActionsTestSuite) TestClientJobRefreshAction() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	resp := &job.RefreshResponse{}

	suite.mockJob.EXPECT().
		Refresh(gomock.Any(), &job.RefreshRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).
		Return(resp, nil)

	suite.NoError(c.JobRefreshAction(testJobID))
}

// TestClientJobDeleteAction tests deleting a job
func (suite *jobActionsTestSuite) TestClientJobDeleteAction() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	tt := []struct {
		req *job.DeleteRequest
		err error
	}{
		{
			req: &job.DeleteRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			err: nil,
		},
		{
			req: &job.DeleteRequest{
				Id: &peloton.JobID{
					Value: testJobID,
				},
			},
			err: errors.New("unable to delete job"),
		},
	}

	for _, t := range tt {
		resp := &job.DeleteResponse{}
		suite.mockJob.EXPECT().
			Delete(gomock.Any(), t.req).
			Return(resp, t.err)

		if t.err != nil {
			suite.Error(c.JobDeleteAction(testJobID))
		} else {
			suite.NoError(c.JobDeleteAction(testJobID))
		}
	}
}

// TestClientJobStopAction tests stopping a job
func (suite *jobActionsTestSuite) TestClientJobStopAction() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
		InvalidInstanceIds: []uint32{3, 4},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	suite.NoError(c.JobStopAction(testJobID, false))
}

// TestClientJobStopActionWithProgress tests stopping a job
// while printing the progress
func (suite *jobActionsTestSuite) TestClientJobStopActionWithProgress() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	getResponse1 := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
				TaskStats: map[string]uint32{
					"RUNNING":   2,
					"SUCCEEDED": 8,
				},
			},
			Config: &job.JobConfig{
				InstanceCount: 10,
			},
		},
	}

	getResponse2 := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_SUCCEEDED,
			},
			Config: &job.JobConfig{
				InstanceCount: 10,
			},
		},
	}

	gomock.InOrder(
		suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).Return(getResponse1, nil),

		suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).Return(getResponse2, nil),
	)

	suite.NoError(c.JobStopAction(testJobID, true))
}

// TestClientJobStopActionProgressTerminate tests stopping a job and
// print progress and job enters terminal state
func (suite *jobActionsTestSuite) TestClientJobStopActionProgressTerminate() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	getResponse1 := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
				TaskStats: map[string]uint32{
					"SUCCEEDED": 10,
				},
			},
			Config: &job.JobConfig{
				InstanceCount: 10,
			},
		},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse1, nil)

	suite.NoError(c.JobStopAction(testJobID, true))
}

// TestClientJobStopActionProgressError tests stopping a job and getting
// an error while fetching the progress
func (suite *jobActionsTestSuite) TestClientJobStopActionProgressError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(nil, errors.New("unable to get job"))

	suite.Error(c.JobStopAction(testJobID, true))
}

// TestClientJobStopActionProgressIterError tests stopping a job and
// getting an error while monitoring the progress
func (suite *jobActionsTestSuite) TestClientJobStopActionProgressIterError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(resp, nil)

	getResponse1 := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
				TaskStats: map[string]uint32{
					"RUNNING":   2,
					"SUCCEEDED": 8,
				},
			},
			Config: &job.JobConfig{
				InstanceCount: 10,
			},
		},
	}

	gomock.InOrder(
		suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).Return(getResponse1, nil),

		suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).Return(nil, errors.New("unable to get job")),
	)

	suite.Error(c.JobStopAction(testJobID, true))
}

// TestClientJobStopActionGetError tests getting an error in
// job get while stopping a job
func (suite *jobActionsTestSuite) TestClientJobStopActionGetError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(nil, errors.New("unable to get job"))

	suite.Error(c.JobStopAction(testJobID, false))
}

// TestClientJobStopActionTerminalJob tests stopping a terminated job
func (suite *jobActionsTestSuite) TestClientJobStopActionTerminalJob() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_SUCCEEDED,
			},
		},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.NoError(c.JobStopAction(testJobID, false))
}

// TestClientJobStopActionStopError tests getting
// an error when stopping a job
func (suite *jobActionsTestSuite) TestClientJobStopActionStopError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: &peloton.JobID{
			Value: testJobID,
		},
	}).Return(getResponse, nil)

	suite.mockTask.EXPECT().
		Stop(gomock.Any(), &task.StopRequest{
			JobId: &peloton.JobID{
				Value: testJobID,
			},
			Ranges: nil,
		}).
		Return(nil, errors.New("unable to stop job"))

	suite.Error(c.JobStopAction(testJobID, false))
}

// TestClientJobStopAction tests error during second stop
// API call when stopping a job
func (suite *jobActionsTestSuite) TestClientJobStopActionIterError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	getResponse := &job.GetResponse{
		JobInfo: &job.JobInfo{
			Id: &peloton.JobID{
				Value: testJobID,
			},
			Runtime: &job.RuntimeInfo{
				State: job.JobState_RUNNING,
			},
		},
	}

	resp := &task.StopResponse{
		StoppedInstanceIds: []uint32{1, 2},
		InvalidInstanceIds: []uint32{3, 4},
	}

	gomock.InOrder(
		suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
			Id: &peloton.JobID{
				Value: testJobID,
			},
		}).Return(getResponse, nil),

		suite.mockTask.EXPECT().
			Stop(gomock.Any(), &task.StopRequest{
				JobId: &peloton.JobID{
					Value: testJobID,
				},
				Ranges: nil,
			}).
			Return(resp, nil),

		suite.mockTask.EXPECT().
			Stop(gomock.Any(), &task.StopRequest{
				JobId: &peloton.JobID{
					Value: testJobID,
				},
				Ranges: nil,
			}).
			Return(resp, errors.New("cannot stop job")),
	)

	suite.Error(c.JobStopAction(testJobID, false))
}

// TestClientJobRestartActionSuccess tests restarting successfully
func (suite *jobActionsTestSuite) TestClientJobRestartActionSuccess() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	restartResponse := &job.RestartResponse{
		ResourceVersion: 2,
		UpdateID:        &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.mockJob.EXPECT().Restart(gomock.Any(), &job.RestartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		RestartConfig: &job.RestartConfig{
			BatchSize: 1,
		},
	}).Return(restartResponse, nil)

	suite.NoError(c.JobRestartAction(testJobID, 1, nil, 1))
}

// TestClientJobRestartActionNonResVersionSuppliedSuccess tests restarting successfully
// without user provides resversion
func (suite *jobActionsTestSuite) TestClientJobRestartActionNonResVersionProvidedSuccess() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	restartResponse := &job.RestartResponse{
		ResourceVersion: 2,
		UpdateID:        &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.mockJob.EXPECT().Restart(gomock.Any(), &job.RestartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		RestartConfig: &job.RestartConfig{
			BatchSize: 1,
		},
	}).Return(restartResponse, nil)

	suite.NoError(c.JobRestartAction(testJobID, 0, nil, 1))
}

// TestClientJobRestartActionError tests restarting fails with concurrency
func (suite *jobActionsTestSuite) TestClientJobRestartActionConcurrencyError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	suite.Error(c.JobRestartAction(testJobID, 2, nil, 1))
}

// TestClientJobRestartActionError tests restarting fails with error
func (suite *jobActionsTestSuite) TestClientJobRestartActionError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	restartResponse := &job.RestartResponse{}

	suite.mockJob.EXPECT().Restart(gomock.Any(), &job.RestartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		RestartConfig: &job.RestartConfig{
			BatchSize: 1,
		},
	}).Return(restartResponse, errors.New("test error"))

	suite.Error(c.JobRestartAction(testJobID, 1, nil, 1))
}

// TestClientJobRestartActionConcurrencyFailRetry tests restarting fails due to
// concurrency error and retry succeeds
func (suite *jobActionsTestSuite) TestClientJobRestartActionConcurrencyFailRetrySucceeds() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	suite.mockJob.EXPECT().Restart(gomock.Any(), &job.RestartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		RestartConfig: &job.RestartConfig{
			BatchSize: 1,
		},
	}).Return(nil, yarpcerrors.InvalidArgumentErrorf(invalidVersionError))

	restartResponse := &job.RestartResponse{}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 2,
			},
		},
	}, nil)

	suite.mockJob.EXPECT().Restart(gomock.Any(), &job.RestartRequest{
		Id:              jobID,
		ResourceVersion: 2,
		RestartConfig: &job.RestartConfig{
			BatchSize: 1,
		},
	}).Return(restartResponse, nil)

	suite.NoError(c.JobRestartAction(testJobID, 0, nil, 1))
}

// TestClientJobStartActionSuccess tests starting successfully
func (suite *jobActionsTestSuite) TestClientJobStartActionSuccess() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	startResponse := &job.StartResponse{
		ResourceVersion: 2,
		UpdateID:        &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.mockJob.EXPECT().Start(gomock.Any(), &job.StartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		StartConfig: &job.StartConfig{
			BatchSize: 1,
		},
	}).Return(startResponse, nil)

	suite.NoError(c.JobStartAction(testJobID, 1, nil, 1))
}

// TestClientJobStartActionError tests starting fails with error
func (suite *jobActionsTestSuite) TestClientJobStartActionError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	startResponse := &job.StartResponse{}

	suite.mockJob.EXPECT().Start(gomock.Any(), &job.StartRequest{
		Id:              jobID,
		ResourceVersion: 1,
		StartConfig: &job.StartConfig{
			BatchSize: 1,
		},
	}).Return(startResponse, errors.New("test error"))

	suite.Error(c.JobStartAction(testJobID, 1, nil, 1))
}

// TestClientJobStopV2ActionSuccess tests stop successfully
func (suite *jobActionsTestSuite) TestClientJobStopV2ActionSuccess() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	stopResponse := &job.StopResponse{
		ResourceVersion: 2,
		UpdateID:        &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.mockJob.EXPECT().Stop(gomock.Any(), &job.StopRequest{
		Id:              jobID,
		ResourceVersion: 1,
		StopConfig: &job.StopConfig{
			BatchSize: 1,
		},
	}).Return(stopResponse, nil)

	suite.NoError(c.JobStopV1BetaAction(testJobID, 1, nil, 1))
}

// TestClientJobStopV2ActionError tests stop fails with error
func (suite *jobActionsTestSuite) TestClientJobStopV2ActionError() {
	c := Client{
		Debug:      false,
		jobClient:  suite.mockJob,
		taskClient: suite.mockTask,
		dispatcher: nil,
		ctx:        suite.ctx,
	}

	jobID := &peloton.JobID{
		Value: testJobID,
	}

	suite.mockJob.EXPECT().Get(gomock.Any(), &job.GetRequest{
		Id: jobID,
	}).Return(&job.GetResponse{
		JobInfo: &job.JobInfo{
			Runtime: &job.RuntimeInfo{
				ConfigurationVersion: 1,
			},
		},
	}, nil)

	stopResponse := &job.StopResponse{}

	suite.mockJob.EXPECT().Stop(gomock.Any(), &job.StopRequest{
		Id:              jobID,
		ResourceVersion: 1,
		StopConfig: &job.StopConfig{
			BatchSize: 1,
		},
	}).Return(stopResponse, errors.New("test error"))

	suite.Error(c.JobStopV1BetaAction(testJobID, 1, nil, 1))
}
