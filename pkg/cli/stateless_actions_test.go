// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	respoolmocks "github.com/uber/peloton/.gen/peloton/api/v0/respool/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	v1alphapod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	v1alphaquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	jobmgrsvcmocks "github.com/uber/peloton/.gen/peloton/private/jobmgrsvc/mocks"

	"github.com/uber/peloton/pkg/common/api"
	jobmgrtask "github.com/uber/peloton/pkg/jobmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
	"gopkg.in/yaml.v2"
)

const (
	testStatelessSpecConfig          = "../../example/stateless/testspec.yaml"
	testRespoolPath                  = "/testPath"
	testEntityVersion                = "1-1-1"
	testOpaqueData                   = "opaqueData"
	testMaxInstanceRetries           = uint32(2)
	testMaxTolerableInstanceFailures = uint32(3)
)

type statelessActionsTestSuite struct {
	suite.Suite
	ctx       context.Context
	client    Client
	respoolID *v1alphapeloton.ResourcePoolID

	ctrl            *gomock.Controller
	statelessClient *mocks.MockJobServiceYARPCClient
	resClient       *respoolmocks.MockResourceManagerYARPCClient
	jobmgrClient    *jobmgrsvcmocks.MockJobManagerServiceYARPCClient
}

func (suite *statelessActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.statelessClient = mocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.resClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.jobmgrClient = jobmgrsvcmocks.NewMockJobManagerServiceYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.respoolID = &v1alphapeloton.ResourcePoolID{Value: uuid.New()}
	suite.client = Client{
		Debug:           false,
		statelessClient: suite.statelessClient,
		jobmgrClient:    suite.jobmgrClient,
		resClient:       suite.resClient,
		dispatcher:      nil,
		ctx:             suite.ctx,
	}
}

func (suite *statelessActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *statelessActionsTestSuite) TestStatelessGetCacheActionSuccess() {
	suite.jobmgrClient.EXPECT().
		GetJobCache(suite.ctx, &jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(&jobmgrsvc.GetJobCacheResponse{}, nil)

	suite.NoError(suite.client.StatelessGetCacheAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessGetCacheActionError() {
	suite.jobmgrClient.EXPECT().
		GetJobCache(suite.ctx, &jobmgrsvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessGetCacheAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessRefreshAction() {
	suite.jobmgrClient.EXPECT().
		RefreshJob(suite.ctx, &jobmgrsvc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(&jobmgrsvc.RefreshJobResponse{}, nil)

	suite.NoError(suite.client.StatelessRefreshAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessRefreshActionError() {
	suite.jobmgrClient.EXPECT().
		RefreshJob(suite.ctx, &jobmgrsvc.RefreshJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRefreshAction(testJobID))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowPauseAction() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	opaque := "test"
	suite.statelessClient.EXPECT().
		PauseJobWorkflow(suite.ctx, &svc.PauseJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		}).
		Return(&svc.PauseJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowPauseAction(testJobID, entityVersion.GetValue(), opaque))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowPauseActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		PauseJobWorkflow(suite.ctx, &svc.PauseJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowPauseAction(testJobID, entityVersion.GetValue(), ""))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowResumeAction() {
	opaque := "test"
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &svc.ResumeJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		}).
		Return(&svc.ResumeJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowResumeAction(testJobID, entityVersion.GetValue(), opaque))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowResumeActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &svc.ResumeJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowResumeAction(testJobID, entityVersion.GetValue(), ""))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowAbortAction() {
	opaque := "test"
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		AbortJobWorkflow(suite.ctx, &svc.AbortJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		}).
		Return(&svc.AbortJobWorkflowResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessWorkflowAbortAction(testJobID, entityVersion.GetValue(), opaque))
}

func (suite *statelessActionsTestSuite) TestStatelessWorkflowAbortActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		AbortJobWorkflow(suite.ctx, &svc.AbortJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowAbortAction(testJobID, entityVersion.GetValue(), ""))
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
	override := false
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	rollbackOnFailure := false
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"

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
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Status: &stateless.JobStatus{
					Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		ReplaceJob(gomock.Any(), gomock.Any()).
		Return(&svc.ReplaceJobResponse{
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
		}, nil)

	suite.NoError(suite.client.StatelessReplaceJobAction(
		testJobID,
		testStatelessSpecConfig,
		batchSize,
		respoolPath,
		"",
		override,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		rollbackOnFailure,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

// TestStatelessReplaceJobActionLookupResourcePoolIDFail tests the failure case of replace
// job due to look up resource pool fails
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobActionLookupResourcePoolIDFail() {
	batchSize := uint32(1)
	override := false
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	rollbackOnFailure := false
	startPaused := true
	inPlace := false
	startPods := false

	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: testRespoolPath,
			},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessReplaceJobAction(
		testJobID,
		testStatelessSpecConfig,
		batchSize,
		testRespoolPath,
		testEntityVersion,
		override,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		rollbackOnFailure,
		startPaused,
		"",
		inPlace,
		startPods,
	))
}

// TestStatelessReplaceJobActionGetJobFail tests the failure case of replace
// job due to get job fails
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobActionGetJobFail() {
	batchSize := uint32(1)
	respoolPath := "/testPath"
	override := false
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	rollbackOnFailure := false
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"

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
		GetJob(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessReplaceJobAction(
		testJobID,
		testStatelessSpecConfig,
		batchSize,
		respoolPath,
		"",
		override,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		rollbackOnFailure,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

func (suite *statelessActionsTestSuite) TestStatelessRollbackJobActionSuccess() {
	batchSize := uint32(1)
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"
	entityVersion := "1-1-1"

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name: "new-job",
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Status: &stateless.JobStatus{
					Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		ReplaceJob(gomock.Any(), gomock.Any()).
		Return(&svc.ReplaceJobResponse{
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
		}, nil)

	suite.NoError(suite.client.StatelessRollbackJobAction(
		testJobID,
		batchSize,
		entityVersion,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

// TestStatelessReplaceJobActionLookupResourcePoolIDFail tests the failure case of replace
// job due to look up resource pool fails
func (suite *statelessActionsTestSuite) TestStatelessRollbackJobGetFailure() {
	batchSize := uint32(1)
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"
	entityVersion := "1-1-1"

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{}, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRollbackJobAction(
		testJobID,
		batchSize,
		entityVersion,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

// TestStatelessReplaceJobActionGetJobFail tests the failure case of replace
// job due to get job fails
func (suite *statelessActionsTestSuite) TestStatelessRollbackJobGetJobSummaryFail() {
	batchSize := uint32(1)
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"
	entityVersion := "1-1-1"

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name: "new-job",
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{}, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRollbackJobAction(
		testJobID,
		batchSize,
		entityVersion,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

func (suite *statelessActionsTestSuite) TestStatelessRollbackJobReplaceFailure() {
	batchSize := uint32(1)
	maxInstanceRetries := uint32(2)
	maxTolerableInstanceFailures := uint32(1)
	startPaused := true
	inPlace := false
	startPods := false
	opaque := "test"
	entityVersion := "1-1-1"

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name: "new-job",
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Status: &stateless.JobStatus{
					Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
				},
			},
		}, nil)

	suite.statelessClient.EXPECT().
		ReplaceJob(gomock.Any(), gomock.Any()).
		Return(&svc.ReplaceJobResponse{}, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRollbackJobAction(
		testJobID,
		batchSize,
		entityVersion,
		maxInstanceRetries,
		maxTolerableInstanceFailures,
		startPaused,
		opaque,
		inPlace,
		startPods,
	))
}

// TestStatelessListJobsActionSuccess tests executing
// ListJobsAction successfully
func (suite *statelessActionsTestSuite) TestStatelessListJobsActionSuccess() {
	stream := mocks.NewMockJobServiceServiceListJobsYARPCClient(suite.ctrl)
	jobs := &svc.ListJobsResponse{
		Jobs: []*stateless.JobSummary{
			{
				JobId: &v1alphapeloton.JobID{Value: testJobID},
				Name:  "test",
			},
		},
	}

	suite.statelessClient.EXPECT().
		ListJobs(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	gomock.InOrder(
		stream.EXPECT().
			Recv().
			Return(jobs, nil),
		stream.EXPECT().
			Recv().
			Return(nil, io.EOF),
	)

	suite.NoError(suite.client.StatelessListJobsAction())
}

// TestStatelessListJobsActionSuccess tests executing
// ListJobsAction and getting an error from the initial connection
func (suite *statelessActionsTestSuite) TestStatelessListJobsActionError() {
	suite.statelessClient.EXPECT().
		ListJobs(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessListJobsAction())
}

// TestStatelessListJobsActionSuccess tests executing
// ListJobsAction and getting an error in stream receive
func (suite *statelessActionsTestSuite) TestStatelessListJobsActionRecvError() {
	stream := mocks.NewMockJobServiceServiceListJobsYARPCClient(suite.ctrl)

	suite.statelessClient.EXPECT().
		ListJobs(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	stream.EXPECT().
		Recv().
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessListJobsAction())
}

// TestStatelessCreateJobActionSuccess tests the success case of creating a job
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionSuccess() {
	gomock.InOrder(
		suite.resClient.EXPECT().
			LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: testRespoolPath,
				},
			}).
			Return(&respool.LookupResponse{
				Id: &peloton.ResourcePoolID{Value: suite.respoolID.GetValue()},
			}, nil),

		suite.statelessClient.EXPECT().
			CreateJob(
				gomock.Any(),
				&svc.CreateJobRequest{
					JobId: &v1alphapeloton.JobID{Value: testJobID},
					Spec:  suite.getSpec(),
					Secrets: api.ConvertV0SecretsToV1Secrets([]*peloton.Secret{
						jobmgrtask.CreateSecretProto(
							"", testSecretPath, []byte(testSecretStr),
						),
					}),
					OpaqueData: &v1alphapeloton.OpaqueData{Data: testOpaqueData},
					CreateSpec: &stateless.CreateSpec{
						StartPaused:                  false,
						BatchSize:                    0,
						MaxTolerableInstanceFailures: testMaxTolerableInstanceFailures,
						MaxInstanceRetries:           testMaxInstanceRetries,
					},
				},
			).Return(&svc.CreateJobResponse{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
		}, nil),
	)

	suite.NoError(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		testStatelessSpecConfig,
		testSecretPath,
		[]byte(testSecretStr),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

// TestStatelessCreateJobActionLookupResourcePoolIDFailure tests the failure
// case of creating a stateless job due to look up resource pool failure
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionLookupResourcePoolIDFailure() {
	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: testRespoolPath,
			},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		testStatelessSpecConfig,
		testSecretPath,
		[]byte(testSecretStr),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

// TestStatelessCreateJobActionNilResourcePoolID tests the failure case of
// creating a stateless job due to look up resource pool returns nil respoolID
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionNilResourcePoolID() {
	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: testRespoolPath,
			},
		}).Return(&respool.LookupResponse{}, nil)

	suite.Error(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		testStatelessSpecConfig,
		testSecretPath,
		[]byte(testSecretStr),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

// TestStatelessCreateJobActionJobAlreadyExists tests the failure case of
// creating a job when the jobID already exists
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionJobAlreadyExists() {
	gomock.InOrder(
		suite.resClient.EXPECT().
			LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: testRespoolPath,
				},
			}).
			Return(&respool.LookupResponse{
				Id: &peloton.ResourcePoolID{Value: suite.respoolID.GetValue()},
			}, nil),

		suite.statelessClient.EXPECT().
			CreateJob(
				gomock.Any(),
				&svc.CreateJobRequest{
					JobId:      &v1alphapeloton.JobID{Value: testJobID},
					Spec:       suite.getSpec(),
					OpaqueData: &v1alphapeloton.OpaqueData{Data: testOpaqueData},
					CreateSpec: &stateless.CreateSpec{
						StartPaused:                  false,
						BatchSize:                    0,
						MaxTolerableInstanceFailures: testMaxTolerableInstanceFailures,
						MaxInstanceRetries:           testMaxInstanceRetries,
					},
				},
			).Return(nil, yarpcerrors.AlreadyExistsErrorf("test error")),
	)

	suite.Error(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		testStatelessSpecConfig,
		"",
		[]byte(""),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

// TestStatelessCreateJobActionInvalidSpec tests the failure case of
// creating a job due to invalid spec path
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionInvalidSpecPath() {
	gomock.InOrder(
		suite.resClient.EXPECT().
			LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: testRespoolPath,
				},
			}).
			Return(&respool.LookupResponse{
				Id: &peloton.ResourcePoolID{Value: suite.respoolID.GetValue()},
			}, nil),
	)

	suite.Error(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		"invalid-path",
		"",
		[]byte(""),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

// TestStatelessCreateJobActionUnmarshalFailed tests the failure case of
// creating a job due to error while unmarshaling job spec
func (suite *statelessActionsTestSuite) TestStatelessCreateJobActionInvalidSpec() {
	gomock.InOrder(
		suite.resClient.EXPECT().
			LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
				Path: &respool.ResourcePoolPath{
					Value: testRespoolPath,
				},
			}).
			Return(&respool.LookupResponse{
				Id: &peloton.ResourcePoolID{Value: suite.respoolID.GetValue()},
			}, nil),

		suite.statelessClient.EXPECT().
			CreateJob(
				gomock.Any(),
				&svc.CreateJobRequest{
					JobId:      &v1alphapeloton.JobID{Value: testJobID},
					Spec:       suite.getSpec(),
					OpaqueData: &v1alphapeloton.OpaqueData{Data: testOpaqueData},
					CreateSpec: &stateless.CreateSpec{
						StartPaused:                  false,
						BatchSize:                    0,
						MaxTolerableInstanceFailures: testMaxTolerableInstanceFailures,
						MaxInstanceRetries:           testMaxInstanceRetries,
					},
				},
			).Return(nil, yarpcerrors.InvalidArgumentErrorf("test error")),
	)

	suite.Error(suite.client.StatelessCreateAction(
		testJobID,
		testRespoolPath,
		0,
		testStatelessSpecConfig,
		"",
		[]byte(""),
		testOpaqueData,
		false,
		testMaxInstanceRetries,
		testMaxTolerableInstanceFailures,
	))
}

func (suite *statelessActionsTestSuite) getSpec() *stateless.JobSpec {
	var jobSpec stateless.JobSpec
	buffer, err := ioutil.ReadFile(testStatelessSpecConfig)
	suite.NoError(err)
	err = yaml.Unmarshal(buffer, &jobSpec)
	suite.NoError(err)
	jobSpec.RespoolId = suite.respoolID
	return &jobSpec
}

// TestStatelessReplaceJobDiffActionSuccess tests successfully invoking
// the GetReplaceJobDiff API
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobDiffActionSuccess() {
	respoolPath := "/testPath"

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
		GetReplaceJobDiff(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, req *svc.GetReplaceJobDiffRequest) {
			suite.Equal(req.GetVersion().GetValue(), testEntityVersion)
			suite.Equal(req.GetJobId().GetValue(), testJobID)
		}).
		Return(&svc.GetReplaceJobDiffResponse{
			InstancesAdded: []*v1alphapod.InstanceIDRange{
				{
					From: uint32(0),
					To:   uint32(5),
				},
			},
		}, nil)

	suite.NoError(suite.client.StatelessReplaceJobDiffAction(
		testJobID,
		testStatelessSpecConfig,
		testEntityVersion,
		respoolPath,
	))
}

// TestStatelessReplaceJobDiffActionSuccess tests getting an error on invoking
// the GetReplaceJobDiff API
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobDiffActionFail() {
	respoolPath := "/testPath"

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
		GetReplaceJobDiff(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessReplaceJobDiffAction(
		testJobID,
		testStatelessSpecConfig,
		testEntityVersion,
		respoolPath,
	))
}

// TestStatelessReplaceJobActionLookupResourcePoolIDFail tests the failure case
// of the GetReplaceJobDiff API due to the look up resource pool failing
func (suite *statelessActionsTestSuite) TestStatelessReplaceJobDiffActionLookupRPFail() {
	respoolPath := "/testPath"

	suite.resClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{
				Value: respoolPath,
			},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessReplaceJobDiffAction(
		testJobID,
		testStatelessSpecConfig,
		testEntityVersion,
		respoolPath,
	))
}

// TestStatelessStopJobActionSuccess tests the success case of stopping job
func (suite *statelessActionsTestSuite) TestStatelessStopJobActionSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		StopJob(suite.ctx, &svc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(&svc.StopJobResponse{
			Version: entityVersion,
		}, nil)

	suite.NoError(suite.client.StatelessStopJobAction(testJobID, entityVersion.GetValue()))
}

// TestStatelessStopJobActionFailure tests the failure of stopping job
func (suite *statelessActionsTestSuite) TestStatelessStopJobActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	suite.statelessClient.EXPECT().
		StopJob(suite.ctx, &svc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessStopJobAction(testJobID, entityVersion.GetValue()))
}

// TestClientJobGetSuccess test the success case of
// getting status and spec of a stateless job
func (suite *statelessActionsTestSuite) TestClientJobGetSuccess() {
	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(&svc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				JobId: &v1alphapeloton.JobID{
					Value: testJobID,
				},
				Spec: &stateless.JobSpec{
					Name: testJobID,
					DefaultSpec: &v1alphapod.PodSpec{
						Revocable: true,
					},
				},
				Status: &stateless.JobStatus{
					State: stateless.JobState_JOB_STATE_RUNNING,
				},
			},
		}, nil)

	suite.NoError(suite.client.StatelessGetAction(testJobID, "3-1", false))
}

// TestClientPodGetCacheSuccess test the failure case of getting cache
func (suite *statelessActionsTestSuite) TestClientJobGetFail() {
	suite.statelessClient.EXPECT().
		GetJob(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessGetAction(testJobID, "3-1-1", false))
}

// TestStatelessRestartJobActionSuccess tests the success path of restart job
func (suite *statelessActionsTestSuite) TestStatelessRestartJobActionSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	opaque := "test"
	batchSize := uint32(1)
	instanceRanges := []*task.InstanceRange{{From: 0, To: 10}}

	suite.statelessClient.
		EXPECT().
		RestartJob(gomock.Any(), &svc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges: []*v1alphapod.InstanceIDRange{
					{From: 0, To: 10},
				},
				InPlace: true,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		}).
		Return(&svc.RestartJobResponse{Version: entityVersion}, nil)

	suite.NoError(suite.client.StatelessRestartJobAction(
		testJobID,
		batchSize,
		entityVersion.GetValue(),
		instanceRanges,
		opaque,
		true,
	))
}

// TestStatelessRestartJobActionFailure tests the failure path of restart job
func (suite *statelessActionsTestSuite) TestStatelessRestartJobActionFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: testEntityVersion}
	opaque := "test"
	batchSize := uint32(1)
	instanceRanges := []*task.InstanceRange{{From: 0, To: 10}}

	suite.statelessClient.
		EXPECT().
		RestartJob(gomock.Any(), &svc.RestartJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			RestartSpec: &stateless.RestartSpec{
				BatchSize: batchSize,
				Ranges: []*v1alphapod.InstanceIDRange{
					{From: 0, To: 10},
				},
				InPlace: false,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessRestartJobAction(
		testJobID,
		batchSize,
		entityVersion.GetValue(),
		instanceRanges,
		opaque,
		false,
	))
}

// TestStatelessListUpdatesAction tests the success case of list updates
func (suite *statelessActionsTestSuite) TestStatelessListUpdatesAction() {
	suite.statelessClient.EXPECT().
		ListJobWorkflows(suite.ctx, &svc.ListJobWorkflowsRequest{
			JobId:        &v1alphapeloton.JobID{Value: testJobID},
			UpdatesLimit: 1,
		}).
		Return(&svc.ListJobWorkflowsResponse{}, nil)

	suite.NoError(suite.client.StatelessListUpdatesAction(testJobID, 1))
}

// TestStatelessListUpdatesActionError tests the failure case of list updates
func (suite *statelessActionsTestSuite) TestStatelessListUpdatesActionError() {
	suite.statelessClient.EXPECT().
		ListJobWorkflows(suite.ctx, &svc.ListJobWorkflowsRequest{
			JobId:        &v1alphapeloton.JobID{Value: testJobID},
			UpdatesLimit: 1,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessListUpdatesAction(testJobID, 1))
}

// TestStatelessWorkflowEventsAction tests the success path
// for get workflow events
func (suite *statelessActionsTestSuite) TestStatelessWorkflowEventsAction() {
	suite.statelessClient.EXPECT().
		GetWorkflowEvents(suite.ctx, &svc.GetWorkflowEventsRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			InstanceId: 0,
		}).
		Return(&svc.GetWorkflowEventsResponse{}, nil)

	suite.NoError(suite.client.StatelessWorkflowEventsAction(testJobID, 0))
}

// TestStatelessWorkflowEventsActionError tests the failure path
// for get workflow events
func (suite *statelessActionsTestSuite) TestStatelessWorkflowEventsActionError() {
	suite.statelessClient.EXPECT().
		GetWorkflowEvents(suite.ctx, &svc.GetWorkflowEventsRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessWorkflowEventsAction(testJobID, 0))
}

// TestStatelessQueryPodsActionSuccess tests the success case of querying pods
func (suite *statelessActionsTestSuite) TestStatelessQueryPodsActionSuccess() {
	podState := "POD_STATE_RUNNING"
	host := "peloton-mesos-agent0"
	name := "test-pod"
	sortBy := "creation_time"
	pagination := &v1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  100,
		OrderBy: []*v1alphaquery.OrderBy{
			{
				Order: v1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &v1alphaquery.PropertyPath{
					Value: sortBy,
				},
			},
		},
	}

	request := &svc.QueryPodsRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec: &v1alphapod.QuerySpec{
			Pagination: pagination,
			PodStates:  []v1alphapod.PodState{v1alphapod.PodState_POD_STATE_RUNNING},
			Hosts:      []string{host},
			Names:      []*v1alphapeloton.PodName{{Value: name}},
		},
		Pagination: pagination,
	}

	suite.statelessClient.EXPECT().
		QueryPods(gomock.Any(), request).
		Return(&svc.QueryPodsResponse{
			Pods: []*v1alphapod.PodInfo{
				{
					Status: &v1alphapod.PodStatus{
						PodId: &v1alphapeloton.PodID{Value: testPodID},
						State: v1alphapod.PodState_POD_STATE_RUNNING,
						Host:  host,
						ContainersStatus: []*v1alphapod.ContainerStatus{
							{
								Name:  name,
								State: v1alphapod.ContainerState_CONTAINER_STATE_RUNNING,
								Healthy: &v1alphapod.HealthStatus{
									State: v1alphapod.HealthState_HEALTH_STATE_HEALTHY,
								},
								StartTime: time.Now().Format(time.RFC3339Nano),
							},
						},
					},
				},
			},
		}, nil)

	suite.NoError(suite.client.StatelessQueryPodsAction(
		testJobID,
		podState,
		name,
		host,
		100,
		0,
		sortBy,
		"ASC",
	))
}

// TestStatelessQueryPodsActionNoResult tests the case
// where there are no pods matching the query criteria
func (suite *statelessActionsTestSuite) TestStatelessQueryPodsActionNoResult() {
	suite.statelessClient.EXPECT().
		QueryPods(gomock.Any(), gomock.Any()).
		Return(&svc.QueryPodsResponse{}, nil)

	suite.NoError(suite.client.StatelessQueryPodsAction(
		testJobID,
		"",
		"",
		"",
		100,
		0,
		"",
		"ASC",
	))
}

// TestStatelessQueryPodsActionJobNotFound tests the failure
// case of querying pods due to job not found error
func (suite *statelessActionsTestSuite) TestStatelessQueryPodsActionJobNotFound() {
	suite.statelessClient.EXPECT().
		QueryPods(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.NotFoundErrorf("test error"))

	suite.Error(suite.client.StatelessQueryPodsAction(
		testJobID,
		"",
		"",
		"",
		100,
		0,
		"",
		"ASC",
	))
}

// TestStatelessQueryPodsActionInvalidSortOrder tests the failure
// case of querying pods due to invalid sort order
func (suite *statelessActionsTestSuite) TestStatelessQueryPodsActionInvalidSortOrder() {
	suite.Error(suite.client.StatelessQueryPodsAction(
		testJobID,
		"",
		"",
		"",
		100,
		0,
		"",
		"",
	))
}

func getListPodsResponse() svc.ListPodsResponse {
	return svc.ListPodsResponse{
		Pods: []*v1alphapod.PodSummary{
			{
				PodName: &v1alphapeloton.PodName{
					Value: "test-1",
				},
				Status: &v1alphapod.PodStatus{
					State: v1alphapod.PodState_POD_STATE_RUNNING,
					PodId: &v1alphapeloton.PodID{Value: testPodID},
					ContainersStatus: []*v1alphapod.ContainerStatus{
						{
							Name:  "test-container",
							State: v1alphapod.ContainerState_CONTAINER_STATE_RUNNING,
							Healthy: &v1alphapod.HealthStatus{
								State: v1alphapod.HealthState_HEALTH_STATE_HEALTHY,
							},
							StartTime: time.Now().Format(time.RFC3339Nano),
						},
						{
							Name:  "test-container2",
							State: v1alphapod.ContainerState_CONTAINER_STATE_RUNNING,
							Healthy: &v1alphapod.HealthStatus{
								State: v1alphapod.HealthState_HEALTH_STATE_HEALTHY,
							},
							StartTime: time.Now().Format(time.RFC3339Nano),
							TerminationStatus: &v1alphapod.TerminationStatus{
								Reason:   v1alphapod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST,
								ExitCode: 0,
								Signal:   "",
							},
						},
					},
				},
			},
		},
	}
}

// TestClientListPodsSuccess tests a successful execution of ListPods action
func (suite *statelessActionsTestSuite) TestClientListPodsSuccess() {
	stream := mocks.NewMockJobServiceServiceListPodsYARPCClient(suite.ctrl)
	pods := getListPodsResponse()
	suite.statelessClient.EXPECT().
		ListPods(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	gomock.InOrder(
		stream.EXPECT().
			Recv().
			Return(&pods, nil),
		stream.EXPECT().
			Recv().
			Return(nil, io.EOF),
	)

	suite.NoError(suite.client.StatelessListPodsAction(
		testJobID,
		&task.InstanceRange{
			From: uint32(0),
			To:   uint32(5),
		},
	))
}

// Test that the printPod works correctly
func (suite *statelessActionsTestSuite) TestClientPrintStatelessListPodsResponse() {
	podsResponse := getListPodsResponse()
	for _, pod := range podsResponse.GetPods() {
		assert.NotPanics(suite.T(), func() { printPod(pod.GetStatus(), pod.GetPodName()) })
	}
}

// TestClientListPodsActionError tests ListPods action getting an error on
// ininvoking the ListPods API
func (suite *statelessActionsTestSuite) TestClientListPodsActionError() {
	suite.statelessClient.EXPECT().
		ListPods(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))
	suite.Error(suite.client.StatelessListPodsAction(testJobID, nil))
}

// TestClientListPodsStreamError tests receiving an error from the stream
// while fetching pods summary via ListPods action
func (suite *statelessActionsTestSuite) TestClientListPodsStreamError() {
	stream := mocks.NewMockJobServiceServiceListPodsYARPCClient(suite.ctrl)

	suite.statelessClient.EXPECT().
		ListPods(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	stream.EXPECT().
		Recv().
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessListPodsAction(testJobID, nil))
}

// TestClientStartJobSuccess tests the success case of starting a stateless job
func (suite *statelessActionsTestSuite) TestClientStartJobSuccess() {
	suite.statelessClient.EXPECT().
		StartJob(gomock.Any(), gomock.Any()).
		Return(&svc.StartJobResponse{
			Version: &v1alphapeloton.EntityVersion{
				Value: testEntityVersion,
			},
		}, nil)

	suite.NoError(suite.client.StatelessStartJobAction(testJobID, testEntityVersion))
}

// TestClientStartJobFail tests the failure case of starting a stateless job
func (suite *statelessActionsTestSuite) TestClientStartJobFail() {
	suite.statelessClient.EXPECT().
		StartJob(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessStartJobAction(testJobID, testEntityVersion))
}

// TestStatelessDeleteActionSuccess tests the success case of deleting a job
func (suite *statelessActionsTestSuite) TestStatelessDeleteActionSuccess() {
	suite.statelessClient.EXPECT().
		DeleteJob(suite.ctx, &svc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
			Force:   true,
		}).
		Return(&svc.DeleteJobResponse{}, nil)

	suite.NoError(suite.client.StatelessDeleteAction(testJobID, testEntityVersion, true))
}

// TestStatelessDeleteActionFailure tests the failure case of deleting a job
func (suite *statelessActionsTestSuite) TestStatelessDeleteActionFailure() {
	suite.statelessClient.EXPECT().
		DeleteJob(suite.ctx, &svc.DeleteJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{Value: testEntityVersion},
			Force:   true,
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.StatelessDeleteAction(testJobID, testEntityVersion, true))
}

func TestStatelessActions(t *testing.T) {
	suite.Run(t, new(statelessActionsTestSuite))
}
