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

package aurorabridge

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	jobmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	podmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	aurorabridgemocks "github.com/uber/peloton/aurorabridge/mocks"

	"github.com/uber/peloton/aurorabridge/atop"
	"github.com/uber/peloton/aurorabridge/fixture"
	"github.com/uber/peloton/aurorabridge/label"
	"github.com/uber/peloton/aurorabridge/mockutil"
	"github.com/uber/peloton/aurorabridge/opaquedata"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

type ServiceHandlerTestSuite struct {
	suite.Suite

	ctx context.Context

	ctrl          *gomock.Controller
	jobClient     *jobmocks.MockJobServiceYARPCClient
	podClient     *podmocks.MockPodServiceYARPCClient
	respoolLoader *aurorabridgemocks.MockRespoolLoader

	handler *ServiceHandler
}

func (suite *ServiceHandlerTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.ctrl = gomock.NewController(suite.T())
	suite.jobClient = jobmocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.podClient = podmocks.NewMockPodServiceYARPCClient(suite.ctrl)

	suite.respoolLoader = aurorabridgemocks.NewMockRespoolLoader(suite.ctrl)

	suite.handler = NewServiceHandler(
		tally.NoopScope,
		suite.jobClient,
		suite.podClient,
		suite.respoolLoader,
	)
}

func (suite *ServiceHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestServiceHandler(t *testing.T) {
	suite.Run(t, &ServiceHandlerTestSuite{})
}

// Test fetch job update summaries using fully populated job key
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummariesWithJobKey() {
	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		JobKey:         jobKey,
		UpdateStatuses: updateStatuses,
	}

	workflowEvents := []*stateless.WorkflowEvent{{
		Type:      stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
		State:     stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
		Timestamp: time.Now().Format(time.RFC3339),
	}}

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     workflowEvents,
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateSummaries(
		context.Background(),
		jobUpdateQuery,
	)
	suite.NoError(err)
	suite.Equal(1,
		len(resp.GetResult().GetJobUpdateSummariesResult.GetUpdateSummaries()))
	suite.Equal(api.JobUpdateStatusRollingForward,
		resp.GetResult().GetJobUpdateSummariesResult.GetUpdateSummaries()[0].GetState().GetStatus())
}

// Tests fetching job update summaries with job key's role only
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummariesWithJobKeyRole() {
	jobID1 := fixture.PelotonJobID()
	jobID2 := fixture.PelotonJobID()
	jobID3 := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	updateStatuses[api.JobUpdateStatusRollingBack] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		Role:           jobKey.Role,
		JobKey:         nil,
		UpdateStatuses: updateStatuses,
	}

	labels := label.BuildPartialAuroraJobKeyLabels(&api.JobKey{
		Role: ptr.String(jobKey.GetRole()),
	})

	// Role based search returned multiple jobs
	suite.expectQueryJobsWithLabels(labels, []*peloton.JobID{jobID1, jobID2, jobID3}, jobKey)

	// fetch update for job1
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID1,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     nil,
			},
		}, nil)

	// fetch update for job2
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID2,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     nil,
			},
		}, nil)

	// fetch update for job3, and will be ignored as it is in INITIALIZED state
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID3,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     nil,
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateSummaries(
		context.Background(),
		jobUpdateQuery,
	)
	suite.NoError(err)
	suite.Equal(2, len(resp.GetResult().GetGetJobUpdateSummariesResult().GetUpdateSummaries()))
}

// Test fetch job update summaries by job update statuses only,
// with job key not present
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummariesUpdateStatuses() {
	jobID1 := fixture.PelotonJobID() // Update is ROLLING_FORWARD
	jobID2 := fixture.PelotonJobID() // Update is ROLLING_BACKWARD and will be filtered
	jobKey := fixture.AuroraJobKey()

	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		Role:           nil,
		JobKey:         nil,
		UpdateStatuses: updateStatuses,
	}

	// Query all the jobs
	suite.expectQueryJobsWithLabels(nil, []*peloton.JobID{jobID1, jobID2}, jobKey)

	// Get updates for all jobs and filter those updates using update query state
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID1,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     nil,
			},
		}, nil)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID2,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
				},
				UpdateSpec: &stateless.UpdateSpec{
					BatchSize:         1,
					RollbackOnFailure: false,
				},
				OpaqueData: nil,
				Events:     nil,
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateSummaries(
		context.Background(),
		jobUpdateQuery,
	)
	suite.NoError(err)
	suite.Equal(1, len(resp.GetResult().GetGetJobUpdateSummariesResult().GetUpdateSummaries()))
}

// Tests failure scenarios for fetching get job update summaries
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummariesFailure() {
	testJobKeyRole := "dummy_role"
	jobKey := &api.JobKey{
		Role: &testJobKeyRole,
	}
	jobUpdateQuery := &api.JobUpdateQuery{
		Role: &testJobKeyRole,
	}

	suite.jobClient.EXPECT().
		QueryJobs(
			suite.ctx,
			&statelesssvc.QueryJobsRequest{
				Spec: &stateless.QuerySpec{
					Labels: label.BuildPartialAuroraJobKeyLabels(jobKey),
				},
			}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	resp, err := suite.handler.GetJobUpdateSummaries(
		context.Background(),
		jobUpdateQuery,
	)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	jobID := fixture.PelotonJobID()
	jobIDs := []*v1alphapeloton.JobID{jobID}
	records := []*stateless.JobSummary{{
		JobId: jobID,
	}}

	suite.jobClient.EXPECT().
		QueryJobs(
			suite.ctx,
			&statelesssvc.QueryJobsRequest{
				Spec: &stateless.QuerySpec{
					Labels: label.BuildPartialAuroraJobKeyLabels(jobKey),
				},
			}).
		Return(&statelesssvc.QueryJobsResponse{
			Records: records,
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobIDs[0],
		}).Return(nil, errors.New("unable to list job updates"))

	resp, err = suite.handler.GetJobUpdateSummaries(
		context.Background(),
		jobUpdateQuery,
	)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Tests parallelism for getJobUpdateDetails success scenario
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetailsParallelismSuccess() {
	var jobIDs []*v1alphapeloton.JobID
	for i := 0; i < 1000; i++ {
		jobID := fixture.PelotonJobID()
		jobIDs = append(jobIDs, jobID)
	}

	jobKey := fixture.AuroraJobKey()
	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		UpdateStatuses: updateStatuses,
	}

	suite.expectQueryJobsWithLabels(nil, jobIDs, jobKey)

	for i := 0; i < 1000; i++ {
		suite.jobClient.EXPECT().
			GetJob(suite.ctx, &statelesssvc.GetJobRequest{
				JobId: jobIDs[i],
			}).
			Return(&statelesssvc.GetJobResponse{
				WorkflowInfo: &stateless.WorkflowInfo{
					Status: &stateless.WorkflowStatus{
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					},
					UpdateSpec: &stateless.UpdateSpec{
						BatchSize:         1,
						RollbackOnFailure: false,
					},
					OpaqueData: nil,
					Events:     nil,
				},
			}, nil)
	}

	resp, _ := suite.handler.getJobUpdateDetails(
		suite.ctx,
		jobUpdateQuery,
		false)
	suite.Equal(1000, len(resp))
}

// Tests parallelism for getJobUpdateDetails with few updates not present
// in expected update statuses
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetailsParallelismFilterUpdates() {
	var jobIDs []*v1alphapeloton.JobID
	for i := 0; i < 1000; i++ {
		jobID := fixture.PelotonJobID()
		jobIDs = append(jobIDs, jobID)
	}

	jobKey := fixture.AuroraJobKey()
	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		UpdateStatuses: updateStatuses,
	}

	suite.expectQueryJobsWithLabels(nil, jobIDs, jobKey)

	for i := 0; i < 1000; i++ {
		if i%100 == 0 {
			suite.jobClient.EXPECT().
				GetJob(suite.ctx, &statelesssvc.GetJobRequest{
					JobId: jobIDs[i],
				}).
				Return(&statelesssvc.GetJobResponse{
					WorkflowInfo: &stateless.WorkflowInfo{
						Status: &stateless.WorkflowStatus{
							Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
							State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
						},
						UpdateSpec: &stateless.UpdateSpec{
							BatchSize:         1,
							RollbackOnFailure: false,
						},
						OpaqueData: nil,
						Events:     nil,
					},
				}, nil)
			continue
		}
		suite.jobClient.EXPECT().
			GetJob(suite.ctx, &statelesssvc.GetJobRequest{
				JobId: jobIDs[i],
			}).
			Return(&statelesssvc.GetJobResponse{
				WorkflowInfo: &stateless.WorkflowInfo{
					Status: &stateless.WorkflowStatus{
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					},
					UpdateSpec: &stateless.UpdateSpec{
						BatchSize:         1,
						RollbackOnFailure: false,
					},
					OpaqueData: nil,
					Events:     nil,
				},
			}, nil)
	}

	resp, _ := suite.handler.getJobUpdateDetails(
		suite.ctx,
		jobUpdateQuery,
		false)
	suite.Equal(990, len(resp))
}

// Tests parallelism for getJobUpdateDetails with few updates not present
// in expected update statuses and few throwing error
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetailsParallelismFailure() {
	var jobIDs []*v1alphapeloton.JobID
	for i := 0; i < 1000; i++ {
		jobID := fixture.PelotonJobID()
		jobIDs = append(jobIDs, jobID)
	}

	jobKey := fixture.AuroraJobKey()
	updateStatuses := make(map[api.JobUpdateStatus]struct{})
	updateStatuses[api.JobUpdateStatusRollingForward] = struct{}{}
	jobUpdateQuery := &api.JobUpdateQuery{
		UpdateStatuses: updateStatuses,
	}

	suite.expectQueryJobsWithLabels(nil, jobIDs, jobKey)

	for i := 0; i < 1000; i++ {
		if i == 500 {
			suite.jobClient.EXPECT().
				GetJob(suite.ctx, &statelesssvc.GetJobRequest{
					JobId: jobIDs[i],
				}).
				Return(nil, errors.New("unable to get update"))
			continue
		}
		if i%100 == 0 {
			suite.jobClient.EXPECT().
				GetJob(suite.ctx, &statelesssvc.GetJobRequest{
					JobId: jobIDs[i],
				}).
				Return(&statelesssvc.GetJobResponse{
					WorkflowInfo: &stateless.WorkflowInfo{
						Status: &stateless.WorkflowStatus{
							Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
							State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_BACKWARD,
						},
						UpdateSpec: &stateless.UpdateSpec{
							BatchSize:         1,
							RollbackOnFailure: false,
						},
						OpaqueData: nil,
						Events:     nil,
					},
				}, nil).
				AnyTimes()
			continue
		}
		suite.jobClient.EXPECT().
			GetJob(suite.ctx, &statelesssvc.GetJobRequest{
				JobId: jobIDs[i],
			}).
			Return(&statelesssvc.GetJobResponse{
				WorkflowInfo: &stateless.WorkflowInfo{
					Status: &stateless.WorkflowStatus{
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					},
					UpdateSpec: &stateless.UpdateSpec{
						BatchSize:         1,
						RollbackOnFailure: false,
					},
					OpaqueData: nil,
					Events:     nil,
				},
			}, nil).
			AnyTimes()
	}

	resp, err := suite.handler.getJobUpdateDetails(
		suite.ctx,
		jobUpdateQuery,
		false)
	suite.Equal(0, len(resp))
	suite.NotEmpty(err.msg)
}

// Tests for failure scenario for get config summary
func (suite *ServiceHandlerTestSuite) TestGetConfigSummaryFailure() {
	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		QueryPods(suite.ctx, &statelesssvc.QueryPodsRequest{
			JobId: jobID,
			Spec:  &pod.QuerySpec{},
		}).
		Return(nil, errors.New("unable to query pods"))
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:   atop.NewJobName(jobKey),
					Labels: label.BuildPartialAuroraJobKeyLabels(jobKey),
				},
			},
		}, nil)

	resp, err := suite.handler.GetConfigSummary(
		suite.ctx,
		jobKey)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

func (suite *ServiceHandlerTestSuite) TestGetConfigSummarySuccess() {
	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	entityVersion := fixture.PelotonEntityVersion()
	podName := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	podID := fmt.Sprintf("%s-%d", podName, 1)
	mdLabel, err := label.NewAuroraMetadata(fixture.AuroraMetadata())
	suite.NoError(err)

	podInfos := []*pod.PodInfo{&pod.PodInfo{
		Spec: &pod.PodSpec{
			PodName: &peloton.PodName{
				Value: podName,
			},
			Labels: []*peloton.Label{
				mdLabel,
			},
		},
		Status: &pod.PodStatus{
			PodId: &peloton.PodID{
				Value: podID,
			},
			Version: entityVersion,
		},
	}}

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		QueryPods(suite.ctx, &statelesssvc.QueryPodsRequest{
			JobId: jobID,
			Spec:  &pod.QuerySpec{},
		}).
		Return(&statelesssvc.QueryPodsResponse{
			Pods: podInfos,
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:   atop.NewJobName(jobKey),
					Labels: label.BuildPartialAuroraJobKeyLabels(jobKey),
				},
			},
		}, nil)

	resp, err := suite.handler.GetConfigSummary(
		suite.ctx,
		jobKey)
	suite.NoError(err)
	// Creates two config groups indicating set of pods which have same entity version (same pod spec)
	suite.Equal(1, len(resp.GetResult().GetConfigSummaryResult().GetSummary().GetGroups()))
}

// Tests get job update diff
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiff() {
	respoolID := fixture.PelotonResourcePoolID()
	jobUpdateRequest := fixture.AuroraJobUpdateRequest()
	jobID := fixture.PelotonJobID()
	jobKey := jobUpdateRequest.GetTaskConfig().GetJob()
	entityVersion := fixture.PelotonEntityVersion()

	jobSpec, _ := atop.NewJobSpecFromJobUpdateRequest(jobUpdateRequest, respoolID)

	addedInstancesIDRange := []*pod.InstanceIDRange{
		&pod.InstanceIDRange{
			From: uint32(5),
			To:   uint32(10),
		},
	}
	instancesAdded := &api.ConfigGroup{
		Config: fixture.AuroraTaskConfig(),
		Instances: []*api.Range{
			&api.Range{
				First: ptr.Int32(5),
				Last:  ptr.Int32(10),
			},
		},
	}

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.expectGetJobVersion(jobID, entityVersion)
	suite.jobClient.EXPECT().
		GetReplaceJobDiff(
			suite.ctx,
			&statelesssvc.GetReplaceJobDiffRequest{
				JobId:   jobID,
				Version: entityVersion,
				Spec:    jobSpec,
			}).Return(&statelesssvc.GetReplaceJobDiffResponse{
		InstancesAdded: addedInstancesIDRange,
	}, nil)

	resp, err := suite.handler.GetJobUpdateDiff(
		suite.ctx,
		jobUpdateRequest,
	)
	suite.NoError(err)
	suite.Equal(instancesAdded.GetInstances(),
		resp.GetResult().GetGetJobUpdateDiffResult().GetAdd()[0].GetInstances())
}

// Tests the failure scenarios for get job update diff
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiffFailure() {
	respoolID := fixture.PelotonResourcePoolID()
	jobUpdateRequest := fixture.AuroraJobUpdateRequest()
	jobID := fixture.PelotonJobID()
	jobKey := jobUpdateRequest.GetTaskConfig().GetJob()
	entityVersion := fixture.PelotonEntityVersion()
	jobSpec, _ := atop.NewJobSpecFromJobUpdateRequest(jobUpdateRequest, respoolID)

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.expectGetJobVersion(jobID, entityVersion)

	suite.jobClient.EXPECT().
		GetReplaceJobDiff(
			suite.ctx,
			&statelesssvc.GetReplaceJobDiffRequest{
				JobId:   jobID,
				Version: entityVersion,
				Spec:    jobSpec,
			}).Return(nil, errors.New("unable to get replace job diff"))

	resp, err := suite.handler.GetJobUpdateDiff(
		suite.ctx,
		jobUpdateRequest,
	)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures StartJobUpdate creates jobs which don't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobSuccess() {
	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)
	newv := fixture.PelotonEntityVersion()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

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
	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	name := atop.NewJobName(req.GetTaskConfig().GetJob())

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

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

// Ensures StartJobUpdate replaces jobs which already exist with no pulse.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_ReplaceJobNoPulseSuccess() {
	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	curv := fixture.PelotonEntityVersion()
	newv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.jobClient.EXPECT().
		ReplaceJob(
			suite.ctx,
			mockutil.MatchReplaceJobRequestOpaqueData(&opaquedata.Data{})).
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

// Ensures StartJobUpdate replaces jobs which already exist with pulse.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_ReplaceJobWithPulseSuccess() {
	respoolID := fixture.PelotonResourcePoolID()
	req := &api.JobUpdateRequest{
		TaskConfig: fixture.AuroraTaskConfig(),
		Settings: &api.JobUpdateSettings{
			BlockIfNoPulsesAfterMs: ptr.Int32(1000),
		},
	}
	k := req.GetTaskConfig().GetJob()
	curv := fixture.PelotonEntityVersion()
	newv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.jobClient.EXPECT().
		ReplaceJob(
			suite.ctx,
			mockutil.MatchReplaceJobRequestOpaqueData(&opaquedata.Data{
				UpdateActions: []opaquedata.UpdateAction{
					opaquedata.StartPulsed,
				},
			})).
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
	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	curv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.jobClient.EXPECT().
		ReplaceJob(suite.ctx, gomock.Any()).
		Return(nil, yarpcerrors.AbortedErrorf(""))

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures PauseJobUpdate successfully maps to PauseJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestPauseJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobVersion(id, v)

	suite.jobClient.EXPECT().
		PauseJobWorkflow(suite.ctx, &statelesssvc.PauseJobWorkflowRequest{
			JobId:   id,
			Version: v,
		}).
		Return(nil, nil)

	resp, err := suite.handler.PauseJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Ensures ResumeJobUpdate successfully maps to ResumeJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestResumeJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobVersion(id, v)

	suite.jobClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &statelesssvc.ResumeJobWorkflowRequest{
			JobId:   id,
			Version: v,
		}).
		Return(nil, nil)

	resp, err := suite.handler.ResumeJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Tests error handling for ResumeJobUpdate.
func (suite *ServiceHandlerTestSuite) TestResumeJobUpdate_Error() {
	k := fixture.AuroraJobUpdateKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k.GetJob()),
		}).
		Return(nil, yarpcerrors.UnknownErrorf("some error"))

	resp, err := suite.handler.ResumeJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures AbortJobUpdate successfully maps to AbortJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestAbortJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobVersion(id, v)

	suite.jobClient.EXPECT().
		AbortJobWorkflow(suite.ctx, &statelesssvc.AbortJobWorkflowRequest{
			JobId:   id,
			Version: v,
		}).
		Return(nil, nil)

	resp, err := suite.handler.AbortJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

func (suite *ServiceHandlerTestSuite) TestAbortJobUpdate_Error() {
	k := fixture.AuroraJobUpdateKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k.GetJob()),
		}).
		Return(nil, yarpcerrors.UnknownErrorf("some error"))

	resp, err := suite.handler.AbortJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures PulseJobUpdate calls ResumeJobWorkflow if the update is awaiting pulse.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_ResumesIfAwaitingPulse() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	od, err := opaquedata.SerializeActions(opaquedata.StartPulsed)
	suite.NoError(err)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				OpaqueData: od,
				Status: &stateless.WorkflowStatus{
					State:   stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
					Version: v,
				},
			},
		}, nil)

	newOD, err := opaquedata.SerializeActions(opaquedata.StartPulsed, opaquedata.Pulse)
	suite.NoError(err)

	suite.jobClient.EXPECT().
		ResumeJobWorkflow(suite.ctx, &statelesssvc.ResumeJobWorkflowRequest{
			JobId:      id,
			Version:    v,
			OpaqueData: newOD,
		}).
		Return(nil, nil)

	resp, err := suite.handler.PulseJobUpdate(suite.ctx, k)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Equal(api.JobUpdatePulseStatusOk, resp.GetResult().GetPulseJobUpdateResult().GetStatus())
}

// Ensures PulseJobUpdate no-ops if update is not awaiting pulse.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_NoopsIfNotAwaitingPulse() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					State:   stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					Version: v,
				},
			},
		}, nil)

	resp, err := suite.handler.PulseJobUpdate(suite.ctx, k)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Equal(api.JobUpdatePulseStatusOk, resp.GetResult().GetPulseJobUpdateResult().GetStatus())
}

// Tests error handling for PulseJobUpdate.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_Error() {
	k := fixture.AuroraJobUpdateKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k.GetJob()),
		}).
		Return(nil, yarpcerrors.UnknownErrorf("some error"))

	resp, err := suite.handler.PulseJobUpdate(suite.ctx, k)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Tests error handling for PulseJobUpdate.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_NotFoundJobIsInvalidRequest() {
	k := fixture.AuroraJobUpdateKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k.GetJob()),
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	resp, err := suite.handler.PulseJobUpdate(suite.ctx, k)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

func (suite *ServiceHandlerTestSuite) expectGetJobIDFromJobName(k *api.JobKey, id *peloton.JobID) {
	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k),
		}).
		Return(&statelesssvc.GetJobIDFromJobNameResponse{
			JobId: []*peloton.JobID{id},
		}, nil)
}

func (suite *ServiceHandlerTestSuite) expectQueryJobsWithLabels(
	labels []*peloton.Label,
	jobIDs []*peloton.JobID,
	jobKey *api.JobKey,
) {
	var summaries []*stateless.JobSummary
	for _, jobID := range jobIDs {
		summaries = append(summaries, &stateless.JobSummary{
			JobId: jobID,
			Name:  atop.NewJobName(jobKey),
		})
	}

	suite.jobClient.EXPECT().
		QueryJobs(suite.ctx,
			&statelesssvc.QueryJobsRequest{
				Spec: &stateless.QuerySpec{
					Labels: labels,
				},
			}).
		Return(&statelesssvc.QueryJobsResponse{Records: summaries}, nil)
}

func (suite *ServiceHandlerTestSuite) expectGetJobVersion(id *peloton.JobID, v *peloton.EntityVersion) {
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: true,
			JobId:       id,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Status: &stateless.JobStatus{
					Version: v,
				},
			},
		}, nil)
}

// TestGetJobIDsFromTaskQuery_ErrorQuery checks getJobIDsFromTaskQuery
// when query is not valid.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_ErrorQuery() {
	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, nil)
	suite.Nil(jobIDs)
	suite.Error(err)

	query := &api.TaskQuery{}

	jobIDs, err = suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.Nil(jobIDs)
	suite.Error(err)
}

// TestGetJobIDsFromTaskQuery_JobKeysOnly checks getJobIDsFromTaskQuery
// returns result when input query only contains JobKeys.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_JobKeysOnly() {
	jobKey1 := &api.JobKey{
		Role:        ptr.String("role1"),
		Environment: ptr.String("env1"),
		Name:        ptr.String("name1"),
	}
	jobKey2 := &api.JobKey{
		Role:        ptr.String("role1"),
		Environment: ptr.String("env1"),
		Name:        ptr.String("name2"),
	}
	jobID1 := fixture.PelotonJobID()
	jobID2 := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(jobKey1, jobID1)
	suite.expectGetJobIDFromJobName(jobKey2, jobID2)

	query := &api.TaskQuery{JobKeys: []*api.JobKey{jobKey1, jobKey2}}

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(2, len(jobIDs))
	for _, jobID := range jobIDs {
		if jobID.GetValue() != jobID1.GetValue() &&
			jobID.GetValue() != jobID2.GetValue() {
			suite.Fail("unexpected job id: \"%s\"", jobID.GetValue())
		}
	}
}

// TestGetJobIDsFromTaskQuery_JobKeysOnlyError checks getJobIDsFromTaskQuery
// returns error when the query fails and input query only consists
// of JobKeys.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_JobKeysOnlyError() {
	jobKey := &api.JobKey{
		Role:        ptr.String("role1"),
		Environment: ptr.String("env1"),
		Name:        ptr.String("name1"),
	}
	query := &api.TaskQuery{JobKeys: []*api.JobKey{jobKey}}

	// when GetJobIDFromJobName returns error
	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx,
			&statelesssvc.GetJobIDFromJobNameRequest{
				JobName: atop.NewJobName(jobKey),
			}).
		Return(nil, errors.New("failed to get job identifiers from job name"))

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.Error(err)
	suite.Nil(jobIDs)

	// when GetJobIDFromJobName returns not found error
	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx,
			&statelesssvc.GetJobIDFromJobNameRequest{
				JobName: atop.NewJobName(jobKey),
			}).
		Return(nil, yarpcerrors.NotFoundErrorf("job id for job name not found"))

	jobIDs, err = suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.NoError(err)
	suite.Empty(jobIDs)
}

// TestGetJobIDsFromTaskQuery_FullJobKey checks getJobIDsFromTaskQuery
// returns result when input query contains full job key parameters -
// role, environment, and job_name.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_FullJobKey() {
	role := "role1"
	env := "env1"
	name := "name1"
	jobKey := &api.JobKey{
		Role:        ptr.String(role),
		Environment: ptr.String(env),
		Name:        ptr.String(name),
	}
	jobID := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(jobKey, jobID)

	query := &api.TaskQuery{
		Role:        ptr.String(role),
		Environment: ptr.String(env),
		JobName:     ptr.String(name),
	}

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(1, len(jobIDs))
	suite.Equal(jobID.GetValue(), jobIDs[0].GetValue())
}

// TestGetJobIDsFromTaskQuery_PartialJobKey checks getJobIDsFromTaskQuery
// returns result when input query only contains partial job key parameters -
// role, environment, and/or job_name.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_PartialJobKey() {
	role := "role1"
	env := "env1"
	jobID1 := fixture.PelotonJobID()
	jobID2 := fixture.PelotonJobID()

	labels := label.BuildPartialAuroraJobKeyLabels(&api.JobKey{
		Role:        ptr.String(role),
		Environment: ptr.String(env),
	})
	suite.expectQueryJobsWithLabels(labels, []*peloton.JobID{jobID1, jobID2}, nil)

	query := &api.TaskQuery{
		Role:        ptr.String(role),
		Environment: ptr.String(env),
	}

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(2, len(jobIDs))
	for _, jobID := range jobIDs {
		if jobID.Value != jobID1.Value && jobID.Value != jobID2.Value {
			suite.Fail("unexpected job id: \"%s\"", jobID.Value)
		}
	}
}

// TestGetJobIDsFromTaskQuery_PartialJobKeyError checks getJobIDsFromTaskQuery
// returns error when the query fails and input query only contains partial
// job key parameters - role, environment, and/or job_name.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_PartialJobKeyError() {
	role := "role1"
	name := "name1"
	labels := label.BuildPartialAuroraJobKeyLabels(&api.JobKey{
		Role: ptr.String(role),
		Name: ptr.String(name),
	})

	suite.jobClient.EXPECT().
		QueryJobs(suite.ctx,
			&statelesssvc.QueryJobsRequest{
				Spec: &stateless.QuerySpec{
					Labels: labels,
				},
			}).
		Return(nil, errors.New("failed to get job summary"))

	query := &api.TaskQuery{
		Role:    ptr.String(role),
		JobName: ptr.String(name),
	}

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.Nil(jobIDs)
	suite.Error(err)
}

// TestGetTasksWithoutConfigs checks GetTasksWithoutConfigs works correctly.
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs() {
	query := fixture.AuroraTaskQuery()
	jobKey := query.GetJobKeys()[0]
	jobID := fixture.PelotonJobID()
	podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}

	mdLabel, err := label.NewAuroraMetadata(fixture.AuroraMetadata())
	suite.NoError(err)
	jkLabel := label.NewAuroraJobKey(jobKey)

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:   atop.NewJobName(jobKey),
					Labels: label.BuildPartialAuroraJobKeyLabels(jobKey),
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		QueryPods(suite.ctx, &statelesssvc.QueryPodsRequest{
			JobId: jobID,
			Spec:  &pod.QuerySpec{PodStates: nil},
		}).
		Return(&statelesssvc.QueryPodsResponse{
			Pods: []*pod.PodInfo{
				{
					Spec: &pod.PodSpec{
						PodName: podName,
						Labels:  []*peloton.Label{mdLabel, jkLabel},
					},
					Status: &pod.PodStatus{
						PodId: &peloton.PodID{Value: podName.GetValue() + "-1"},
						Host:  "peloton-host-0",
						State: pod.PodState_POD_STATE_RUNNING,
					},
				},
			},
		}, nil)
	suite.podClient.EXPECT().
		GetPodEvents(suite.ctx, &podsvc.GetPodEventsRequest{
			PodName: podName,
		}).
		Return(&podsvc.GetPodEventsResponse{
			Events: []*pod.PodEvent{
				{
					Timestamp:   "2019-01-03T22:14:58Z",
					Message:     "",
					ActualState: task.TaskState_RUNNING.String(),
				},
			},
		}, nil)

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 1)
}
