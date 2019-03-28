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
	"strconv"
	"testing"

	"github.com/pborman/uuid"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	jobmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	podmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"
	pbquery "github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	aurorabridgemocks "github.com/uber/peloton/pkg/aurorabridge/mocks"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/aurorabridge/mockutil"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"

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

	config        ServiceHandlerConfig
	thermosConfig atop.ThermosExecutorConfig

	handler *ServiceHandler
}

func (suite *ServiceHandlerTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.ctrl = gomock.NewController(suite.T())
	suite.jobClient = jobmocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.podClient = podmocks.NewMockPodServiceYARPCClient(suite.ctrl)
	suite.respoolLoader = aurorabridgemocks.NewMockRespoolLoader(suite.ctrl)

	suite.config = ServiceHandlerConfig{
		GetJobUpdateWorkers:           25,
		GetTasksWithoutConfigsWorkers: 25,
		StopPodWorkers:                25,
		PodRunsDepth:                  2,
		ThermosExecutor: atop.ThermosExecutorConfig{
			Path: "/usr/share/aurora/bin/thermos_executor.pex",
		},
	}
	handler, err := NewServiceHandler(
		suite.config,
		tally.NoopScope,
		suite.jobClient,
		suite.podClient,
		suite.respoolLoader,
	)
	suite.NoError(err)
	suite.handler = handler
}

func (suite *ServiceHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestServiceHandler(t *testing.T) {
	suite.Run(t, &ServiceHandlerTestSuite{})
}

// Tests for success scenario for GetJobSummary
func (suite *ServiceHandlerTestSuite) TestGetJobSummary() {
	role := "role1"
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	labels := fixture.DefaultPelotonJobLabels(jobKey)
	instanceCount := uint32(1)
	podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	suite.expectQueryJobsWithLabels(ql, []*peloton.JobID{jobID}, jobKey)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:          atop.NewJobName(jobKey),
					InstanceCount: instanceCount,
					DefaultSpec: &pod.PodSpec{
						PodName:    podName,
						Labels:     labels,
						Containers: []*pod.ContainerSpec{{}},
					},
				},
			},
		}, nil)

	resp, err := suite.handler.GetJobSummary(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetJobSummaryResult().GetSummaries(), 1)
}

// Tests for failure scenario for get config summary
func (suite *ServiceHandlerTestSuite) TestGetConfigSummaryFailure() {
	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	instanceCount := uint32(2)

	suite.expectGetJobIDFromJobName(jobKey, jobID)

	suite.jobClient.EXPECT().
		QueryPods(suite.ctx, &statelesssvc.QueryPodsRequest{
			JobId: jobID,
			Spec: &pod.QuerySpec{
				Pagination: &pbquery.PaginationSpec{
					Limit: instanceCount,
				},
			},
			Pagination: &pbquery.PaginationSpec{
				Limit: instanceCount,
			},
		}).
		Return(nil, errors.New("unable to query pods"))

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: true,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Name:          atop.NewJobName(jobKey),
				InstanceCount: instanceCount,
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
	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.expectQueryPods(jobID,
		[]*peloton.PodName{{Value: podName}},
		mdLabel,
		entityVersion,
		1,
	)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: true,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Name:          atop.NewJobName(jobKey),
				InstanceCount: uint32(1),
			},
		}, nil)

	resp, err := suite.handler.GetConfigSummary(
		suite.ctx,
		jobKey)
	suite.NoError(err)
	// Creates two config groups indicating set of pods which have same entity version (same pod spec)
	suite.Equal(1, len(resp.GetResult().GetConfigSummaryResult().GetSummary().GetGroups()))
}

// Tests for success scenario for GetJobs
func (suite *ServiceHandlerTestSuite) TestGetJobs() {
	role := "role1"
	jobKey := fixture.AuroraJobKey()
	jobID := fixture.PelotonJobID()
	instanceCount := uint32(1)
	podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}

	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())
	jkLabel := label.NewAuroraJobKey(jobKey)

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	suite.expectQueryJobsWithLabels(ql, []*peloton.JobID{jobID}, jobKey)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:          atop.NewJobName(jobKey),
					InstanceCount: instanceCount,
					DefaultSpec: &pod.PodSpec{
						PodName:    podName,
						Labels:     append([]*peloton.Label{jkLabel}, mdLabel...),
						Containers: []*pod.ContainerSpec{{}},
					},
				},
			},
		}, nil)

	resp, err := suite.handler.GetJobs(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetGetJobsResult().GetConfigs(), 1)
}

// Tests get job update diff
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiff() {
	respoolID := fixture.PelotonResourcePoolID()
	jobUpdateRequest := fixture.AuroraJobUpdateRequest()
	jobID := fixture.PelotonJobID()
	jobKey := jobUpdateRequest.GetTaskConfig().GetJob()
	entityVersion := fixture.PelotonEntityVersion()

	jobSpec, _ := atop.NewJobSpecFromJobUpdateRequest(
		jobUpdateRequest,
		respoolID,
		suite.config.ThermosExecutor,
	)

	addedInstancesIDRange := []*pod.InstanceIDRange{
		{
			From: uint32(5),
			To:   uint32(10),
		},
	}
	instancesAdded := &api.ConfigGroup{
		Config: fixture.AuroraTaskConfig(),
		Instances: []*api.Range{
			{
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
	suite.Nil(resp.GetResult().GetGetJobUpdateDiffResult().GetUpdate())
	suite.Nil(resp.GetResult().GetGetJobUpdateDiffResult().GetRemove())
	suite.Nil(resp.GetResult().GetGetJobUpdateDiffResult().GetUnchanged())
}

// Tests the failure scenarios for get job update diff
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiffFailure() {
	respoolID := fixture.PelotonResourcePoolID()
	jobUpdateRequest := fixture.AuroraJobUpdateRequest()
	jobID := fixture.PelotonJobID()
	jobKey := jobUpdateRequest.GetTaskConfig().GetJob()
	entityVersion := fixture.PelotonEntityVersion()
	jobSpec, _ := atop.NewJobSpecFromJobUpdateRequest(
		jobUpdateRequest,
		respoolID,
		suite.config.ThermosExecutor,
	)

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

func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiff_JobNotFound() {
	respoolID := fixture.PelotonResourcePoolID()
	k := fixture.AuroraJobKey()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k),
		}).
		Return(nil, yarpcerrors.NotFoundErrorf("job not found"))

	resp, err := suite.handler.GetJobUpdateDiff(
		suite.ctx,
		&api.JobUpdateRequest{
			TaskConfig:    &api.TaskConfig{Job: k},
			InstanceCount: ptr.Int32(10),
		})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetGetJobUpdateDiffResult()
	suite.Equal(int32(0), result.GetAdd()[0].GetInstances()[0].GetFirst())
	suite.Equal(int32(9), result.GetAdd()[0].GetInstances()[0].GetLast())
	suite.Empty(result.GetUpdate())
	suite.Empty(result.GetRemove())
	suite.Empty(result.GetUnchanged())
}

func (suite *ServiceHandlerTestSuite) TestGetTierConfigs() {
	resp, err := suite.handler.GetTierConfigs(suite.ctx)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Ensures StartJobUpdate creates jobs which don't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobSuccess() {
	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.jobClient.EXPECT().
		CreateJob(suite.ctx, gomock.Any()).
		Return(&statelesssvc.CreateJobResponse{}, nil)

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
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
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.jobClient.EXPECT().
		ReplaceJob(
			suite.ctx,
			mockutil.MatchReplaceJobRequestUpdateActions(nil)).
		Return(&statelesssvc.ReplaceJobResponse{}, nil)

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
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
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(suite.ctx).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.jobClient.EXPECT().
		ReplaceJob(
			suite.ctx,
			mockutil.MatchReplaceJobRequestUpdateActions([]opaquedata.UpdateAction{
				opaquedata.StartPulsed,
			})).
		Return(&statelesssvc.ReplaceJobResponse{}, nil)

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
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

// Ensures StartJobUpdate errors out when seeing pinned instances request
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_PinnedInstancesError() {
	req := fixture.AuroraJobUpdateRequest()
	req.Settings = &api.JobUpdateSettings{
		UpdateOnlyTheseInstances: []*api.Range{
			{
				First: ptr.Int32(0),
				Last:  ptr.Int32(0),
			},
		},
	}

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

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

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

// Ensures PauseJobUpdate returns INVALID_REQUEST if update id does not match workflow.
func (suite *ServiceHandlerTestSuite) TestPauseJobUpdate_InvalidUpdateID() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, "some other id", v)

	resp, err := suite.handler.PauseJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures ResumeJobUpdate successfully maps to ResumeJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestResumeJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

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

// Ensures ResumeJobUpdate returns INVALID_REQUEST if update id does not match workflow.
func (suite *ServiceHandlerTestSuite) TestResumeJobUpdate_InvalidUpdateID() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, "some other id", v)

	resp, err := suite.handler.ResumeJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures AbortJobUpdate successfully maps to AbortJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestAbortJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

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

// Ensures AbortJobUpdate returns INVALID_REQUEST if update id does not match workflow.
func (suite *ServiceHandlerTestSuite) TestAbortJobUpdate_InvalidUpdateID() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, "some other id", v)

	resp, err := suite.handler.AbortJobUpdate(suite.ctx, k, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures PulseJobUpdate calls ResumeJobWorkflow if the update is awaiting pulse.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_ResumesIfAwaitingPulse() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	d := &opaquedata.Data{UpdateID: k.GetID()}
	d.AppendUpdateAction(opaquedata.StartPulsed)

	curOD, err := d.Serialize()
	suite.NoError(err)

	d.AppendUpdateAction(opaquedata.Pulse)

	newOD, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Status: &stateless.JobStatus{
					Version: v,
				},
			},
			WorkflowInfo: &stateless.WorkflowInfo{
				OpaqueData: curOD,
				Status: &stateless.WorkflowStatus{
					State: stateless.WorkflowState_WORKFLOW_STATE_PAUSED,
				},
			},
		}, nil)

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

	d := &opaquedata.Data{UpdateID: k.GetID()}
	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				OpaqueData: od,
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

// Ensures PulseJobUpdate returns INVALID_REQUEST if update id does not match workflow.
func (suite *ServiceHandlerTestSuite) TestPulseJobUpdate_InvalidUpdateID() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, "some other id", v)

	resp, err := suite.handler.PulseJobUpdate(suite.ctx, k)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
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
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
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
			JobId:  jobID,
			Name:   atop.NewJobName(jobKey),
			Labels: labels,
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

func (suite *ServiceHandlerTestSuite) expectGetJobAndWorkflow(
	jobID *peloton.JobID,
	updateID string,
	v *peloton.EntityVersion,
) {
	d := &opaquedata.Data{UpdateID: updateID}
	od, err := d.Serialize()
	suite.NoError(err)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Status: &stateless.JobStatus{
					Version: v,
				},
			},
			WorkflowInfo: &stateless.WorkflowInfo{
				OpaqueData: od,
			},
		}, nil)
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

	labels := append(
		label.BuildPartialAuroraJobKeyLabels(role, env, ""),
		common.BridgeJobLabel,
	)
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

// TestGetJobIDsFromTaskQuery_PartialJobKeyFilterUnexpected checks
// getJobIDsFromTaskQuery returns result when input query only contains
// partial job key parameters - role, environment, and/or job_name,
// meanwhile jobs that does not contain expected labels are filtered out.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_PartialJobKeyFilterUnexpected() {
	role := "role1"
	env := "env1"
	jobID1 := fixture.PelotonJobID()
	jobID2 := fixture.PelotonJobID()
	jobID3 := fixture.PelotonJobID()

	labels := append(
		label.BuildPartialAuroraJobKeyLabels(role, env, ""),
		common.BridgeJobLabel,
	)

	var summaries []*stateless.JobSummary
	for _, jobID := range []*peloton.JobID{jobID1, jobID2} {
		summaries = append(summaries, &stateless.JobSummary{
			JobId:  jobID,
			Name:   atop.NewJobName(nil),
			Labels: labels,
		})
	}

	var unexpctedLabels []*peloton.Label
	for _, l := range labels {
		unexpctedLabels = append(unexpctedLabels, &peloton.Label{
			Key:   l.GetKey(),
			Value: l.GetValue() + "_unexpcted",
		})
	}
	summaries = append(summaries, &stateless.JobSummary{
		JobId:  jobID3,
		Name:   atop.NewJobName(nil),
		Labels: unexpctedLabels,
	})

	suite.jobClient.EXPECT().
		QueryJobs(suite.ctx,
			&statelesssvc.QueryJobsRequest{
				Spec: &stateless.QuerySpec{
					Labels: labels,
				},
			}).
		Return(&statelesssvc.QueryJobsResponse{Records: summaries}, nil)

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

	labels := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", name),
		common.BridgeJobLabel,
	)

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

func (suite *ServiceHandlerTestSuite) expectGetJob(
	jobKey *api.JobKey,
	jobID *peloton.JobID,
	instanceCount uint32,
) {
	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: false,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					Name:          atop.NewJobName(jobKey),
					InstanceCount: instanceCount,
				},
			},
		}, nil)
}

func (suite *ServiceHandlerTestSuite) expectGetJobSummary(
	jobKey *api.JobKey,
	jobID *peloton.JobID,
	instanceCount uint32,
) {
	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			SummaryOnly: true,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				Name:          atop.NewJobName(jobKey),
				InstanceCount: instanceCount,
			},
		}, nil)
}

func (suite *ServiceHandlerTestSuite) expectQueryPods(
	jobID *peloton.JobID,
	podNames []*peloton.PodName,
	labels []*peloton.Label,
	entityVersion *peloton.EntityVersion,
	currentRunID int,
) {
	var pods []*pod.PodInfo
	for _, podName := range podNames {
		pods = append(pods, &pod.PodInfo{
			Spec: &pod.PodSpec{
				PodName:    podName,
				Labels:     labels,
				Containers: []*pod.ContainerSpec{{}},
			},
			Status: &pod.PodStatus{
				PodId:   &peloton.PodID{Value: podName.GetValue() + "-" + strconv.Itoa(currentRunID)},
				Host:    "peloton-host-0",
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion,
			},
		})
	}

	suite.jobClient.EXPECT().
		QueryPods(suite.ctx, &statelesssvc.QueryPodsRequest{
			JobId: jobID,
			Spec: &pod.QuerySpec{
				PodStates: nil,
				Pagination: &pbquery.PaginationSpec{
					Limit: uint32(len(podNames)),
				},
			},
			Pagination: &pbquery.PaginationSpec{
				Limit: uint32(len(podNames)),
			},
		}).
		Return(&statelesssvc.QueryPodsResponse{Pods: pods}, nil)
}

// TestGetTasksWithoutConfigs_ParallelismSuccess tests parallelism for
// GetTasksWithoutConfig success scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_ParallelismSuccess() {
	query := fixture.AuroraTaskQuery()
	jobKey := query.GetJobKeys()[0]
	jobID := fixture.PelotonJobID()
	entityVersion := fixture.PelotonEntityVersion()
	labels := fixture.DefaultPelotonJobLabels(jobKey)

	suite.expectGetJobSummary(jobKey, jobID, 1000)

	var podNames []*peloton.PodName
	for i := 0; i < 1000; i++ {
		podName := &peloton.PodName{
			Value: util.CreatePelotonTaskID(jobID.GetValue(), uint32(i)),
		}
		podID := &peloton.PodID{Value: podName.GetValue() + "-1"}
		podNames = append(podNames, podName)

		suite.podClient.EXPECT().
			GetPodEvents(gomock.Any(), &podsvc.GetPodEventsRequest{
				PodName: podName,
			}).
			Return(&podsvc.GetPodEventsResponse{
				Events: []*pod.PodEvent{
					{
						PodId:       podID,
						Timestamp:   "2019-01-03T22:14:58Z",
						Message:     "",
						ActualState: task.TaskState_RUNNING.String(),
						Hostname:    "peloton-host-0",
					},
				},
			}, nil)
	}

	suite.expectQueryPods(jobID, podNames, labels, entityVersion, 1)

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 1000)
}

// TestGetTasksWithoutConfigs_ParallelismFailure tests parallelism for
// GetTasksWithoutConfig failure scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_ParallelismFailure() {
	query := fixture.AuroraTaskQuery()
	jobKey := query.GetJobKeys()[0]
	jobID := fixture.PelotonJobID()
	entityVersion := fixture.PelotonEntityVersion()
	labels := fixture.DefaultPelotonJobLabels(jobKey)

	suite.expectGetJobSummary(jobKey, jobID, 1000)

	var podNames []*peloton.PodName
	for i := 0; i < 1000; i++ {
		podName := &peloton.PodName{
			Value: util.CreatePelotonTaskID(jobID.GetValue(), uint32(i)),
		}
		podID := &peloton.PodID{Value: podName.GetValue() + "-1"}
		podNames = append(podNames, podName)

		if i%100 == 0 {
			suite.podClient.EXPECT().
				GetPodEvents(gomock.Any(), &podsvc.GetPodEventsRequest{
					PodName: podName,
				}).
				Return(&podsvc.GetPodEventsResponse{},
					errors.New("failed to get pod events")).
				MaxTimes(1)
			continue
		}

		suite.podClient.EXPECT().
			GetPodEvents(gomock.Any(), &podsvc.GetPodEventsRequest{
				PodName: podName,
			}).
			Return(&podsvc.GetPodEventsResponse{
				Events: []*pod.PodEvent{
					{
						PodId:       podID,
						Timestamp:   "2019-01-03T22:14:58Z",
						Message:     "",
						ActualState: task.TaskState_RUNNING.String(),
						Hostname:    "peloton-host-0",
					},
				},
			}, nil).
			MaxTimes(1)
	}

	suite.expectQueryPods(jobID, podNames, labels, entityVersion, 1)

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 0)
	suite.NotEmpty(resp.GetDetails())
}

type podRun struct {
	runID int
}

// TestGetTasksWithoutConfigs_QueryPreviousRuns tests GetTasksWithoutConfig
// returns pods from previous runs correctly based on PodRunsDepth config
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_QueryPreviousRuns() {
	query := fixture.AuroraTaskQuery()
	jobKey := query.GetJobKeys()[0]
	query.Statuses = map[api.ScheduleStatus]struct{}{
		api.ScheduleStatusRunning: {},
	}
	jobID := fixture.PelotonJobID()
	entityVersion := fixture.PelotonEntityVersion()
	labels := fixture.DefaultPelotonJobLabels(jobKey)

	// Sets up jobKey to jobID mapping, and returns a basic JobInfo
	// for the specific jobID
	suite.expectGetJobSummary(jobKey, jobID, 2)

	// Sets up GetPodEvents queries for all the pods (pod 0 and 1) from
	// all the runs (run 1, 2, 3) - PodEvent will be used to construct
	// aurora ScheduledTask struct
	expectPods := []struct {
		instanceID     string
		runID          string
		prevRunID      string
		currentRun     bool
		taskState      task.TaskState
		expectInResult bool
	}{
		{
			// pod 0 latest run
			// expect to be included in output
			instanceID:     "0",
			runID:          "3",
			prevRunID:      "2",
			currentRun:     true,
			taskState:      task.TaskState_RUNNING,
			expectInResult: true,
		},
		{
			// pod 0 previous run
			// expect to be included in output since PodRunsDepth = 2
			instanceID:     "0",
			runID:          "2",
			prevRunID:      "1",
			currentRun:     false,
			taskState:      task.TaskState_RUNNING,
			expectInResult: true,
		},
		{
			// pod 0 oldest run
			// do not expect to be included in output since PodRunsDepth = 2
			instanceID:     "0",
			runID:          "1",
			prevRunID:      "",
			currentRun:     false,
			taskState:      task.TaskState_RUNNING,
			expectInResult: false,
		},
		{
			// pod 1 latest run
			// expect to be included in output
			instanceID:     "1",
			runID:          "3",
			prevRunID:      "2",
			currentRun:     true,
			taskState:      task.TaskState_RUNNING,
			expectInResult: true,
		},
		{
			// pod 1 previous run
			// do not expect to be included in output since task state is FAILED
			instanceID:     "1",
			runID:          "2",
			prevRunID:      "1",
			currentRun:     false,
			taskState:      task.TaskState_FAILED,
			expectInResult: false,
		},
		{
			// pod 1 oldest run
			// do not expect to be included in output since PodRunsDepth = 2
			instanceID:     "1",
			runID:          "1",
			prevRunID:      "",
			currentRun:     false,
			taskState:      task.TaskState_RUNNING,
			expectInResult: false,
		},
	}

	expectedPodNames := make(map[string]struct{})
	expectedTaskIds := make(map[string]struct{})

	for _, e := range expectPods {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-" + e.instanceID}
		expectedPodNames[podName.GetValue()] = struct{}{}

		podID := &peloton.PodID{Value: podName.GetValue() + "-" + e.runID}

		var prevPodID *peloton.PodID
		if e.prevRunID != "" {
			prevPodID = &peloton.PodID{Value: podName.GetValue() + "-" + e.prevRunID}
		}

		if e.expectInResult {
			expectedTaskIds[podID.GetValue()] = struct{}{}
		}

		podEvents := []*pod.PodEvent{
			{
				PodId:       podID,
				PrevPodId:   prevPodID,
				Timestamp:   "2019-01-03T22:14:58Z",
				Message:     "",
				ActualState: e.taskState.String(),
				Hostname:    "peloton-host-0",
			},
		}

		if e.currentRun {
			suite.podClient.EXPECT().
				GetPodEvents(gomock.Any(), &podsvc.GetPodEventsRequest{
					PodName: podName,
				}).
				Return(&podsvc.GetPodEventsResponse{
					Events: podEvents,
				}, nil)
		} else {
			suite.podClient.EXPECT().
				GetPodEvents(gomock.Any(), &podsvc.GetPodEventsRequest{
					PodName: podName,
					PodId:   podID,
				}).
				Return(&podsvc.GetPodEventsResponse{
					Events: podEvents,
				}, nil).
				MaxTimes(1)
		}
	}

	// Sets up QueryPods for the specific jobID - the result
	// contains a list of pods from current run
	var podNames []*peloton.PodName
	for p := range expectedPodNames {
		podNames = append(podNames, &peloton.PodName{Value: p})
	}
	suite.expectQueryPods(jobID, podNames, labels, entityVersion, 2)

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	// Expect 3 tasks: 2 instances from current + one previous run (4)
	// minus one failed task (1)
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 3)
	for _, t := range resp.GetResult().GetScheduleStatusResult().GetTasks() {
		suite.Equal(api.ScheduleStatusRunning, t.GetStatus())
		_, ok := expectedTaskIds[t.GetAssignedTask().GetTaskId()]
		suite.True(ok)
	}
}

// Ensures that KillTasks maps to StopPods correctly.
func (suite *ServiceHandlerTestSuite) TestKillTasks_Success() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	instances := fixture.AuroraInstanceSet(50, 100)

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId:       id,
			SummaryOnly: true,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				InstanceCount: 100,
			},
		}, nil)

	for i := range instances {
		suite.podClient.EXPECT().
			StopPod(gomock.Any(), &podsvc.StopPodRequest{
				PodName: &peloton.PodName{
					Value: util.CreatePelotonTaskID(id.GetValue(), uint32(i)),
				},
			}).
			Return(&podsvc.StopPodResponse{}, nil)
	}

	resp, err := suite.handler.KillTasks(suite.ctx, k, instances, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

func pickN(m map[int32]struct{}, n int) map[int32]struct{} {
	p := make(map[int32]struct{})
	for i := range m {
		if len(p) == n {
			break
		}
		p[i] = struct{}{}
	}
	return p
}

// Ensures that if a StopPod request fails, the concurrency exits gracefully.
func (suite *ServiceHandlerTestSuite) TestKillTasks_StopPodError() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	instances := fixture.AuroraInstanceSet(50, 100)

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId:       id,
			SummaryOnly: true,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				InstanceCount: 100,
			},
		}, nil)

	// Out of the 50 instances, pick 10 to produce StopPod errors.
	shouldError := pickN(instances, 10)

	for i := range instances {
		if _, ok := shouldError[i]; ok {
			suite.podClient.EXPECT().
				StopPod(gomock.Any(), &podsvc.StopPodRequest{
					PodName: &peloton.PodName{
						Value: util.CreatePelotonTaskID(
							id.GetValue(), uint32(i)),
					},
				}).
				Return(nil, errors.New("some error")).
				MaxTimes(1)
		} else {
			suite.podClient.EXPECT().
				StopPod(gomock.Any(), &podsvc.StopPodRequest{
					PodName: &peloton.PodName{
						Value: util.CreatePelotonTaskID(
							id.GetValue(), uint32(i)),
					},
				}).
				Return(&podsvc.StopPodResponse{}, nil).
				MaxTimes(1)
		}
	}

	resp, err := suite.handler.KillTasks(suite.ctx, k, instances, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures that if the context is cancelled externally, the concurrency exits
// gracefully.
func (suite *ServiceHandlerTestSuite) TestKillTasks_CancelledContextError() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	instances := fixture.AuroraInstanceSet(50, 100)

	ctx, cancel := context.WithCancel(suite.ctx)
	cancel()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		GetJob(ctx, &statelesssvc.GetJobRequest{
			JobId:       id,
			SummaryOnly: true,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				InstanceCount: 100,
			},
		}, nil)

	resp, err := suite.handler.KillTasks(ctx, k, instances, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures that if all instances are specified in KillTasks, then StopJob
// is used instead of individual StopPods.
func (suite *ServiceHandlerTestSuite) TestKillTasks_StopAll() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	tests := []struct {
		instances map[int32]struct{}
	}{
		{
			instances: map[int32]struct{}{
				0: {},
				1: {},
				2: {},
			},
		},
		{
			instances: nil,
		},
		{
			instances: map[int32]struct{}{},
		},
	}

	for _, t := range tests {
		suite.expectGetJobIDFromJobName(k, id)

		suite.jobClient.EXPECT().
			GetJob(suite.ctx, &statelesssvc.GetJobRequest{
				JobId:       id,
				SummaryOnly: true,
			}).
			Return(&statelesssvc.GetJobResponse{
				Summary: &stateless.JobSummary{
					InstanceCount: 3,
					Status: &stateless.JobStatus{
						Version: v,
					},
				},
			}, nil)

		suite.jobClient.EXPECT().
			StopJob(suite.ctx, &statelesssvc.StopJobRequest{
				JobId:   id,
				Version: v,
			}).
			Return(&statelesssvc.StopJobResponse{}, nil)

		resp, err := suite.handler.KillTasks(suite.ctx, k, t.instances, nil)
		suite.NoError(err)
		suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	}
}

// Ensures that RollbackJobUpdate calls ReplaceJob using the previous JobSpec.
func (suite *ServiceHandlerTestSuite) TestRollbackJobUpdate_Success() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	prevVersion := fixture.PelotonEntityVersion()
	curVersion := fixture.PelotonEntityVersion()

	updateSpec := &stateless.UpdateSpec{
		BatchSize:         1,
		StartPaused:       true,
		RollbackOnFailure: true,
	}

	prevSpec := &stateless.JobSpec{
		Name:        atop.NewJobName(k.GetJob()),
		Description: "the previous job spec",
	}

	d := &opaquedata.Data{UpdateID: k.GetID()}

	curOD, err := d.Serialize()
	suite.NoError(err)

	d.AppendUpdateAction(opaquedata.Rollback)

	newOD, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Status: &stateless.JobStatus{
					Version: curVersion,
				},
			},
			WorkflowInfo: &stateless.WorkflowInfo{
				OpaqueData: curOD,
				Status: &stateless.WorkflowStatus{
					State:       stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					PrevVersion: prevVersion,
				},
				UpdateSpec: updateSpec,
			},
		}, nil)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: prevVersion,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: prevSpec,
			},
		}, nil)

	suite.jobClient.EXPECT().
		ReplaceJob(suite.ctx, &statelesssvc.ReplaceJobRequest{
			JobId:   id,
			Version: curVersion,
			Spec:    prevSpec,
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize:         1,
				StartPaused:       false,
				RollbackOnFailure: false,
			},
			OpaqueData: newOD,
		}).
		Return(&statelesssvc.ReplaceJobResponse{}, nil)

	resp, err := suite.handler.RollbackJobUpdate(suite.ctx, k, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Ensures RollbackJobUpdate returns INVALID_REQUEST if the update id does not
// match the current workflow.
func (suite *ServiceHandlerTestSuite) TestRollbackJobUpdate_InvalidUpdateID() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	d := &opaquedata.Data{UpdateID: "some other update id"}

	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				OpaqueData: od,
			},
		}, nil)

	resp, err := suite.handler.RollbackJobUpdate(suite.ctx, k, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures RollbackJobUpdate returns INVALID_REQUEST if the update was already
// rolled back.
func (suite *ServiceHandlerTestSuite) TestRollbackJobUpdate_UpdateAlreadyRolledBack() {
	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	d := &opaquedata.Data{UpdateID: k.GetID()}
	d.AppendUpdateAction(opaquedata.Rollback)

	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(suite.ctx, &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			WorkflowInfo: &stateless.WorkflowInfo{
				Status: &stateless.WorkflowStatus{
					State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
				},
				OpaqueData: od,
			},
		}, nil)

	resp, err := suite.handler.RollbackJobUpdate(suite.ctx, k, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Very simple test to ensure GetJobUpdateSummaries is hooked into
// GetJobUpdateDetails correctly. More detailed testing can be found in
// GetJobUpdateDetails tests.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummaries_Success() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId: id,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{fixture.PelotonWorkflowInfo("")},
		}, nil)

	resp, err := suite.handler.GetJobUpdateSummaries(
		suite.ctx, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetGetJobUpdateSummariesResult().GetUpdateSummaries(), 1)
}

// Very simple test checking GetJobUpdateSummaries error.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateSummaries_Error() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId: id,
		}).
		Return(nil, errors.New("some error"))

	resp, err := suite.handler.GetJobUpdateSummaries(
		suite.ctx, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Very simple test checking GetJobUpdateDetails error.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_Error() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          id,
			InstanceEvents: true,
		}).
		Return(nil, errors.New("some error"))

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx, nil, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Ensures that a NOT_FOUND error from Peloton job query results in an empty
// response.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_JobNotFound() {
	k := fixture.AuroraJobKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(suite.ctx, &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: atop.NewJobName(k),
		}).
		Return(nil, yarpcerrors.NotFoundErrorf("job not found"))

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx, nil, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Empty(resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList())
}

// Ensures that a NOT_FOUND error from Peloton workflow query results in an
// empty response.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_WorkflowsNotFound() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          id,
			InstanceEvents: true,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf("workflows not found"))

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx, nil, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Empty(resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList())
}

// Ensures that querying GetJobUpdateDetails by role returns the workflow
// history for all jobs under that role.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_QueryByRoleSuccess() {
	role := "some-role"

	labels := []*peloton.Label{
		label.NewAuroraJobKeyRole(role),
		common.BridgeJobLabel,
	}

	keys := []*api.JobKey{
		{Role: &role, Environment: ptr.String("env-1"), Name: ptr.String("job-1")},
		{Role: &role, Environment: ptr.String("env-2"), Name: ptr.String("job-2")},
	}

	summaries := []*stateless.JobSummary{
		{JobId: fixture.PelotonJobID(), Name: atop.NewJobName(keys[0]), Labels: labels},
		{JobId: fixture.PelotonJobID(), Name: atop.NewJobName(keys[1]), Labels: labels},
	}

	suite.jobClient.EXPECT().
		QueryJobs(suite.ctx, &statelesssvc.QueryJobsRequest{
			Spec: &stateless.QuerySpec{
				Labels: labels,
			},
		}).
		Return(&statelesssvc.QueryJobsResponse{
			Records: summaries,
		}, nil)

	wf0 := fixture.PelotonWorkflowInfo("2018-10-02T15:00:00Z")
	wf1 := fixture.PelotonWorkflowInfo("2018-10-02T19:00:00Z")
	wf2 := fixture.PelotonWorkflowInfo("2018-10-02T14:00:00Z")

	od, err := opaquedata.Deserialize(wf0.GetOpaqueData())
	suite.NoError(err)
	wf0updateID := od.UpdateID

	od, err = opaquedata.Deserialize(wf1.GetOpaqueData())
	suite.NoError(err)
	wf1updateID := od.UpdateID

	od, err = opaquedata.Deserialize(wf2.GetOpaqueData())
	suite.NoError(err)
	wf2updateID := od.UpdateID

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          summaries[0].JobId,
			InstanceEvents: true,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{wf0, wf1, wf2},
		}, nil)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          summaries[1].JobId,
			InstanceEvents: true,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				fixture.PelotonWorkflowInfo("2018-10-03T14:00:00Z"),
			},
		}, nil)

	// Just make sure we get 3 updates back.
	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx, nil, &api.JobUpdateQuery{Role: &role})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList(), 4)

	// Verify update details are sorted by time descending per job
	var updateIDOrder []string
	for _, details := range resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList() {
		updateKey := details.GetUpdate().GetSummary().GetKey()
		if keys[0].Equals(updateKey.GetJob()) {
			updateIDOrder = append(updateIDOrder, updateKey.GetID())
		}
	}
	suite.Equal([]string{wf1updateID, wf0updateID, wf2updateID}, updateIDOrder)
}

// Ensures that update+rollback workflows which share an UpdateID are joined.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_JoinRollbacksByUpdateID() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	// Original update.
	d := &opaquedata.Data{UpdateID: uuid.New()}
	od1, err := d.Serialize()
	suite.NoError(err)

	// Rollback update (has same UpdateID).
	d.AppendUpdateAction(opaquedata.Rollback)
	od2, err := d.Serialize()
	suite.NoError(err)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          id,
			InstanceEvents: true,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ABORTED,
					},
					OpaqueData: od1,
				}, {
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					},
					OpaqueData: od2,
				},
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx, nil, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList()
	suite.Len(result, 1)
	suite.Equal(
		api.JobUpdateStatusRollingBack,
		result[0].GetUpdate().GetSummary().GetState().GetStatus())
}

// Ensures that any updates which don't match the query's UpdateStatuses
// are filtered out.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_UpdateStatusFilter() {
	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:          id,
			InstanceEvents: true,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				}, {
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				},
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx,
		nil,
		&api.JobUpdateQuery{
			JobKey: k,
			UpdateStatuses: map[api.JobUpdateStatus]struct{}{
				api.JobUpdateStatusRolledForward: {},
			},
		})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	// The ROLLING_FORWARD update should have been filtered out.
	result := resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList()
	suite.Len(result, 1)
	suite.Equal(
		api.JobUpdateStatusRolledForward,
		result[0].GetUpdate().GetSummary().GetState().GetStatus())
}
