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
	"io"
	"strconv"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	jobmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	podmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	jobmgrmocks "github.com/uber/peloton/.gen/peloton/private/jobmgrsvc/mocks"
	"github.com/uber/peloton/.gen/thrift/aurora/api"
	cachemocks "github.com/uber/peloton/pkg/aurorabridge/cache/mocks"
	commonmocks "github.com/uber/peloton/pkg/aurorabridge/common/mocks"
	aurorabridgemocks "github.com/uber/peloton/pkg/aurorabridge/mocks"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"
	"github.com/uber/peloton/pkg/aurorabridge/mockutil"
	"github.com/uber/peloton/pkg/aurorabridge/opaquedata"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/goleak"
	"go.uber.org/thriftrw/ptr"
	"go.uber.org/yarpc/yarpcerrors"
)

const _randomUUID = "ca22d714-b063-41cf-b563-a6ba8ef8681d"

type ServiceHandlerTestSuite struct {
	suite.Suite

	ctx context.Context

	ctrl           *gomock.Controller
	jobClient      *jobmocks.MockJobServiceYARPCClient
	jobmgrClient   *jobmgrmocks.MockJobManagerServiceYARPCClient
	listPodsStream *jobmocks.MockJobServiceServiceListPodsYARPCClient
	podClient      *podmocks.MockPodServiceYARPCClient
	respoolLoader  *aurorabridgemocks.MockRespoolLoader
	random         *commonmocks.MockRandom
	jobIdCache     *cachemocks.MockJobIDCache

	config        ServiceHandlerConfig
	thermosConfig config.ThermosExecutorConfig

	handler *ServiceHandler
}

func (suite *ServiceHandlerTestSuite) SetupTest() {
	suite.ctx = context.Background()

	suite.ctrl = gomock.NewController(suite.T())
	suite.jobClient = jobmocks.NewMockJobServiceYARPCClient(suite.ctrl)
	suite.jobmgrClient = jobmgrmocks.NewMockJobManagerServiceYARPCClient(suite.ctrl)
	suite.listPodsStream = jobmocks.NewMockJobServiceServiceListPodsYARPCClient(suite.ctrl)
	suite.podClient = podmocks.NewMockPodServiceYARPCClient(suite.ctrl)
	suite.respoolLoader = aurorabridgemocks.NewMockRespoolLoader(suite.ctrl)
	suite.random = commonmocks.NewMockRandom(suite.ctrl)
	suite.jobIdCache = cachemocks.NewMockJobIDCache(suite.ctrl)

	suite.random.EXPECT().
		RandomUUID().
		Return(_randomUUID).
		AnyTimes()

	suite.config = ServiceHandlerConfig{
		GetJobUpdateWorkers:           25,
		GetTasksWithoutConfigsWorkers: 25,
		StopPodWorkers:                25,
		PodRunsDepth:                  2,
		ThermosExecutor: config.ThermosExecutorConfig{
			Path: "/usr/share/aurora/bin/thermos_executor.pex",
		},
		EnableInPlace: true,
	}
	suite.config.normalize()
	handler, err := NewServiceHandler(
		suite.config,
		tally.NoopScope,
		suite.jobClient,
		suite.jobmgrClient,
		suite.podClient,
		suite.respoolLoader,
		suite.random,
		suite.jobIdCache,
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

// TestGetJobSummary tests for success scenario for GetJobSummary
func (suite *ServiceHandlerTestSuite) TestGetJobSummary() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	labels := fixture.DefaultPelotonJobLabels(jobKey)
	instanceCount := uint32(1)
	jobs := 500

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for _, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		suite.jobClient.EXPECT().
			GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	}

	resp, err := suite.handler.GetJobSummary(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetJobSummaryResult().GetSummaries(), jobs)
}

// TestGetJobSummarySkipNotFoundJobs tests GetJobSummary endpoint when some
// jobs have NotFound error returned by Peloton GetJob API, GetJobs should not
// return an error, but instead exclude those jobs from the result.
func (suite *ServiceHandlerTestSuite) TestGetJobSummarySkipNotFoundJobs() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	labels := fixture.DefaultPelotonJobLabels(jobKey)
	instanceCount := uint32(1)
	jobs := 500
	jobsNotFound := map[int]struct{}{
		250: {}, 270: {}, 300: {},
	}

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for i, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		if _, ok := jobsNotFound[i]; !ok {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: false,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.NotFoundErrorf("job id not found"))
		}
	}

	resp, err := suite.handler.GetJobSummary(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetJobSummaryResult().GetSummaries(), jobs-len(jobsNotFound))
}

// TestGetJobSummaryFailure tests GetJobSummary endpoint when some jobs have
// errors returned by Peloton GetJob API, GetJobSummary should error out and
// not returning any results.
func (suite *ServiceHandlerTestSuite) TestGetJobSummaryFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	labels := fixture.DefaultPelotonJobLabels(jobKey)
	instanceCount := uint32(1)
	jobs := 500
	jobsError := map[int]struct{}{
		250: {}, 270: {}, 300: {},
	}

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for i, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		if _, ok := jobsError[i]; !ok {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
				}, nil).
				MaxTimes(1)
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: false,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.InvalidArgumentErrorf("job error")).
				MaxTimes(1)
		}
	}

	resp, err := suite.handler.GetJobSummary(suite.ctx, &role)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Tests for failure scenario for get config summary
func (suite *ServiceHandlerTestSuite) TestGetConfigSummaryFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	instanceCount := uint32(2)

	suite.expectGetJobIDFromJobName(jobKey, jobID)

	suite.podClient.EXPECT().
		GetPod(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("unable to query pods")).
		AnyTimes()

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	jobID := fixture.PelotonJobID()
	jobKey := fixture.AuroraJobKey()
	entityVersion := fixture.PelotonEntityVersion()
	podName := fmt.Sprintf("%s-%d", jobID.GetValue(), 0)
	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.podClient.EXPECT().GetPod(gomock.Any(), &podsvc.GetPodRequest{
		PodName:    &peloton.PodName{Value: podName},
		StatusOnly: false,
		Limit:      1,
	}).Return(&podsvc.GetPodResponse{
		Current: &pod.PodInfo{
			Spec: &pod.PodSpec{
				PodName:    &peloton.PodName{Value: podName},
				Labels:     mdLabel,
				Containers: []*pod.ContainerSpec{{}},
			},
			Status: &pod.PodStatus{
				PodId:   &peloton.PodID{Value: podName + "-" + strconv.Itoa(1)},
				Host:    "peloton-host-0",
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion,
			},
		},
	}, nil)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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

// TestGetJobs tests for success scenario for GetJobs.
func (suite *ServiceHandlerTestSuite) TestGetJobs() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	instanceCount := uint32(1)
	jobs := 500

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())
	jkLabel := label.NewAuroraJobKey(jobKey)

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for _, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		suite.jobClient.EXPECT().
			GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	}

	resp, err := suite.handler.GetJobs(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetGetJobsResult().GetConfigs(), jobs)
}

// TestGetJobsSkipNotFoundJobs tests GetJobs endpoint when some jobs have
// NotFound error returned by Peloton GetJob API, GetJobs should not return an
// error, but instead exclude those jobs from the result.
func (suite *ServiceHandlerTestSuite) TestGetJobsSkipNotFoundJobs() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	instanceCount := uint32(1)
	jobs := 500
	jobsNotFound := map[int]struct{}{
		250: {}, 270: {}, 300: {},
	}

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())
	jkLabel := label.NewAuroraJobKey(jobKey)

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for i, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		if _, ok := jobsNotFound[i]; !ok {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: false,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.NotFoundErrorf("job id not found"))
		}
	}

	resp, err := suite.handler.GetJobs(suite.ctx, &role)
	suite.NoError(err)
	suite.Len(resp.GetResult().GetGetJobsResult().GetConfigs(), jobs-len(jobsNotFound))
}

// TestGetJobsFailure tests GetJobs endpoint when some jobs have errors
// returned by Peloton GetJob API, GetJobs should error out and not returning
// any results.
func (suite *ServiceHandlerTestSuite) TestGetJobsFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	jobKey := fixture.AuroraJobKey()
	instanceCount := uint32(1)
	jobs := 500
	jobsError := map[int]struct{}{
		250: {}, 270: {}, 300: {},
	}

	var jobIDs []*peloton.JobID
	for i := 0; i < jobs; i++ {
		jobIDs = append(jobIDs, fixture.PelotonJobID())
	}

	mdLabel := label.NewAuroraMetadataLabels(fixture.AuroraMetadata())
	jkLabel := label.NewAuroraJobKey(jobKey)

	ql := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", ""),
		common.BridgeJobLabel,
	)
	jobCache := suite.expectQueryJobsWithLabels(ql, jobIDs, jobKey)

	// Expect cache not populated
	suite.jobIdCache.EXPECT().GetJobIDs(role).Return(nil)
	suite.jobIdCache.EXPECT().PopulateFromJobCache(role, jobCache)

	for i, jobID := range jobIDs {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-0"}
		if _, ok := jobsError[i]; !ok {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
				}, nil).
				MaxTimes(1)
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: false,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.InvalidArgumentErrorf("job error")).
				MaxTimes(1)
		}
	}

	resp, err := suite.handler.GetJobs(suite.ctx, &role)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Tests get job update diff
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDiff() {
	defer goleak.VerifyNoLeaks(suite.T())

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

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.expectGetJobVersion(jobID, entityVersion)
	suite.jobClient.EXPECT().
		GetReplaceJobDiff(
			gomock.Any(),
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
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	jobUpdateRequest := fixture.AuroraJobUpdateRequest()
	jobID := fixture.PelotonJobID()
	jobKey := jobUpdateRequest.GetTaskConfig().GetJob()
	entityVersion := fixture.PelotonEntityVersion()

	labels := []*api.Metadata{
		{
			Key:   ptr.String(common.AuroraGpuResourceKey),
			Value: ptr.String("2"),
		},
	}
	jobUpdateRequest.TaskConfig.Metadata = labels

	jobSpec, _ := atop.NewJobSpecFromJobUpdateRequest(
		jobUpdateRequest,
		respoolID,
		suite.config.ThermosExecutor,
	)

	suite.respoolLoader.EXPECT().Load(gomock.Any(), true).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(jobKey, jobID)
	suite.expectGetJobVersion(jobID, entityVersion)

	suite.jobClient.EXPECT().
		GetReplaceJobDiff(
			gomock.Any(),
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
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	k := fixture.AuroraJobKey()

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	resp, err := suite.handler.GetTierConfigs(suite.ctx)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Ensures StartJobUpdate creates jobs which don't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobSuccess() {
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	name := atop.NewJobName(k)

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.jobClient.EXPECT().
		CreateJob(gomock.Any(), gomock.Any()).
		Return(&statelesssvc.CreateJobResponse{}, nil)

	suite.jobIdCache.EXPECT().Invalidate(k.GetRole())

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	result := resp.GetResult().GetStartJobUpdateResult()
	suite.Equal(k, result.GetKey().GetJob())
}

// Ensures StartJobUpdate returns an INVALID_REQUEST error if there is a conflict
// when trying to create a job which doesn't exist.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_NewJobConflict() {
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	labels := []*api.Metadata{
		{
			Key:   ptr.String(common.AuroraGpuResourceKey),
			Value: ptr.String("2"),
		},
	}
	req.TaskConfig.Metadata = labels
	name := atop.NewJobName(req.GetTaskConfig().GetJob())

	suite.respoolLoader.EXPECT().Load(gomock.Any(), true).Return(respoolID, nil)

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
			JobName: name,
		}).
		Return(nil, yarpcerrors.NotFoundErrorf(""))

	suite.jobClient.EXPECT().
		CreateJob(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.AlreadyExistsErrorf(""))

	suite.jobIdCache.EXPECT().Invalidate(req.GetTaskConfig().GetJob().GetRole())

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures StartJobUpdate replaces jobs which already exist with no pulse.
func (suite *ServiceHandlerTestSuite) TestStartJobUpdate_ReplaceJobNoPulseSuccess() {
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	curv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.expectListPods(id, []*pod.PodSummary{})

	suite.jobClient.EXPECT().
		ReplaceJob(
			gomock.Any(),
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
	defer goleak.VerifyNoLeaks(suite.T())

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

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.expectListPods(id, []*pod.PodSummary{})

	suite.jobClient.EXPECT().
		ReplaceJob(
			gomock.Any(),
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
	defer goleak.VerifyNoLeaks(suite.T())

	respoolID := fixture.PelotonResourcePoolID()
	req := fixture.AuroraJobUpdateRequest()
	k := req.GetTaskConfig().GetJob()
	curv := fixture.PelotonEntityVersion()
	id := fixture.PelotonJobID()

	suite.respoolLoader.EXPECT().Load(gomock.Any(), false).Return(respoolID, nil)

	suite.expectGetJobIDFromJobName(k, id)

	suite.expectGetJobVersion(id, curv)

	suite.expectListPods(id, []*pod.PodSummary{})

	suite.jobClient.EXPECT().
		ReplaceJob(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.AbortedErrorf(""))

	resp, err := suite.handler.StartJobUpdate(suite.ctx, req, ptr.String("some message"))
	suite.NoError(err)
	suite.Equal(api.ResponseCodeInvalidRequest, resp.GetResponseCode())
}

// Ensures PauseJobUpdate successfully maps to PauseJobWorkflow.
func (suite *ServiceHandlerTestSuite) TestPauseJobUpdate_Success() {
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

	suite.jobClient.EXPECT().
		PauseJobWorkflow(gomock.Any(), &statelesssvc.PauseJobWorkflowRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

	suite.jobClient.EXPECT().
		ResumeJobWorkflow(gomock.Any(), &statelesssvc.ResumeJobWorkflowRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.expectGetJobAndWorkflow(id, k.GetID(), v)

	suite.jobClient.EXPECT().
		AbortJobWorkflow(gomock.Any(), &statelesssvc.AbortJobWorkflowRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		ResumeJobWorkflow(gomock.Any(), &statelesssvc.ResumeJobWorkflowRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()
	v := fixture.PelotonEntityVersion()

	d := &opaquedata.Data{UpdateID: k.GetID()}
	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
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
) (jobCache []*jobmgrsvc.QueryJobCacheResponse_JobCache) {
	for _, jobID := range jobIDs {
		jobCache = append(jobCache, &jobmgrsvc.QueryJobCacheResponse_JobCache{
			JobId: jobID,
			Name:  atop.NewJobName(jobKey),
		})
	}

	suite.jobmgrClient.EXPECT().
		QueryJobCache(
			gomock.Any(),
			&jobmgrsvc.QueryJobCacheRequest{
				Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
					Labels: labels,
				},
			}).
		Return(&jobmgrsvc.QueryJobCacheResponse{
			Result: jobCache,
		}, nil)

	return
}

func (suite *ServiceHandlerTestSuite) expectGetJobVersion(id *peloton.JobID, v *peloton.EntityVersion) {
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

	jobKey := &api.JobKey{
		Role:        ptr.String("role1"),
		Environment: ptr.String("env1"),
		Name:        ptr.String("name1"),
	}
	query := &api.TaskQuery{JobKeys: []*api.JobKey{jobKey}}

	// when GetJobIDFromJobName returns error
	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(),
			&statelesssvc.GetJobIDFromJobNameRequest{
				JobName: atop.NewJobName(jobKey),
			}).
		Return(nil, errors.New("failed to get job identifiers from job name"))

	jobIDs, err := suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.Error(err)
	suite.Nil(jobIDs)

	// when GetJobIDFromJobName returns not found error
	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(),
			&statelesssvc.GetJobIDFromJobNameRequest{
				JobName: atop.NewJobName(jobKey),
			}).
		Return(nil, yarpcerrors.NotFoundErrorf("job id for job name not found"))

	jobIDs, err = suite.handler.getJobIDsFromTaskQuery(suite.ctx, query)
	suite.NoError(err)
	suite.Empty(jobIDs)
}

func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_FullJobKey() {
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

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

// TestGetJobIDsFromTaskQuery_PartialJobKeyError checks getJobIDsFromTaskQuery
// returns error when the query fails and input query only contains partial
// job key parameters - role, environment, and/or job_name.
func (suite *ServiceHandlerTestSuite) TestGetJobIDsFromTaskQuery_PartialJobKeyError() {
	defer goleak.VerifyNoLeaks(suite.T())

	role := "role1"
	name := "name1"

	labels := append(
		label.BuildPartialAuroraJobKeyLabels(role, "", name),
		common.BridgeJobLabel,
	)

	suite.jobmgrClient.EXPECT().
		QueryJobCache(
			gomock.Any(),
			&jobmgrsvc.QueryJobCacheRequest{
				Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
					Labels: labels,
				},
			}).
		Return(nil, errors.New("failed to query job cache"))

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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			SummaryOnly: true,
			JobId:       jobID,
		}).
		Return(&statelesssvc.GetJobResponse{
			Summary: &stateless.JobSummary{
				JobId:         jobID,
				Name:          atop.NewJobName(jobKey),
				InstanceCount: instanceCount,
			},
		}, nil)
}

// TestGetTasksWithoutConfigs_ParallelismSuccess tests parallelism for
// GetTasksWithoutConfig success scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_ParallelismSuccess() {
	defer goleak.VerifyNoLeaks(suite.T())

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
			GetPod(gomock.Any(), &podsvc.GetPodRequest{
				PodName:    podName,
				StatusOnly: false,
				Limit:      1,
			}).Return(&podsvc.GetPodResponse{
			Current: &pod.PodInfo{
				Spec: &pod.PodSpec{
					PodName:    podName,
					Labels:     labels,
					Containers: []*pod.ContainerSpec{{}},
				},
				Status: &pod.PodStatus{
					PodId:   podID,
					Host:    "peloton-host-0",
					State:   pod.PodState_POD_STATE_RUNNING,
					Version: entityVersion,
				},
			},
		}, nil)

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
						ActualState: pod.PodState_POD_STATE_RUNNING.String(),
						Hostname:    "peloton-host-0",
					},
				},
			}, nil)
	}

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 1000)
}

// TestGetTasksWithoutConfigs_ParallelismFailure tests parallelism for
// GetTasksWithoutConfig failure scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_GetPodParallelismFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

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
				GetPod(gomock.Any(), &podsvc.GetPodRequest{
					PodName:    podName,
					StatusOnly: false,
					Limit:      1,
				}).
				Return(&podsvc.GetPodResponse{},
					errors.New("failed to get pod events")).
				MaxTimes(1)
			continue
		}

		suite.podClient.EXPECT().
			GetPod(gomock.Any(), &podsvc.GetPodRequest{
				PodName:    podName,
				StatusOnly: false,
				Limit:      1,
			}).Return(&podsvc.GetPodResponse{
			Current: &pod.PodInfo{
				Spec: &pod.PodSpec{
					PodName:    podName,
					Labels:     labels,
					Containers: []*pod.ContainerSpec{{}},
				},
				Status: &pod.PodStatus{
					PodId:   podID,
					Host:    "peloton-host-0",
					State:   pod.PodState_POD_STATE_RUNNING,
					Version: entityVersion,
				},
			},
		}, nil).MaxTimes(1)
	}

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 0)
	suite.NotEmpty(resp.GetDetails())
}

// TestGetTasksWithoutConfigs_ParallelismFailure tests parallelism for
// GetTasksWithoutConfig failure scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_ParallelismFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

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
			GetPod(gomock.Any(), &podsvc.GetPodRequest{
				PodName:    podName,
				StatusOnly: false,
				Limit:      1,
			}).Return(&podsvc.GetPodResponse{
			Current: &pod.PodInfo{
				Spec: &pod.PodSpec{
					PodName:    podName,
					Labels:     labels,
					Containers: []*pod.ContainerSpec{{}},
				},
				Status: &pod.PodStatus{
					PodId:   podID,
					Host:    "peloton-host-0",
					State:   pod.PodState_POD_STATE_RUNNING,
					Version: entityVersion,
				},
			},
		}, nil)

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
						ActualState: pod.PodState_POD_STATE_RUNNING.String(),
						Hostname:    "peloton-host-0",
					},
				},
			}, nil).
			MaxTimes(1)
	}

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
	defer goleak.VerifyNoLeaks(suite.T())

	query := fixture.AuroraTaskQuery()
	jobKey := query.GetJobKeys()[0]
	query.Statuses = map[api.ScheduleStatus]struct{}{
		api.ScheduleStatusRunning: {},
	}
	jobID := fixture.PelotonJobID()
	entityVersion := fixture.PelotonEntityVersion()
	podVersion := &peloton.EntityVersion{Value: "1-0-0"}
	labels := fixture.DefaultPelotonJobLabels(jobKey)

	// Sets up jobKey to jobID mapping, and returns a basic JobInfo
	// for the specific jobID
	suite.expectGetJobSummary(jobKey, jobID, 2)

	// Only expect to be called once when querying for previous pod run
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   jobID,
			Version: podVersion,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				JobId: jobID,
				Spec: &stateless.JobSpec{
					Name: atop.NewJobName(jobKey),
					DefaultSpec: &pod.PodSpec{
						Containers: []*pod.ContainerSpec{{}},
					},
				},
			},
		}, nil)

	// Sets up GetPodEvents queries for all the pods (pod 0 and 1) from
	// all the runs (run 1, 2, 3) - PodEvent will be used to construct
	// aurora ScheduledTask struct
	expectPods := []struct {
		instanceID     string
		runID          string
		prevRunID      string
		currentRun     bool
		taskState      pod.PodState
		expectInResult bool
	}{
		{
			// pod 0 latest run
			// expect to be included in output
			instanceID:     "0",
			runID:          "3",
			prevRunID:      "2",
			currentRun:     true,
			taskState:      pod.PodState_POD_STATE_RUNNING,
			expectInResult: true,
		},
		{
			// pod 0 previous run
			// expect to be included in output since PodRunsDepth = 2
			instanceID:     "0",
			runID:          "2",
			prevRunID:      "1",
			currentRun:     false,
			taskState:      pod.PodState_POD_STATE_RUNNING,
			expectInResult: true,
		},
		{
			// pod 0 oldest run
			// do not expect to be included in output since PodRunsDepth = 2
			instanceID:     "0",
			runID:          "1",
			prevRunID:      "",
			currentRun:     false,
			taskState:      pod.PodState_POD_STATE_RUNNING,
			expectInResult: false,
		},
		{
			// pod 1 latest run
			// expect to be included in output
			instanceID:     "1",
			runID:          "3",
			prevRunID:      "2",
			currentRun:     true,
			taskState:      pod.PodState_POD_STATE_RUNNING,
			expectInResult: true,
		},
		{
			// pod 1 previous run
			// do not expect to be included in output since task state is FAILED
			instanceID:     "1",
			runID:          "2",
			prevRunID:      "1",
			currentRun:     false,
			taskState:      pod.PodState_POD_STATE_FAILED,
			expectInResult: false,
		},
		{
			// pod 1 oldest run
			// do not expect to be included in output since PodRunsDepth = 2
			instanceID:     "1",
			runID:          "1",
			prevRunID:      "",
			currentRun:     false,
			taskState:      pod.PodState_POD_STATE_RUNNING,
			expectInResult: false,
		},
	}

	expectedTaskIds := make(map[string]struct{})
	for _, e := range expectPods {
		podName := &peloton.PodName{Value: jobID.GetValue() + "-" + e.instanceID}
		podID := &peloton.PodID{Value: podName.GetValue() + "-" + e.runID}

		suite.podClient.EXPECT().
			GetPod(gomock.Any(), &podsvc.GetPodRequest{
				PodName:    podName,
				StatusOnly: false,
				Limit:      1,
			}).Return(&podsvc.GetPodResponse{
			Current: &pod.PodInfo{
				Spec: &pod.PodSpec{
					PodName:    podName,
					Labels:     labels,
					Containers: []*pod.ContainerSpec{{}},
				},
				Status: &pod.PodStatus{
					PodId:   podID,
					Host:    "peloton-host-0",
					State:   pod.PodState_POD_STATE_RUNNING,
					Version: entityVersion,
				},
			},
		}, nil).AnyTimes()

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
				Version:     podVersion,
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

// TestGetTasksWithoutConfigs_MultiJobsSuccess tests parallel fetching of
// multiple jobs on GetTasksWithoutConfig success scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_MultiJobsSuccess() {
	defer goleak.VerifyNoLeaks(suite.T())

	query := &api.TaskQuery{Role: ptr.String("role")}
	jobKeys := []*api.JobKey{
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name1")},
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name2")},
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name3")},
	}
	jobIDs := []*peloton.JobID{
		fixture.PelotonJobID(),
		fixture.PelotonJobID(),
		fixture.PelotonJobID(),
	}
	var labels [][]*peloton.Label
	for _, jobKey := range jobKeys {
		labels = append(labels, fixture.DefaultPelotonJobLabels(jobKey))
	}

	var jobCache []*jobmgrsvc.QueryJobCacheResponse_JobCache
	for i := range jobIDs {
		jobCache = append(jobCache, &jobmgrsvc.QueryJobCacheResponse_JobCache{
			JobId: jobIDs[i],
			Name:  atop.NewJobName(jobKeys[i]),
		})
	}
	entityVersion := fixture.PelotonEntityVersion()

	suite.jobmgrClient.EXPECT().
		QueryJobCache(gomock.Any(), &jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: []*peloton.Label{
					label.NewAuroraJobKeyRole("role"),
					common.BridgeJobLabel,
				},
			},
		}).
		Return(&jobmgrsvc.QueryJobCacheResponse{
			Result: jobCache,
		}, nil)

	for i, jobID := range jobIDs {
		if i == 1 {
			// Expect handler to skip jobs with "not found error"
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: true,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.NotFoundErrorf("job not found"))
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: true,
					JobId:       jobID,
				}).
				Return(&statelesssvc.GetJobResponse{
					Summary: &stateless.JobSummary{
						JobId:         jobID,
						Name:          atop.NewJobName(jobKeys[i]),
						InstanceCount: 1000,
					},
				}, nil)
		}
	}

	for i, jobID := range jobIDs {
		if i == 1 {
			continue
		}

		label := labels[i]
		for j := 0; j < 1000; j++ {
			podName := &peloton.PodName{
				Value: util.CreatePelotonTaskID(jobID.GetValue(), uint32(j)),
			}
			podID := &peloton.PodID{Value: podName.GetValue() + "-1"}

			suite.podClient.EXPECT().
				GetPod(gomock.Any(), &podsvc.GetPodRequest{
					PodName:    podName,
					StatusOnly: false,
					Limit:      1,
				}).Return(&podsvc.GetPodResponse{
				Current: &pod.PodInfo{
					Spec: &pod.PodSpec{
						PodName:    podName,
						Labels:     label,
						Containers: []*pod.ContainerSpec{{}},
					},
					Status: &pod.PodStatus{
						PodId:   podID,
						Host:    "peloton-host-0",
						State:   pod.PodState_POD_STATE_RUNNING,
						Version: entityVersion,
					},
				},
			}, nil)

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
							ActualState: pod.PodState_POD_STATE_RUNNING.String(),
							Hostname:    "peloton-host-0",
						},
					},
				}, nil)
		}
	}

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 2000)
}

// TestGetTasksWithoutConfigs_MultiJobsFailure tests parallel fetching of
// multiple jobs on GetTasksWithoutConfig failure scenario
func (suite *ServiceHandlerTestSuite) TestGetTasksWithoutConfigs_MultiJobsFailure() {
	defer goleak.VerifyNoLeaks(suite.T())

	query := &api.TaskQuery{Role: ptr.String("role")}
	jobKeys := []*api.JobKey{
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name1")},
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name2")},
		{Role: ptr.String("role"), Environment: ptr.String("env"), Name: ptr.String("name3")},
	}
	jobIDs := []*peloton.JobID{
		fixture.PelotonJobID(),
		fixture.PelotonJobID(),
		fixture.PelotonJobID(),
	}
	var labels [][]*peloton.Label
	for _, jobKey := range jobKeys {
		labels = append(labels, fixture.DefaultPelotonJobLabels(jobKey))
	}
	var jobCache []*jobmgrsvc.QueryJobCacheResponse_JobCache
	for i := range jobIDs {
		jobCache = append(jobCache, &jobmgrsvc.QueryJobCacheResponse_JobCache{
			JobId: jobIDs[i],
			Name:  atop.NewJobName(jobKeys[i]),
		})
	}
	entityVersion := fixture.PelotonEntityVersion()

	suite.jobmgrClient.EXPECT().
		QueryJobCache(gomock.Any(), &jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: []*peloton.Label{
					label.NewAuroraJobKeyRole("role"),
					common.BridgeJobLabel,
				},
			},
		}).
		Return(&jobmgrsvc.QueryJobCacheResponse{
			Result: jobCache,
		}, nil)

	for i, jobID := range jobIDs {
		if i == 1 {
			// Expect handler to skip jobs with "not found error"
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: true,
					JobId:       jobID,
				}).
				Return(nil, yarpcerrors.NotFoundErrorf("job not found"))
		} else {
			suite.jobClient.EXPECT().
				GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
					SummaryOnly: true,
					JobId:       jobID,
				}).
				Return(&statelesssvc.GetJobResponse{
					Summary: &stateless.JobSummary{
						JobId:         jobID,
						Name:          atop.NewJobName(jobKeys[i]),
						InstanceCount: 1000,
					},
				}, nil)
		}
	}

	for i, jobID := range jobIDs {
		if i == 1 {
			continue
		}

		label := labels[i]
		for j := 0; j < 1000; j++ {
			podName := &peloton.PodName{
				Value: util.CreatePelotonTaskID(jobID.GetValue(), uint32(j)),
			}
			podID := &peloton.PodID{Value: podName.GetValue() + "-1"}

			suite.podClient.EXPECT().
				GetPod(gomock.Any(), &podsvc.GetPodRequest{
					PodName:    podName,
					StatusOnly: false,
					Limit:      1,
				}).Return(&podsvc.GetPodResponse{
				Current: &pod.PodInfo{
					Spec: &pod.PodSpec{
						PodName:    podName,
						Labels:     label,
						Containers: []*pod.ContainerSpec{{}},
					},
					Status: &pod.PodStatus{
						PodId:   podID,
						Host:    "peloton-host-0",
						State:   pod.PodState_POD_STATE_RUNNING,
						Version: entityVersion,
					},
				},
			}, nil)

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
							ActualState: pod.PodState_POD_STATE_RUNNING.String(),
							Hostname:    "peloton-host-0",
						},
					},
				}, nil).
				MaxTimes(1)
		}
	}

	resp, err := suite.handler.GetTasksWithoutConfigs(suite.ctx, query)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
	suite.Len(resp.GetResult().GetScheduleStatusResult().GetTasks(), 0)
	suite.NotEmpty(resp.GetDetails())
}

// Ensures that KillTasks maps to StopPods correctly.
func (suite *ServiceHandlerTestSuite) TestKillTasks_Success() {
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	instances := fixture.AuroraInstanceSet(50, 100)

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()
	instances := fixture.AuroraInstanceSet(50, 100)

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

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
	defer goleak.VerifyNoLeaks(suite.T())

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
			GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
			StopJob(gomock.Any(), &statelesssvc.StopJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	prevVersion := fixture.PelotonEntityVersion(1)
	curVersion := fixture.PelotonEntityVersion(2)

	updateSpec := &stateless.UpdateSpec{
		BatchSize:         1,
		StartPaused:       true,
		RollbackOnFailure: true,
		InPlace:           true,
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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: prevVersion,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: prevSpec,
			},
		}, nil)

	suite.jobClient.EXPECT().
		ReplaceJob(gomock.Any(), &statelesssvc.ReplaceJobRequest{
			JobId:   id,
			Version: curVersion,
			Spec:    prevSpec,
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize:         1,
				StartPaused:       false,
				RollbackOnFailure: false,
				InPlace:           true,
			},
			OpaqueData: newOD,
		}).
		Return(&statelesssvc.ReplaceJobResponse{}, nil)

	resp, err := suite.handler.RollbackJobUpdate(suite.ctx, k, nil)
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())
}

// Ensures that RollbackJobUpdate calls ReplaceJob using the current job spec
// but instance count set to 0 when RollbackJobUpdate is called on the very
// first deployment
func (suite *ServiceHandlerTestSuite) TestRollbackJobUpdate_FirstDeployment_Success() {
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	prevVersion := fixture.PelotonEntityVersion(0, 1, 1)
	curVersion := fixture.PelotonEntityVersion(1, 1, 1)

	updateSpec := &stateless.UpdateSpec{
		BatchSize:         1,
		StartPaused:       true,
		RollbackOnFailure: true,
		InPlace:           true,
	}

	spec := &stateless.JobSpec{
		Name:          atop.NewJobName(k.GetJob()),
		Description:   "the current job spec",
		InstanceCount: 5,
	}
	prevSpec := &stateless.JobSpec{
		Name:          atop.NewJobName(k.GetJob()),
		Description:   "the current job spec",
		InstanceCount: 0,
	}

	d := &opaquedata.Data{UpdateID: k.GetID()}

	curOD, err := d.Serialize()
	suite.NoError(err)

	d.AppendUpdateAction(opaquedata.Rollback)

	newOD, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId: id,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: spec,
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
		ReplaceJob(gomock.Any(), &statelesssvc.ReplaceJobRequest{
			JobId:   id,
			Version: curVersion,
			Spec:    prevSpec,
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize:         1,
				StartPaused:       false,
				RollbackOnFailure: false,
				InPlace:           true,
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	d := &opaquedata.Data{UpdateID: "some other update id"}

	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobUpdateKey()
	id := fixture.PelotonJobID()

	d := &opaquedata.Data{UpdateID: k.GetID()}
	d.AppendUpdateAction(opaquedata.Rollback)

	od, err := d.Serialize()
	suite.NoError(err)

	suite.expectGetJobIDFromJobName(k.GetJob(), id)

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
		}).
		Return(nil, errors.New("some error"))

	resp, err := suite.handler.GetJobUpdateSummaries(
		suite.ctx, &api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeError, resp.GetResponseCode())
}

// Very simple test checking GetJobUpdateDetails error.
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_Error() {
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()

	suite.jobClient.EXPECT().
		GetJobIDFromJobName(gomock.Any(), &statelesssvc.GetJobIDFromJobNameRequest{
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
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
	defer goleak.VerifyNoLeaks(suite.T())

	role := "some-role"

	labels := []*peloton.Label{
		label.NewAuroraJobKeyRole(role),
		common.BridgeJobLabel,
	}

	keys := []*api.JobKey{
		{Role: &role, Environment: ptr.String("env-1"), Name: ptr.String("job-1")},
		{Role: &role, Environment: ptr.String("env-2"), Name: ptr.String("job-2")},
	}

	jobCache := []*jobmgrsvc.QueryJobCacheResponse_JobCache{
		{JobId: fixture.PelotonJobID(), Name: atop.NewJobName(keys[0])},
		{JobId: fixture.PelotonJobID(), Name: atop.NewJobName(keys[1])},
	}

	suite.jobmgrClient.EXPECT().
		QueryJobCache(gomock.Any(), &jobmgrsvc.QueryJobCacheRequest{
			Spec: &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
				Labels: labels,
			},
		}).
		Return(&jobmgrsvc.QueryJobCacheResponse{
			Result: jobCache,
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
			JobId:               jobCache[0].JobId,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{wf0, wf1, wf2},
		}, nil)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               jobCache[1].JobId,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
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
	defer goleak.VerifyNoLeaks(suite.T())

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
			JobId:               id,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ABORTED,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					},
					OpaqueData: od1,
				},
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
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
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				}, {
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
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

// TestGetJobUpdateDetails_FilterNonUpdateWorkflow
func (suite *ServiceHandlerTestSuite) TestGetJobUpdateDetails_FilterNonUpdateWorkflow() {
	defer goleak.VerifyNoLeaks(suite.T())

	k := fixture.AuroraJobKey()
	id := fixture.PelotonJobID()

	suite.expectGetJobIDFromJobName(k, id)

	suite.jobClient.EXPECT().
		ListJobWorkflows(gomock.Any(), &statelesssvc.ListJobWorkflowsRequest{
			JobId:               id,
			InstanceEvents:      true,
			UpdatesLimit:        suite.config.UpdatesLimit,
			InstanceEventsLimit: suite.config.InstanceEventsLimit,
		}).
		Return(&statelesssvc.ListJobWorkflowsResponse{
			WorkflowInfos: []*stateless.WorkflowInfo{
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				},
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_RESTART,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				},
				{
					Status: &stateless.WorkflowStatus{
						State: stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
						Type:  stateless.WorkflowType_WORKFLOW_TYPE_UPDATE,
					},
					OpaqueData: fixture.PelotonOpaqueData(),
				},
			},
		}, nil)

	resp, err := suite.handler.GetJobUpdateDetails(
		suite.ctx,
		nil,
		&api.JobUpdateQuery{JobKey: k})
	suite.NoError(err)
	suite.Equal(api.ResponseCodeOk, resp.GetResponseCode())

	// The RESTART workflow should be filtered out
	result := resp.GetResult().GetGetJobUpdateDetailsResult().GetDetailsList()
	suite.Len(result, 2)
}

// expectListPods sets up expect for ListPods API based on input JobID
// and a list of PodSummary.
func (suite *ServiceHandlerTestSuite) expectListPods(
	jobID *peloton.JobID,
	pods []*pod.PodSummary,
) {
	suite.jobClient.EXPECT().
		ListPods(gomock.Any(), &statelesssvc.ListPodsRequest{
			JobId: jobID,
		}).
		Return(suite.listPodsStream, nil)

	for _, p := range pods {
		suite.listPodsStream.EXPECT().
			Recv().
			Return(&statelesssvc.ListPodsResponse{
				Pods: []*pod.PodSummary{p},
			}, nil)
	}

	suite.listPodsStream.EXPECT().
		Recv().
		Return(nil, io.EOF)
}

// TestListPods tests listPods util function to return result correctly.
func (suite *ServiceHandlerTestSuite) TestListPods() {
	defer goleak.VerifyNoLeaks(suite.T())

	id := fixture.PelotonJobID()
	pods := []*pod.PodSummary{
		{
			PodName: &peloton.PodName{Value: "pod-0"},
		},
		{
			PodName: &peloton.PodName{Value: "pod-1"},
		},
		{
			PodName: &peloton.PodName{Value: "pod-2"},
		},
	}

	suite.expectListPods(id, pods)

	ps, err := suite.handler.listPods(suite.ctx, id)
	suite.NoError(err)
	suite.Equal(pods, ps)
}

// TestListPods tests getCurrentPods() util function using job spec with
// default spec only.
func (suite *ServiceHandlerTestSuite) TestGetCurrentPods_WithDefaultSpecOnly() {
	defer goleak.VerifyNoLeaks(suite.T())

	id := fixture.PelotonJobID()
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}
	podSpec1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v1",
				Value: "v1v",
			},
		},
	}
	podSpec2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v2",
				Value: "v2v",
			},
		},
	}
	podSpec3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v3",
				Value: "v3v",
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec1,
				},
			}}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec2,
				},
			}}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec3,
				},
			}}, nil)

	podStates, err := suite.handler.getCurrentPods(suite.ctx, id)
	suite.NoError(err)
	suite.Equal(map[uint32]*podStateSpec{
		0: {
			state:   pod.PodState_POD_STATE_RUNNING,
			podSpec: podSpec3,
		},
		1: {
			state:   pod.PodState_POD_STATE_FAILED,
			podSpec: podSpec1,
		},
		2: {
			state:   pod.PodState_POD_STATE_PENDING,
			podSpec: podSpec2,
		},
	}, podStates)
}

// TestListPods tests getCurrentPods() util function using job spec with
// both default spec and instance spec.
func (suite *ServiceHandlerTestSuite) TestGetCurrentPods_WithInstanceSpec() {
	defer goleak.VerifyNoLeaks(suite.T())

	id := fixture.PelotonJobID()
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}
	podSpec1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v1",
				Value: "v1v",
			},
		},
	}
	podSpec2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v2",
				Value: "v2v",
			},
		},
	}
	podSpec3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v3",
				Value: "v3v",
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec:  podSpec1,
					InstanceSpec: map[uint32]*pod.PodSpec{1: podSpec2},
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec:  podSpec2,
					InstanceSpec: map[uint32]*pod.PodSpec{0: podSpec1},
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec:  podSpec3,
					InstanceSpec: map[uint32]*pod.PodSpec{1: podSpec3},
				},
			},
		}, nil)

	podStates, err := suite.handler.getCurrentPods(suite.ctx, id)
	suite.NoError(err)
	suite.Equal(map[uint32]*podStateSpec{
		0: {
			state:   pod.PodState_POD_STATE_RUNNING,
			podSpec: podSpec3,
		},
		1: {
			state:   pod.PodState_POD_STATE_FAILED,
			podSpec: podSpec2,
		},
		2: {
			state:   pod.PodState_POD_STATE_PENDING,
			podSpec: podSpec2,
		},
	}, podStates)
}

// TestListPods tests getCurrentPods() util function when failed to query
// job spec.
func (suite *ServiceHandlerTestSuite) TestGetCurrentPods_Fail() {
	defer goleak.VerifyNoLeaks(suite.T())

	id := fixture.PelotonJobID()
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(
			&statelesssvc.GetJobResponse{},
			errors.New("failed to get job"),
		).
		AnyTimes()
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(
			&statelesssvc.GetJobResponse{},
			errors.New("failed to get job"),
		).
		AnyTimes()
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(
			&statelesssvc.GetJobResponse{},
			errors.New("failed to get job"),
		).
		AnyTimes()

	_, err := suite.handler.getCurrentPods(suite.ctx, id)
	suite.Error(err)
}

// TestCreateJobSpecForUpdate_NoPinned tests createJobSpecForUpdate()
// util function update request with no pinned instances.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdate_NoPinned() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := fixture.AuroraJobUpdateRequest()
	req.Settings = &api.JobUpdateSettings{} // no pinned instances
	id := fixture.PelotonJobID()
	spec := &stateless.JobSpec{
		Name: atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: &pod.PodSpec{
			Labels: []*peloton.Label{
				{
					Key:   "label-key",
					Value: "label-value",
				},
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{})

	newSpec, err := suite.handler.createJobSpecForUpdate(suite.ctx, req, id, spec)
	suite.NoError(err)
	suite.Equal(spec, newSpec)
}

// TestCreateJobSpecForUpdate_WithPinned tests createJobSpecForUpdate()
// util function update request with pinned instances.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdate_WithPinned() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := fixture.AuroraJobUpdateRequest()
	req.Settings = &api.JobUpdateSettings{
		UpdateOnlyTheseInstances: []*api.Range{
			{First: ptr.Int32(0), Last: ptr.Int32(0)},
		},
	}
	req.InstanceCount = ptr.Int32(3)

	id := fixture.PelotonJobID()
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "label-key",
				Value: "label-value",
			},
		},
	}

	spec := &stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
	}
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}
	podSpec1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v1",
				Value: "v1v",
			},
		},
	}
	podSpec2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v2",
				Value: "v2v",
			},
		},
	}
	podSpec3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v3",
				Value: "v3v",
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec1,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec2,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec3,
				},
			},
		}, nil)

	newSpec, err := suite.handler.createJobSpecForUpdate(suite.ctx, req, id, spec)
	suite.NoError(err)
	suite.Equal(&stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			1: podSpec1,
			2: podSpec2,
		},
	}, newSpec)
}

// TestCreateJobSpecForUpdate_WithPinned_AddInstance tests
// createJobSpecForUpdate() util function update request with pinned instances
// and added instance.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdate_WithPinned_AddInstance() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := fixture.AuroraJobUpdateRequest()
	req.Settings = &api.JobUpdateSettings{
		UpdateOnlyTheseInstances: []*api.Range{
			{First: ptr.Int32(0), Last: ptr.Int32(0)},
		},
	}
	req.InstanceCount = ptr.Int32(4)

	id := fixture.PelotonJobID()
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "label-key",
				Value: "label-value",
			},
		},
	}

	spec := &stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
	}
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}
	podSpec1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v1",
				Value: "v1v",
			},
		},
	}
	podSpec2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v2",
				Value: "v2v",
			},
		},
	}
	podSpec3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v3",
				Value: "v3v",
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec1,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec2,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec3,
				},
			},
		}, nil)

	newSpec, err := suite.handler.createJobSpecForUpdate(suite.ctx, req, id, spec)
	suite.NoError(err)
	suite.Equal(&stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			1: podSpec1,
			2: podSpec2,
		},
	}, newSpec)
}

// TestCreateJobSpecForUpdate_WithPinned_AddInstance tests
// createJobSpecForUpdate() util function update request with pinned instances
// and removed instance.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdate_WithPinned_RemoveInstance() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := fixture.AuroraJobUpdateRequest()
	req.Settings = &api.JobUpdateSettings{
		UpdateOnlyTheseInstances: []*api.Range{
			{First: ptr.Int32(0), Last: ptr.Int32(0)},
		},
	}
	req.InstanceCount = ptr.Int32(2)

	id := fixture.PelotonJobID()
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "label-key",
				Value: "label-value",
			},
		},
	}

	spec := &stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
	}
	entityVersion1 := &peloton.EntityVersion{Value: "1-0-0"}
	entityVersion2 := &peloton.EntityVersion{Value: "2-0-0"}
	entityVersion3 := &peloton.EntityVersion{Value: "3-0-0"}
	podSpec1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v1",
				Value: "v1v",
			},
		},
	}
	podSpec2 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v2",
				Value: "v2v",
			},
		},
	}
	podSpec3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{
				Key:   "v3",
				Value: "v3v",
			},
		},
	}

	suite.expectListPods(id, []*pod.PodSummary{
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 0),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_RUNNING,
				Version: entityVersion3,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 1),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_FAILED,
				Version: entityVersion1,
			},
		},
		{
			PodName: &peloton.PodName{
				Value: util.CreatePelotonTaskID(id.GetValue(), 2),
			},
			Status: &pod.PodStatus{
				State:   pod.PodState_POD_STATE_PENDING,
				Version: entityVersion2,
			},
		},
	})

	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion1,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec1,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion2,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec2,
				},
			},
		}, nil)
	suite.jobClient.EXPECT().
		GetJob(gomock.Any(), &statelesssvc.GetJobRequest{
			JobId:   id,
			Version: entityVersion3,
		}).
		Return(&statelesssvc.GetJobResponse{
			JobInfo: &stateless.JobInfo{
				Spec: &stateless.JobSpec{
					DefaultSpec: podSpec3,
				},
			},
		}, nil)

	newSpec, err := suite.handler.createJobSpecForUpdate(suite.ctx, req, id, spec)
	suite.NoError(err)
	suite.Equal(&stateless.JobSpec{
		Name:        atop.NewJobName(req.GetTaskConfig().GetJob()),
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			1: podSpec1,
		},
	}, newSpec)
}

// TestCreateJobSpecForUpdateInternal_WipeOut tests
// createJobSpecForUpdateInternal returns job spec with default spec only
// if update covers all instances and all of them has spec change.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_WipeOut() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(3)
	jobSpec := &stateless.JobSpec{
		DefaultSpec: &pod.PodSpec{
			Labels: []*peloton.Label{
				{Key: "k2", Value: "v2"},
			},
		},
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{}
	updateInstances := map[uint32]struct{}{
		0: {}, 1: {}, 2: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {}, 1: {}, 2: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(jobSpec, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_WipeOut_AddInstance tests
// createJobSpecForUpdateInternal returns job spec with default spec only
// if update covers all instances, all of them has spec change, and has
// added instances.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_WipeOut_AddInstance() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(4)
	jobSpec := &stateless.JobSpec{
		DefaultSpec: &pod.PodSpec{
			Labels: []*peloton.Label{
				{Key: "k2", Value: "v2"},
			},
		},
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{}
	updateInstances := map[uint32]struct{}{
		0: {}, 1: {}, 2: {}, 3: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {}, 1: {}, 2: {}, 3: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(jobSpec, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_WipeOut_RemoveInstance tests
// createJobSpecForUpdateInternal returns job spec with default spec only
// if update covers all instances, all of them has spec change, and has
// removed instances.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_WipeOut_RemoveInstance() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(2)
	jobSpec := &stateless.JobSpec{
		DefaultSpec: &pod.PodSpec{
			Labels: []*peloton.Label{
				{Key: "k2", Value: "v2"},
			},
		},
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{}
	updateInstances := map[uint32]struct{}{
		0: {}, 1: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {}, 1: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(jobSpec, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_UpdateOnly tests
// createJobSpecForUpdateInternal returns job spec with instance specs
// with instances within updateInstances map are updated with new spec,
// and others are kept with original spec.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_UpdateOnly() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(3)
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: podSpec,
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{}
	updateInstances := map[uint32]struct{}{
		1: {},
	}
	specChangeInstances := map[uint32]struct{}{
		1: {}, 2: {}, 3: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(&stateless.JobSpec{
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			0: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
			2: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_BridgeUpdateLabel tests
// createJobSpecForUpdateInternal returns job spec which includes
// "bridge update label" when a force instance start is needed.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_BridgeUpdateLabel() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(4)
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: podSpec,
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_KILLED,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_KILLED,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
		3: {
			state: pod.PodState_POD_STATE_KILLED,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{
		1: {}, 2: {}, 3: {},
	}
	updateInstances := map[uint32]struct{}{
		1: {}, 2: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(&stateless.JobSpec{
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			0: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
			1: {
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: _randomUUID},
				},
			},
			2: {
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: _randomUUID},
				},
			},
			3: {
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
	}, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_SpecChangeSwipeLabel tests
// createJobSpecForUpdateInternal returns job spec which removes
// "bridge update label" when pod spec is changed.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_SpecChangeSwipeLabel() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(3)
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: podSpec,
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_KILLED,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_KILLED,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{
		0: {}, 1: {},
	}
	updateInstances := map[uint32]struct{}{
		1: {}, 2: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {}, 1: {}, 2: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(&stateless.JobSpec{
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			0: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
	}, newJobSpec)
}

// TestCreateJobSpecForUpdateInternal_KeepSpec tests
// createJobSpecForUpdateInternal returns job spec which keeps the original
// pod spec if the instance has no config change and is not terminated.
func (suite *ServiceHandlerTestSuite) TestCreateJobSpecForUpdateInternal_KeepSpec() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(3)
	podSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k2", Value: "v2"},
			{Key: "k1", Value: "v1"},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: podSpec,
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		1: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
		},
		2: {
			state: pod.PodState_POD_STATE_RUNNING,
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}
	terminalInstances := map[uint32]struct{}{}
	updateInstances := map[uint32]struct{}{
		1: {}, 2: {},
	}
	specChangeInstances := map[uint32]struct{}{
		0: {}, 2: {},
	}

	newJobSpec := suite.handler.createJobSpecForUpdateInternal(
		instances,
		jobSpec,
		podStates,
		terminalInstances,
		updateInstances,
		specChangeInstances,
	)
	suite.Equal(&stateless.JobSpec{
		DefaultSpec: podSpec,
		InstanceSpec: map[uint32]*pod.PodSpec{
			// instance not to be updated
			0: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
			// instance does not have config change, keep original
			1: {
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "1"},
				},
			},
			// instance 2 expected to use the new config
		},
	}, newJobSpec)
}

// TestGetInstanceSpecLabelOnly check getInstanceSpecLabelOnly extracts
// instance spec correctly.
func TestGetInstanceSpecLabelOnly(t *testing.T) {
	defer goleak.VerifyNoLeaks(t)

	testCases := []struct {
		name   string
		pod    *pod.PodSpec
		expect *pod.PodSpec
	}{
		{
			"test with pod spec with labels only",
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
			},
		},
		{
			"test with pod spec with labels and one boolean field",
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: true,
			},
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: true,
			},
		},
		{
			"test with pod spec with labels and multiple boolean fields",
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: false,
				Revocable:  true,
			},
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: false,
				Revocable:  true,
			},
		},
		{
			"test with pod spec with redundant fields",
			&pod.PodSpec{
				PodName: &peloton.PodName{Value: "pod-1"},
				Containers: []*pod.ContainerSpec{
					{
						Resource: &pod.ResourceSpec{CpuLimit: 100},
						Image:    "test-image",
					},
				},
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: false,
				Revocable:  true,
			},
			&pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
				},
				Controller: false,
				Revocable:  true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expect, getInstanceSpecLabelOnly(tc.pod))
		})
	}
}

// TestGetTerminalInstances tests getTerminalInstances util function.
func TestGetTerminalInstances(t *testing.T) {
	defer goleak.VerifyNoLeaks(t)

	req := &api.JobUpdateRequest{
		InstanceCount: ptr.Int32(3),
	}
	podStates := map[uint32]*podStateSpec{
		0: {state: pod.PodState_POD_STATE_KILLED},
		1: {state: pod.PodState_POD_STATE_FAILED},
		2: {state: pod.PodState_POD_STATE_RUNNING},
	}

	ti := getTerminalInstances(req, podStates)
	assert.Equal(t, map[uint32]struct{}{
		0: {},
		1: {},
	}, ti)
}

// TestGetUpdateInstances_AllInstances checks when UpdateOnlyTheseInstances
// is not set and new pod spec is different from all currently running pod
// specs, getUpdateInstances should return all the instance ids.
func (suite *ServiceHandlerTestSuite) TestGetUpdateInstances_AllInstances() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := &api.JobUpdateRequest{
		InstanceCount: ptr.Int32(3),
	}

	ui := getUpdateInstances(req)
	suite.Equal(map[uint32]struct{}{
		0: {},
		1: {},
		2: {},
	}, ui)
}

// TestGetUpdateInstances_PinnedInstances checks when UpdateOnlyTheseInstances
// is set to subset of instances and new pod spec is different from all
// currently running pod specs, getUpdateInstances should return only the
// instance ids specified by UpdateOnlyTheseInstances.
func (suite *ServiceHandlerTestSuite) TestGetUpdateInstances_PinnedInstances() {
	defer goleak.VerifyNoLeaks(suite.T())

	req := &api.JobUpdateRequest{
		InstanceCount: ptr.Int32(3),
		Settings: &api.JobUpdateSettings{
			UpdateOnlyTheseInstances: []*api.Range{
				{First: ptr.Int32(0), Last: ptr.Int32(1)},
			},
		},
	}

	ui := getUpdateInstances(req)
	suite.Equal(map[uint32]struct{}{
		0: {},
		1: {},
	}, ui)
}

// TestGetSpecChangedInstances tests getSpecChangedInstances util function.
func (suite *ServiceHandlerTestSuite) TestGetSpecChangedInstances() {
	defer goleak.VerifyNoLeaks(suite.T())

	instances := int32(3)
	genPodSpec := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
	}
	podStates := map[uint32]*podStateSpec{
		0: {
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k2", Value: "v2"},
				},
			},
		},
		1: {
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k1", Value: "v1"},
					{Key: "k3", Value: "v3"},
				},
			},
		},
		2: {
			podSpec: &pod.PodSpec{
				Labels: []*peloton.Label{
					{Key: "k2", Value: "v2"},
					{Key: common.BridgeUpdateLabelKey, Value: "12345"},
					{Key: "k1", Value: "v1"},
				},
			},
		},
	}

	si, err := suite.handler.getSpecChangedInstances(
		suite.ctx,
		instances,
		podStates,
		genPodSpec,
	)
	suite.NoError(err)
	suite.Equal(map[uint32]struct{}{
		1: {},
	}, si)
}

// TestChangeBridgeUpdateLabel tests changeBridgeUpdateLabel
func (suite *ServiceHandlerTestSuite) TestChangeBridgeUpdateLabel() {
	defer goleak.VerifyNoLeaks(suite.T())

	p1v := "12345"
	p1 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: common.BridgeUpdateLabelKey, Value: p1v},
			{Key: "k1", Value: "v1"},
		},
	}
	p2 := &pod.PodSpec{}
	p3 := &pod.PodSpec{
		Labels: []*peloton.Label{
			{Key: "k1", Value: "v1"},
		},
	}

	p1n := suite.handler.changeBridgeUpdateLabel(p1)
	suite.Equal(p1v, p1.GetLabels()[0].GetValue())
	suite.NotEqual(p1v, p1n.GetLabels()[0].GetValue())

	p2n := suite.handler.changeBridgeUpdateLabel(p2)
	suite.Empty(p2.GetLabels())
	suite.Equal(common.BridgeUpdateLabelKey, p2n.GetLabels()[0].GetKey())

	p3n := suite.handler.changeBridgeUpdateLabel(p3)
	suite.Len(p3.GetLabels(), 1)
	suite.Equal(common.BridgeUpdateLabelKey, p3n.GetLabels()[1].GetKey())
}

// TestGetPodRunsLimit tests getPodRunsLimit
func TestGetPodRunsLimit(t *testing.T) {
	testCases := []struct {
		name         string
		podsNum      uint32
		podsMax      uint32
		podRunsDepth uint32
		wantPodRuns  uint32
	}{
		{
			name:         "test total pods less than pod max",
			podsNum:      100,
			podsMax:      1000,
			podRunsDepth: 5,
			wantPodRuns:  5,
		},
		{
			name:         "test total pods larger than pod max, calculated depth larger than min pod run depth",
			podsNum:      300,
			podsMax:      1000,
			podRunsDepth: 6,
			wantPodRuns:  4,
		},
		{
			name:         "test total pods larger than pod max, calculated depth less than min pod run depth",
			podsNum:      1000,
			podsMax:      1000,
			podRunsDepth: 5,
			wantPodRuns:  minPodRunsDepth,
		},
		{
			name:         "test total pods larger than pod max, calculated depth less than min pod run depth, but pod_runs_depth is less min_pod_runs_depth",
			podsNum:      1000,
			podsMax:      1000,
			podRunsDepth: 1,
			wantPodRuns:  util.Min(1, minPodRunsDepth),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := getPodRunsLimit(tc.podsNum, tc.podsMax, tc.podRunsDepth)
			assert.Equal(t, tc.wantPodRuns, l)
		})
	}
}
