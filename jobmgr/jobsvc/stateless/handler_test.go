package stateless

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	statelesssvcmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc/mocks"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	pelotonv1alphaquery "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/query"
	pelotonv1alpharespool "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobmgrcommon "code.uber.internal/infra/peloton/jobmgr/common"
	"code.uber.internal/infra/peloton/jobmgr/jobsvc"
	handlerutil "code.uber.internal/infra/peloton/jobmgr/util/handler"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
	"code.uber.internal/infra/peloton/util"

	respoolmocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool/mocks"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	leadermocks "code.uber.internal/infra/peloton/leader/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testJobName              = "test-job"
	testJobID                = "481d565e-28da-457d-8434-f6bb7faa0e95"
	testUpdateID             = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
	testEntityVersion        = "1-1-1"
	testSecretPath           = "/tmp/secret"
	testSecretStr            = "top-secret-token"
	testConfigurationVersion = uint64(1)
)

var (
	testRespoolID = &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	testCmd               = "echo test"
	defaultResourceConfig = &pod.ResourceSpec{
		CpuLimit:    10,
		MemLimitMb:  10,
		DiskLimitMb: 10,
		FdLimit:     10,
	}
)

type statelessHandlerTestSuite struct {
	suite.Suite

	handler *serviceHandler

	ctrl            *gomock.Controller
	cachedJob       *cachedmocks.MockJob
	cachedWorkflow  *cachedmocks.MockUpdate
	jobFactory      *cachedmocks.MockJobFactory
	candidate       *leadermocks.MockCandidate
	respoolClient   *respoolmocks.MockResourceManagerYARPCClient
	goalStateDriver *goalstatemocks.MockDriver
	jobStore        *storemocks.MockJobStore
	updateStore     *storemocks.MockUpdateStore
	listJobsServer  *statelesssvcmocks.MockJobServiceServiceListJobsYARPCServer
	secretStore     *storemocks.MockSecretStore
	taskStore       *storemocks.MockTaskStore
}

func (suite *statelessHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedWorkflow = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.candidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.taskStore = storemocks.NewMockTaskStore(suite.ctrl)
	suite.respoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.listJobsServer = statelesssvcmocks.NewMockJobServiceServiceListJobsYARPCServer(suite.ctrl)
	suite.secretStore = storemocks.NewMockSecretStore(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:      suite.jobFactory,
		candidate:       suite.candidate,
		goalStateDriver: suite.goalStateDriver,
		jobStore:        suite.jobStore,
		updateStore:     suite.updateStore,
		taskStore:       suite.taskStore,
		respoolClient:   suite.respoolClient,
		secretStore:     suite.secretStore,
		rootCtx:         context.Background(),
		jobSvcCfg: jobsvc.Config{
			EnableSecrets:  true,
			MaxTasksPerJob: 100000,
		},
	}
}

func (suite *statelessHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestGetJobWithSummarySuccess tests invoking
// GetJob API to get job summary
func (suite *statelessHandlerTestSuite) TestGetJobWithSummarySuccess() {
	suite.jobStore.EXPECT().
		GetJobSummaryFromIndex(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(&pbjob.JobSummary{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetSummary().GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetSummary().GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
}

// TestGetJobWithSummaryGetConfigError tests invoking
// GetJob API to get job summary with DB error
func (suite *statelessHandlerTestSuite) TestGetJobWithSummaryGetConfigError() {
	suite.jobStore.EXPECT().
		GetJobSummaryFromIndex(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobWithSummaryGetUpdateError tests invoking
// GetJob API to get job summary with DB error when fetching update info
func (suite *statelessHandlerTestSuite) TestGetJobWithSummaryGetUpdateError() {
	suite.jobStore.EXPECT().
		GetJobSummaryFromIndex(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(&pbjob.JobSummary{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:       &v1alphapeloton.JobID{Value: testJobID},
			SummaryOnly: true,
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobConfigVersionSuccess tests invoking
// GetJob API to get job configuration for a given version
func (suite *statelessHandlerTestSuite) TestGetJobConfigVersionSuccess() {
	version := uint64(3)
	instanceCount := uint32(5)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			version,
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(version, 1, 1),
		})
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetJobInfo().GetSpec().GetInstanceCount(),
		instanceCount,
	)
}

// TestGetJobConfigVersionError tests invoking
// GetJob API to get job configuration for a given version with DB error
func (suite *statelessHandlerTestSuite) TestGetJobConfigVersionError() {
	version := uint64(3)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			version,
		).
		Return(nil, nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(version, 1, 1),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobSuccess tests invoking GetJob API to get job
// configuration, runtime and workflow information
func (suite *statelessHandlerTestSuite) TestGetJobSuccess() {
	instanceCount := uint32(5)

	suite.jobStore.EXPECT().
		GetJobConfig(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(
		resp.GetJobInfo().GetSpec().GetInstanceCount(),
		instanceCount,
	)
	suite.Equal(
		resp.GetJobInfo().GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetJobInfo().GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
}

// TestGetJobConfigGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job configuration
func (suite *statelessHandlerTestSuite) TestGetJobConfigGetError() {
	suite.jobStore.EXPECT().
		GetJobConfig(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(nil, nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobRuntimeGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch job runtime
func (suite *statelessHandlerTestSuite) TestGetJobRuntimeGetError() {
	instanceCount := uint32(5)

	suite.jobStore.EXPECT().
		GetJobConfig(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobUpdateGetError tests invoking GetJob API to get job
// configuration, runtime and workflow information with DB error
// when trying to fetch update information
func (suite *statelessHandlerTestSuite) TestGetJobUpdateGetError() {
	instanceCount := uint32(5)

	suite.jobStore.EXPECT().
		GetJobConfig(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
		}, nil, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
		).
		Return(&pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	resp, err := suite.handler.GetJob(context.Background(),
		&statelesssvc.GetJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobCacheWithUpdateSuccess test the success case of getting job
// cache which has update
func (suite *statelessHandlerTestSuite) TestGetJobCacheWithUpdateSuccess() {
	instanceCount := uint32(10)
	curJobVersion := uint64(1)
	targetJobVersion := uint64(2)
	instancesDone := []uint32{0, 1, 2}
	instancesFailed := []uint32{3, 4}
	instancesCurrent := []uint32{5}
	totalInstances := []uint32{0, 1, 2, 3, 4, 5, 6, 7}
	priority := uint32(2)
	preemptible := true
	revocable := false
	maximumUnavailableInstances := uint32(2)

	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:     pbjob.JobState_PENDING,
			GoalState: pbjob.JobState_RUNNING,
			UpdateID:  &peloton.UpdateID{Value: testUpdateID},
		}, nil)

	suite.cachedJob.EXPECT().
		GetConfig(gomock.Any()).
		Return(&pbjob.JobConfig{
			InstanceCount: instanceCount,
			SLA: &pbjob.SlaConfig{
				Priority:                    priority,
				Preemptible:                 preemptible,
				Revocable:                   revocable,
				MaximumUnavailableInstances: maximumUnavailableInstances,
			},
		}, nil)

	suite.cachedJob.EXPECT().
		GetWorkflow(&peloton.UpdateID{Value: testUpdateID}).
		Return(suite.cachedWorkflow)

	suite.cachedWorkflow.EXPECT().
		GetWorkflowType().
		Return(models.WorkflowType_UPDATE)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			State: pbupdate.State_ROLLING_FORWARD,
		})

	suite.cachedWorkflow.EXPECT().
		GetInstancesDone().
		Return(instancesDone).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetInstancesFailed().
		Return(instancesFailed).
		AnyTimes()

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: totalInstances,
		})

	suite.cachedWorkflow.EXPECT().
		GetInstancesCurrent().
		Return(instancesCurrent)

	suite.cachedWorkflow.EXPECT().
		GetState().
		Return(&cached.UpdateStateVector{
			JobVersion: curJobVersion,
		})

	suite.cachedWorkflow.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			JobVersion: targetJobVersion,
		})

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})

	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(resp.GetSpec().GetInstanceCount(), instanceCount)
	suite.Equal(resp.GetSpec().GetSla().GetPriority(), priority)
	suite.Equal(resp.GetSpec().GetSla().GetPreemptible(), preemptible)
	suite.Equal(resp.GetSpec().GetSla().GetRevocable(), revocable)
	suite.Equal(resp.GetSpec().GetSla().GetMaximumUnavailableInstances(),
		maximumUnavailableInstances)

	suite.Equal(resp.GetStatus().GetState(), stateless.JobState_JOB_STATE_PENDING)
	suite.Equal(resp.GetStatus().GetDesiredState(), stateless.JobState_JOB_STATE_RUNNING)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetType(),
		stateless.WorkflowType_WORKFLOW_TYPE_UPDATE)
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesCompleted(),
		uint32(len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesFailed(),
		uint32(len(instancesFailed)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetNumInstancesRemaining(),
		uint32(len(totalInstances)-len(instancesFailed)-len(instancesDone)))
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetInstancesCurrent(),
		instancesCurrent)
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetPrevVersion().GetValue(),
		"1")
	suite.Equal(
		resp.GetStatus().GetWorkflowStatus().GetVersion().GetValue(),
		"2")
}

// TestGetJobCacheGetJobFail test the failure case of getting job cache due to
// failure of getting runtime
func (suite *statelessHandlerTestSuite) TestGetJobCacheGetRuntimeFail() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetJobCacheNotFound test the failure case of getting job cache due to
// job cache not found
func (suite *statelessHandlerTestSuite) TestGetJobCacheNotFound() {
	suite.jobFactory.EXPECT().
		GetJob(&peloton.JobID{Value: testJobID}).
		Return(nil)

	resp, err := suite.handler.GetJobCache(context.Background(),
		&statelesssvc.GetJobCacheRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Error(err)
	suite.Nil(resp)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestRefreshJobSuccess tests the case of successfully refreshing job
func (suite *statelessHandlerTestSuite) TestRefreshJobSuccess() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
	}
	configAddOn := &models.ConfigAddOn{}
	jobRuntime := &pbjob.RuntimeInfo{
		State: pbjob.JobState_RUNNING,
	}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(jobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(jobRuntime, nil)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), &pbjob.JobInfo{
			Config:  jobConfig,
			Runtime: jobRuntime,
		}, configAddOn, cached.UpdateCacheOnly).
		Return(nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestRefreshJobFailNonLeader tests the failure case of refreshing job
// due to JobMgr is not leader
func (suite *statelessHandlerTestSuite) TestRefreshJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)
	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetConfigFail tests the case of failure due to
// failure of getting job config
func (suite *statelessHandlerTestSuite) TestRefreshJobGetConfigFail() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestRefreshJobGetRuntimeFail tests the case of failure due to
// failure of getting job runtime
func (suite *statelessHandlerTestSuite) TestRefreshJobGetRuntimeFail() {
	jobConfig := &pbjob.JobConfig{
		InstanceCount: 10,
	}
	configAddOn := &models.ConfigAddOn{}

	suite.candidate.EXPECT().
		IsLeader().
		Return(true)

	suite.jobStore.EXPECT().
		GetJobConfig(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(jobConfig, configAddOn, nil)

	suite.jobStore.EXPECT().
		GetJobRuntime(gomock.Any(), &peloton.JobID{Value: testJobID}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.RefreshJob(context.Background(), &statelesssvc.RefreshJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
	})
	suite.Nil(resp)
	suite.Error(err)
}

// TestQueryJobsSuccess tests the success case of query jobs
func (suite *statelessHandlerTestSuite) TestQueryJobsSuccess() {
	pagination := &pelotonv1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  10,
		OrderBy: []*pelotonv1alphaquery.OrderBy{
			{
				Order:    pelotonv1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &pelotonv1alphaquery.PropertyPath{Value: "creation_time"},
			},
		},
		MaxLimit: 100,
	}
	labels := []*v1alphapeloton.Label{{Key: "k1", Value: "v1"}}
	keywords := []string{"key1", "key2"}
	jobstates := []stateless.JobState{stateless.JobState_JOB_STATE_RUNNING}
	respoolPath := &pelotonv1alpharespool.ResourcePoolPath{
		Value: "/testPath",
	}
	owner := "owner1"
	name := "test"
	respoolID := &peloton.ResourcePoolID{Value: "321d565e-28da-457d-8434-f6bb7faa0e95"}
	updateID := &peloton.UpdateID{Value: "322e122e-28da-457d-8434-f6bb7faa0e95"}
	jobSummary := &pbjob.JobSummary{
		Name:  name,
		Owner: owner,
		Runtime: &pbjob.RuntimeInfo{
			State:    pbjob.JobState_RUNNING,
			UpdateID: updateID,
		},
		Labels: []*peloton.Label{{
			Key:   labels[0].GetKey(),
			Value: labels[0].GetValue(),
		}},
	}
	timestamp, err := ptypes.TimestampProto(time.Now())
	suite.NoError(err)
	spec := &stateless.QuerySpec{
		Pagination: pagination,
		Labels:     labels,
		Keywords:   keywords,
		JobStates:  jobstates,
		Respool:    respoolPath,
		Owner:      owner,
		Name:       name,
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
	}
	totalResult := uint32(1)

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: respoolPath.GetValue()},
		}).
		Return(&respool.LookupResponse{Id: respoolID}, nil)

	suite.jobStore.EXPECT().
		QueryJobs(gomock.Any(), respoolID, gomock.Any(), true).
		Return(nil, []*pbjob.JobSummary{jobSummary}, totalResult, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), updateID).
		Return(&models.UpdateModel{
			Type:                 models.WorkflowType_UPDATE,
			State:                pbupdate.State_ROLLING_FORWARD,
			InstancesTotal:       10,
			InstancesDone:        1,
			InstancesFailed:      6,
			InstancesCurrent:     []uint32{0, 1, 2},
			PrevJobConfigVersion: 1,
			JobConfigVersion:     2,
		}, nil)

	resp, err := suite.handler.QueryJobs(
		context.Background(),
		&statelesssvc.QueryJobsRequest{
			Spec: spec,
		},
	)
	suite.NotNil(resp)
	suite.Equal(resp.GetPagination(), &pelotonv1alphaquery.Pagination{
		Offset: pagination.GetOffset(),
		Limit:  pagination.GetLimit(),
		Total:  totalResult,
	})
	suite.Equal(resp.GetSpec(), spec)
	suite.Equal(resp.GetRecords()[0].GetOwner(), jobSummary.GetOwner())
	suite.Equal(resp.GetRecords()[0].GetOwningTeam(), jobSummary.GetOwningTeam())
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetKey(),
		jobSummary.GetLabels()[0].GetKey(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetLabels()[0].GetValue(),
		jobSummary.GetLabels()[0].GetValue(),
	)
	suite.Equal(
		resp.GetRecords()[0].GetStatus().GetState(),
		stateless.JobState_JOB_STATE_RUNNING,
	)
	suite.Equal(
		resp.GetRecords()[0].GetStatus().GetWorkflowStatus().GetState(),
		stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
	)
	suite.NoError(err)
}

// TestQueryJobsGetRespoolIDFail tests the failure case of query jobs
// due to get respool id
func (suite *statelessHandlerTestSuite) TestQueryJobsGetRespoolIdFail() {
	pagination := &pelotonv1alphaquery.PaginationSpec{
		Offset: 0,
		Limit:  10,
		OrderBy: []*pelotonv1alphaquery.OrderBy{
			{
				Order:    pelotonv1alphaquery.OrderBy_ORDER_BY_ASC,
				Property: &pelotonv1alphaquery.PropertyPath{Value: "creation_time"},
			},
		},
		MaxLimit: 100,
	}
	labels := []*v1alphapeloton.Label{{Key: "k1", Value: "v1"}}
	keywords := []string{"key1", "key2"}
	jobstates := []stateless.JobState{stateless.JobState_JOB_STATE_RUNNING}
	respoolPath := &pelotonv1alpharespool.ResourcePoolPath{
		Value: "/testPath",
	}
	owner := "owner1"
	name := "test"
	timestamp, err := ptypes.TimestampProto(time.Now())
	suite.NoError(err)
	spec := &stateless.QuerySpec{
		Pagination: pagination,
		Labels:     labels,
		Keywords:   keywords,
		JobStates:  jobstates,
		Respool:    respoolPath,
		Owner:      owner,
		Name:       name,
		CreationTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
		CompletionTimeRange: &v1alphapeloton.TimeRange{
			Max: timestamp,
		},
	}

	suite.respoolClient.EXPECT().
		LookupResourcePoolID(gomock.Any(), &respool.LookupRequest{
			Path: &respool.ResourcePoolPath{Value: respoolPath.GetValue()},
		}).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.QueryJobs(
		context.Background(),
		&statelesssvc.QueryJobsRequest{
			Spec: spec,
		},
	)
	suite.Nil(resp)
	suite.Error(err)
}

// TestReplaceJobSuccess tests the success case of replacing job
func (suite *statelessHandlerTestSuite) TestReplaceJobSuccess() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	batchSize := uint32(1)
	opaque := "test"

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			configVersion,
		).Return(
		&pbjob.JobConfig{
			Type: pbjob.JobType_SERVICE,
		},
		&models.ConfigAddOn{
			SystemLabels: []*peloton.Label{
				{Key: common.SystemLabelResourcePool, Value: "/testRespool"},
			},
		},
		nil)

	suite.cachedJob.EXPECT().
		CreateWorkflow(
			gomock.Any(),
			models.WorkflowType_UPDATE,
			&pbupdate.UpdateConfig{
				BatchSize: batchSize,
			},
			jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{
				Value: testUpdateID,
			},
			jobutil.GetJobEntityVersion(configVersion+1, desiredStateVersion, workflowVersion+1),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any()).
		Return()

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		},
	)
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), jobutil.GetJobEntityVersion(configVersion+1, desiredStateVersion, workflowVersion+1))
}

// TestReplaceJobInitializedJobFailure tests the failure case of replacing job
// due to job is in INITIALIZED state
func (suite *statelessHandlerTestSuite) TestReplaceJobInitializedJobFailure() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	batchSize := uint32(1)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_INITIALIZED,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestReplaceJobGetJobConfigFailure tests the failure case of replacing job
// due to not able to get job config
func (suite *statelessHandlerTestSuite) TestReplaceJobGetJobConfigFailure() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	batchSize := uint32(1)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			configVersion,
		).Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

// TestGetReplaceJobDiffSuccess tests the success case of getting the
// difference in configuration for ReplaceJob API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffSuccess() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	entityVersion := jobutil.GetJobEntityVersion(
		configVersion,
		workflowVersion,
		desiredStateVersion,
	)
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			configVersion,
		).Return(
		&pbjob.JobConfig{
			Type:          pbjob.JobType_SERVICE,
			InstanceCount: instanceCount,
		},
		nil,
		nil)

	suite.taskStore.EXPECT().
		GetTaskRuntimesForJobByRange(gomock.Any(), gomock.Any(), nil).
		Return(taskRuntimes, nil)

	suite.taskStore.EXPECT().
		GetTaskConfig(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil, nil).
		Times(int(instanceCount))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.NoError(err)
}

// TestGetReplaceJobDiffSuccess tests the failure case of DB error when
// fetching the job runtime when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffRuntimeDBError() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	entityVersion := jobutil.GetJobEntityVersion(
		configVersion,
		desiredStateVersion,
		workflowVersion,
	)
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestGetReplaceJobDiffValidateVersionFail failure case of entity
// version validation failure when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffValidateVersionFail() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	entityVersion := jobutil.GetJobEntityVersion(
		configVersion,
		desiredStateVersion,
		workflowVersion,
	)
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestGetReplaceJobDiffGetConfigError tests the failure case of DB error when
// fetching the job configuration when invoking GetReplaceJobDiff API
func (suite *statelessHandlerTestSuite) TestGetReplaceJobDiffGetConfigError() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
	desiredStateVersion := uint64(1)
	entityVersion := jobutil.GetJobEntityVersion(
		configVersion,
		desiredStateVersion,
		workflowVersion,
	)
	instanceCount := uint32(5)
	taskRuntimes := make(map[uint32]*pbtask.RuntimeInfo)
	for i := uint32(0); i < instanceCount; i++ {
		runtime := &pbtask.RuntimeInfo{
			State: pbtask.TaskState_RUNNING,
		}
		taskRuntimes[i] = runtime
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			WorkflowVersion:      workflowVersion,
			ConfigurationVersion: configVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		ValidateEntityVersion(gomock.Any(), entityVersion).
		Return(nil)

	suite.jobStore.EXPECT().
		GetJobConfigWithVersion(
			gomock.Any(),
			&peloton.JobID{Value: testJobID},
			configVersion,
		).Return(
		nil, nil, yarpcerrors.InternalErrorf("test error"))

	_, err := suite.handler.GetReplaceJobDiff(
		context.Background(),
		&statelesssvc.GetReplaceJobDiffRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
			Spec: &stateless.JobSpec{
				InstanceCount: instanceCount,
			},
		},
	)
	suite.Error(err)
}

// TestResumeJobWorkflowSuccess tests the success case of resume workflow
func (suite *statelessHandlerTestSuite) TestResumeJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-2"}
	opaque := "test"

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.ResumeJobWorkflow(context.Background(),
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestResumeJobWorkflowFailure tests the failure case of resume workflow
// due to fail to resume workflow
func (suite *statelessHandlerTestSuite) TestResumeJobWorkflowResumeWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		ResumeWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.ResumeJobWorkflow(context.Background(),
		&statelesssvc.ResumeJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestAbortJobWorkflowSuccess tests the success case of abort workflow
func (suite *statelessHandlerTestSuite) TestAbortJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-2"}
	opaque := "test"

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.AbortJobWorkflow(context.Background(),
		&statelesssvc.AbortJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestAbortJobWorkflowAbortWorkflowFailure tests the failure case of abort workflow
// due to fail to abort workflow
func (suite *statelessHandlerTestSuite) TestAbortJobWorkflowAbortWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		AbortWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.AbortJobWorkflow(context.Background(),
		&statelesssvc.AbortJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestPauseJobWorkflowSuccess tests the success case of pause workflow
func (suite *statelessHandlerTestSuite) TestPauseJobWorkflowSuccess() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}
	newEntityVersion := &v1alphapeloton.EntityVersion{Value: "1-2"}
	opaque := "test"

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(&peloton.UpdateID{Value: testUpdateID}, newEntityVersion, nil)

	suite.cachedJob.EXPECT().
		ID().
		Return(&peloton.JobID{Value: testJobID})

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any())

	resp, err := suite.handler.PauseJobWorkflow(context.Background(),
		&statelesssvc.PauseJobWorkflowRequest{
			JobId:      &v1alphapeloton.JobID{Value: testJobID},
			Version:    entityVersion,
			OpaqueData: &v1alphapeloton.OpaqueData{Data: opaque},
		})
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), newEntityVersion)
}

// TestGetJobIDFromName tests the job name to job ids look up
func (suite *statelessHandlerTestSuite) TestGetJobIDFromName() {
	var jobIDs []*v1alphapeloton.JobID
	jobIDs = append(jobIDs, &v1alphapeloton.JobID{
		Value: testJobID,
	})

	suite.jobStore.EXPECT().
		GetJobIDFromJobName(gomock.Any(), testJobName).
		Return(nil, errors.New("failed to get job ids for job name"))

	_, err := suite.handler.GetJobIDFromJobName(context.Background(),
		&statelesssvc.GetJobIDFromJobNameRequest{
			JobName: testJobName,
		})
	suite.Error(err)

	suite.jobStore.EXPECT().
		GetJobIDFromJobName(gomock.Any(), testJobName).
		Return(jobIDs, nil)
	resp, err := suite.handler.GetJobIDFromJobName(context.Background(),
		&statelesssvc.GetJobIDFromJobNameRequest{
			JobName: testJobName,
		})

	suite.Equal(len(jobIDs), len(resp.GetJobId()))
}

// TestPauseJobWorkflowPauseWorkflowFailure tests the failure case of pause workflow
// due to fail to pause workflow
func (suite *statelessHandlerTestSuite) TestPauseJobWorkflowPauseWorkflowFailure() {
	entityVersion := &v1alphapeloton.EntityVersion{Value: "1-1"}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		PauseWorkflow(gomock.Any(), entityVersion, gomock.Any()).
		Return(nil, nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.PauseJobWorkflow(context.Background(),
		&statelesssvc.PauseJobWorkflowRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: entityVersion,
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestListJobsSuccess tests invoking ListJobs API successfully
func (suite *statelessHandlerTestSuite) TestListJobsSuccess() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobStore.EXPECT().
		GetAllJobsInJobIndex(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	suite.listJobsServer.EXPECT().
		Send(gomock.Any()).
		Do(func(resp *statelesssvc.ListJobsResponse) {
			suite.Equal(1, len(resp.GetJobs()))
			job := resp.GetJobs()[0]
			suite.Equal(job.GetName(), "testjob")
			suite.Equal(job.GetStatus().GetState(), stateless.JobState_JOB_STATE_RUNNING)
			suite.Equal(
				job.GetStatus().GetWorkflowStatus().GetState(),
				stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD,
			)
		}).
		Return(nil)

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.NoError(err)
}

// TestListJobsGetSummaryDBError tests getting DB error when fetching all
// job summaries from DB in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsGetSummaryDBError() {
	suite.jobStore.EXPECT().
		GetAllJobsInJobIndex(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.Error(err)
}

// TestListJobsGetUpdateError tests getting DB error when fetching
// the updae info from DB in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsGetUpdateError() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobStore.EXPECT().
		GetAllJobsInJobIndex(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(nil, fmt.Errorf("fake db error"))

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.Error(err)
}

// TestListJobsSendError tests getting an error during Send to
// the stream in the ListJobs API invocation
func (suite *statelessHandlerTestSuite) TestListJobsSendError() {
	jobs := []*pbjob.JobSummary{
		{
			Id:   &peloton.JobID{Value: testJobID},
			Name: "testjob",
			Runtime: &pbjob.RuntimeInfo{
				State:    pbjob.JobState_RUNNING,
				UpdateID: &peloton.UpdateID{Value: testUpdateID},
			},
		},
	}

	suite.jobStore.EXPECT().
		GetAllJobsInJobIndex(gomock.Any()).
		Return(jobs, nil)

	suite.updateStore.EXPECT().
		GetUpdate(gomock.Any(), &peloton.UpdateID{Value: testUpdateID}).
		Return(&models.UpdateModel{
			UpdateID: &peloton.UpdateID{Value: testUpdateID},
			State:    pbupdate.State_ROLLING_FORWARD,
		}, nil)

	suite.listJobsServer.EXPECT().
		Send(gomock.Any()).
		Return(fmt.Errorf("fake db error"))

	err := suite.handler.ListJobs(
		&statelesssvc.ListJobsRequest{},
		suite.listJobsServer,
	)
	suite.Error(err)
}

// TestCreateJobSuccess tests the success case of creating a job
func (suite *statelessHandlerTestSuite) TestCreateJobSuccess() {
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   testRespoolID,
	}

	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			Create(gomock.Any(), jobConfig, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				ConfigurationVersion: testConfigurationVersion,
				DesiredStateVersion:  1,
				WorkflowVersion:      1,
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response.GetJobId())
	suite.Equal(testEntityVersion, response.GetVersion().GetValue())
}

// TestCreateJobFailNonLeader tests the failure case of creating job
// due to JobMgr is not leader
func (suite *statelessHandlerTestSuite) TestCreateJobFailNonLeader() {
	suite.candidate.EXPECT().
		IsLeader().
		Return(false)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailInvalidJobID tests the failure case of creating job
// due to invalid JobID provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailInvalidJobID() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	response, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: "invalid-Job-ID"},
		})
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobFailNullResourcePool tests the failure case of creating job
// due to missing resource pool in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailNullResourcePool() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailRootResourcePool tests the failure case of creating job
// due to root resource pool provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailRootResourcePool() {
	suite.candidate.EXPECT().IsLeader().Return(true)

	resp, err := suite.handler.CreateJob(
		context.Background(),
		&statelesssvc.CreateJobRequest{
			JobId: &v1alphapeloton.JobID{Value: testJobID},
			Spec: &stateless.JobSpec{
				RespoolId: &v1alphapeloton.ResourcePoolID{
					Value: common.RootResPoolID,
				},
			},
		})
	suite.Nil(resp)
	suite.Error(err)
}

// TestCreateJobFailGetResourcePoolFailure tests the failure case of creating job
// due to error while getting resource pool info
func (suite *statelessHandlerTestSuite) TestCreateJobFailGetResourcePoolFailure() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailResourcePoolNotFound tests the failure case of creating job
// due to resource pool not found error
func (suite *statelessHandlerTestSuite) TestCreateJobFailResourcePoolNotFound() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(&respool.GetResponse{}, nil),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailResourcePoolNotFound tests the failure case of creating job
// due to non-leaf resource pool provided in the request
func (suite *statelessHandlerTestSuite) TestCreateJobFailNonLeafResourcePool() {
	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(&respool.GetResponse{
			Poolinfo: &respool.ResourcePoolInfo{
				Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				Children: []*peloton.ResourcePoolID{
					{
						Value: "child-respool1",
					},
				},
			},
		}, nil),
	)

	jobSpec := &stateless.JobSpec{
		RespoolId: testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailJobSpecToJobConfigConversionFailure tests the failure case of creating job
// due to error while converting job spec to job config
func (suite *statelessHandlerTestSuite) TestCreateJobFailJobSpecToJobConfigConversionFailure() {
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   testRespoolID,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobFailInvalidJobConfig tests the failure case of creating job
// due to invalid job config
func (suite *statelessHandlerTestSuite) TestCreateJobFailInvalidJobConfig() {
	jobSpec := &stateless.JobSpec{
		RespoolId:     testRespoolID,
		InstanceCount: suite.handler.jobSvcCfg.MaxTasksPerJob + 1,
	}
	request := &statelesssvc.CreateJobRequest{
		Spec: jobSpec,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Error(err)
	suite.Nil(response)
}

// TestCreateJobWithSecretsSuccess tests success scenario
// of creating a job with secrets
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsSuccess() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	dockerContainerizer := mesos.ContainerInfo_DOCKER
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	jobSpec.InstanceSpec = make(map[uint32]*pod.PodSpec)
	for i := uint32(0); i < 25; i++ {
		jobSpec.InstanceSpec[i] = &pod.PodSpec{
			Containers: []*pod.ContainerSpec{
				{
					Resource:  defaultResourceConfig,
					Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
				},
			},
		}
	}
	// Create job where one instance is using docker containerizer.
	// The create should succeed and this instance will be
	// launched without secrets because container info in default
	// config is overridden.
	jobSpec.InstanceSpec[10].Containers[0].Container =
		&mesos.ContainerInfo{Type: &dockerContainerizer}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.secretStore.EXPECT().CreateSecret(
			gomock.Any(), gomock.Any(), &peloton.JobID{Value: testJobID}).
			Return(nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			Create(gomock.Any(), jobConfig, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(&pbjob.RuntimeInfo{
				ConfigurationVersion: testConfigurationVersion,
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	// Create a job with a secret
	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.NoError(err)
	suite.NotNil(response)
	suite.Equal(testJobID, response.GetJobId().GetValue())
}

// TestCreateJobWithSecretsFailureSecretsAddedToSpec tests failure scenario of
// creating a job with secrets when secret volumes are directly added to job spec
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureSecretsAddedToSpec() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	secret := &peloton.Secret{
		Path: testSecretPath,
		Value: &peloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	jobSpec.GetDefaultSpec().GetContainers()[0].GetContainer().Volumes =
		append(
			jobSpec.GetDefaultSpec().GetContainers()[0].GetContainer().Volumes,
			util.CreateSecretVolume(secret.GetPath(),
				secret.GetId().GetValue()),
		)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: handlerutil.ConvertV0SecretsToV1Secrets([]*peloton.Secret{secret}),
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsCreateSecretsFailure tests failure scenario of
// creating a job with secrets due to db error
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsCreateSecretsFailure() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.secretStore.EXPECT().CreateSecret(
			gomock.Any(), gomock.Any(), &peloton.JobID{Value: testJobID}).
			Return(yarpcerrors.InternalErrorf("test error")),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureNoContainerizer tests failure scenario of
// creating a job with secrets when default spec doesn't contain any containers
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureNoContainerizer() {
	testCmd := "echo test"
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(base64.StdEncoding.EncodeToString(
				[]byte(testSecretStr))),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureEncodingError tests failure scenario of
// creating a job with secrets that are not base64 encoded
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureEncodingError() {
	testCmd := "echo test"
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command: &mesos.CommandInfo{Value: &testCmd},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureNonMesosContainerizer tests failure scenario of
// creating a job with secrets when default spec doesn't use Mesos containerizer
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureNonMesosContainerizer() {
	testCmd := "echo test"
	dockerContainerizer := mesos.ContainerInfo_DOCKER
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &dockerContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureSecretsDisabled tests failure scenario of
// creating a job with secrets when secrets are not enabled in JobSvcCfg
func (suite *statelessHandlerTestSuite) TestCreateJobWithSecretsFailureSecretsDisabled() {
	testCmd := "echo test"
	mesosContainerizer := mesos.ContainerInfo_MESOS
	defaultSpec := &pod.PodSpec{
		Containers: []*pod.ContainerSpec{
			{
				Command:   &mesos.CommandInfo{Value: &testCmd},
				Container: &mesos.ContainerInfo{Type: &mesosContainerizer},
			},
		},
	}
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		DefaultSpec: defaultSpec,
		RespoolId:   respoolID,
	}

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),
	)

	secret := &v1alphapeloton.Secret{
		Path: testSecretPath,
		Value: &v1alphapeloton.Secret_Value{
			Data: []byte(testSecretStr),
		},
	}

	request := &statelesssvc.CreateJobRequest{
		JobId:   &v1alphapeloton.JobID{Value: testJobID},
		Spec:    jobSpec,
		Secrets: []*v1alphapeloton.Secret{secret},
	}

	suite.handler.jobSvcCfg.EnableSecrets = false

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobWithSecretsFailureJobCacheCreateError tests failure scenario of
// creating a job with secrets due to error while creating job in cache
func (suite *statelessHandlerTestSuite) TestCreateJobFailureJobCacheCreateError() {
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		RespoolId: respoolID,
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			Create(gomock.Any(), jobConfig, gomock.Any(), gomock.Any()).
			Return(yarpcerrors.InternalErrorf("test error")),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec:  jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestCreateJobFailureGetJobRuntimeError tests failure scenario of
// creating a job due to error while getting job runtime
func (suite *statelessHandlerTestSuite) TestCreateJobFailureGetJobRuntimeError() {
	respoolID := &v1alphapeloton.ResourcePoolID{
		Value: "test-respool",
	}
	jobSpec := &stateless.JobSpec{
		RespoolId: respoolID,
	}

	jobConfig, err := handlerutil.ConvertJobSpecToJobConfig(jobSpec)
	suite.NoError(err)

	gomock.InOrder(
		suite.candidate.EXPECT().IsLeader().Return(true),

		suite.respoolClient.EXPECT().
			GetResourcePool(
				gomock.Any(),
				&respool.GetRequest{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			).Return(
			&respool.GetResponse{
				Poolinfo: &respool.ResourcePoolInfo{
					Id: &peloton.ResourcePoolID{Value: testRespoolID.GetValue()},
				},
			}, nil),

		suite.jobFactory.EXPECT().
			AddJob(gomock.Any()).
			Return(suite.cachedJob),

		suite.cachedJob.EXPECT().
			Create(gomock.Any(), jobConfig, gomock.Any(), gomock.Any()).
			Return(nil),

		suite.goalStateDriver.EXPECT().
			EnqueueJob(
				gomock.Any(),
				gomock.Any(),
			),

		suite.cachedJob.EXPECT().
			GetRuntime(gomock.Any()).
			Return(nil, yarpcerrors.InternalErrorf("test error")),
	)

	request := &statelesssvc.CreateJobRequest{
		JobId: &v1alphapeloton.JobID{Value: testJobID},
		Spec:  jobSpec,
	}

	response, err := suite.handler.CreateJob(context.Background(), request)
	suite.Nil(response)
	suite.Error(err)
}

// TestStopJobSuccess tests the success case of stopping job
func (suite *statelessHandlerTestSuite) TestStopJobSuccess() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion + 1,
		WorkflowVersion:      workflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		jobutil.GetJobEntityVersion(configVersion, desiredStateVersion+1, workflowVersion))
}

// TestStopJobInvalidEntityVersionFailure tests the failure
// case of stopping job due to invalid entity version
func (suite *statelessHandlerTestSuite) TestStopJobInvalidEntityVersionFailure() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion+1, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobSuccessWithCompareAndSetRuntimeRetry tests the success
// case of stopping job after CompareAndSetRuntime retries due to UnexpectedVersionError
func (suite *statelessHandlerTestSuite) TestStopJobSuccessWithCompareAndSetRuntimeRetry() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			GoalState:            pbjob.JobState_RUNNING,
			ConfigurationVersion: configVersion,
			DesiredStateVersion:  desiredStateVersion,
			WorkflowVersion:      workflowVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(nil, jobmgrcommon.UnexpectedVersionError)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			State:                pbjob.JobState_RUNNING,
			GoalState:            pbjob.JobState_RUNNING,
			ConfigurationVersion: configVersion,
			DesiredStateVersion:  desiredStateVersion,
			WorkflowVersion:      workflowVersion,
		}, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(&pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_KILLED,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion + 1,
		WorkflowVersion:      workflowVersion,
	}, nil)

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.NoError(err)
	suite.Equal(
		resp.GetVersion(),
		jobutil.GetJobEntityVersion(configVersion, desiredStateVersion+1, workflowVersion))
}

// TestStopJobGetRuntimeFailure tests the failure case of stopping job
// due to runtime failure
func (suite *statelessHandlerTestSuite) TestStopJobGetRuntimeFailure() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

// TestStopJobCompareAndSetRuntimeFailure tests the failure case
// of stopping job due to CompareAndSetRuntime error
func (suite *statelessHandlerTestSuite) TestStopJobCompareAndSetRuntimeFailure() {
	configVersion := uint64(2)
	desiredStateVersion := uint64(3)
	workflowVersion := uint64(4)

	jobRuntime := &pbjob.RuntimeInfo{
		State:                pbjob.JobState_RUNNING,
		GoalState:            pbjob.JobState_RUNNING,
		ConfigurationVersion: configVersion,
		DesiredStateVersion:  desiredStateVersion,
		WorkflowVersion:      workflowVersion,
	}

	suite.jobFactory.EXPECT().
		AddJob(&peloton.JobID{Value: testJobID}).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		CompareAndSetRuntime(gomock.Any(), gomock.Any()).
		Do(func(ctx context.Context, jobRuntime *pbjob.RuntimeInfo) {
			suite.Equal(jobRuntime.GetGoalState(), pbjob.JobState_KILLED)
			suite.Equal(jobRuntime.GetDesiredStateVersion(), desiredStateVersion+1)
		}).Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.goalStateDriver.EXPECT().
		EnqueueJob(&peloton.JobID{Value: testJobID}, gomock.Any())

	resp, err := suite.handler.StopJob(
		context.Background(),
		&statelesssvc.StopJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, desiredStateVersion, workflowVersion),
		})
	suite.Error(err)
	suite.Nil(resp)
}

func TestStatelessServiceHandler(t *testing.T) {
	suite.Run(t, new(statelessHandlerTestSuite))
}
