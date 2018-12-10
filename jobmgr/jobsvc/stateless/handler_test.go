package stateless

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	pelotonv1alphaquery "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/query"
	pelotonv1alpharespool "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"

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
	testJobID    = "481d565e-28da-457d-8434-f6bb7faa0e95"
	testUpdateID = "941ff353-ba82-49fe-8f80-fb5bc649b04d"
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
	suite.respoolClient = respoolmocks.NewMockResourceManagerYARPCClient(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:      suite.jobFactory,
		candidate:       suite.candidate,
		goalStateDriver: suite.goalStateDriver,
		jobStore:        suite.jobStore,
		updateStore:     suite.updateStore,
		respoolClient:   suite.respoolClient,
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
			JobId: &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{
				Value: fmt.Sprintf("%d-%d", version, version),
			},
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
			JobId: &v1alphapeloton.JobID{Value: testJobID},
			Version: &v1alphapeloton.EntityVersion{
				Value: fmt.Sprintf("%d-%d", version, version),
			},
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
			jobutil.GetJobEntityVersion(configVersion, workflowVersion),
			gomock.Any(),
		).
		Return(
			&peloton.UpdateID{
				Value: testUpdateID,
			},
			jobutil.GetJobEntityVersion(configVersion+1, workflowVersion+1),
			nil)

	suite.goalStateDriver.EXPECT().
		EnqueueUpdate(&peloton.JobID{Value: testJobID}, &peloton.UpdateID{Value: testUpdateID}, gomock.Any()).
		Return()

	resp, err := suite.handler.ReplaceJob(
		context.Background(),
		&statelesssvc.ReplaceJobRequest{
			JobId:   &v1alphapeloton.JobID{Value: testJobID},
			Version: jobutil.GetJobEntityVersion(configVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
		},
	)
	suite.NoError(err)
	suite.Equal(resp.GetVersion(), jobutil.GetJobEntityVersion(configVersion+1, workflowVersion+1))
}

// TestReplaceJobInitializedJobFailure tests the failure case of replacing job
// due to job is in INITIALIZED state
func (suite *statelessHandlerTestSuite) TestReplaceJobInitializedJobFailure() {
	configVersion := uint64(1)
	workflowVersion := uint64(1)
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
			Version: jobutil.GetJobEntityVersion(configVersion, workflowVersion),
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
			Version: jobutil.GetJobEntityVersion(configVersion, workflowVersion),
			Spec:    &stateless.JobSpec{},
			UpdateSpec: &stateless.UpdateSpec{
				BatchSize: batchSize,
			},
		},
	)
	suite.Error(err)
	suite.Nil(resp)
}

func TestStatelessServiceHandler(t *testing.T) {
	suite.Run(t, new(statelessHandlerTestSuite))
}
