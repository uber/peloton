package stateless

import (
	"context"
	"testing"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	statelesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"
	"code.uber.internal/infra/peloton/jobmgr/cached"

	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	goalstatemocks "code.uber.internal/infra/peloton/jobmgr/goalstate/mocks"
	leadermocks "code.uber.internal/infra/peloton/leader/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
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
	goalStateDriver *goalstatemocks.MockDriver
	jobStore        *storemocks.MockJobStore
}

func (suite *statelessHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedWorkflow = cachedmocks.NewMockUpdate(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.candidate = leadermocks.NewMockCandidate(suite.ctrl)
	suite.goalStateDriver = goalstatemocks.NewMockDriver(suite.ctrl)
	suite.jobStore = storemocks.NewMockJobStore(suite.ctrl)
	suite.handler = &serviceHandler{
		jobFactory:      suite.jobFactory,
		candidate:       suite.candidate,
		goalStateDriver: suite.goalStateDriver,
		jobStore:        suite.jobStore,
	}
}

func (suite *statelessHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
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
	suite.Equal(resp.GetStatus().GetWorkflowStatus().GetCurrentInstances(),
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

func TestStatelessServiceHandler(t *testing.T) {
	suite.Run(t, new(statelessHandlerTestSuite))
}
