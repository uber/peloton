package goalstate

import (
	"context"
	"fmt"
	"testing"
	"time"

	pbjob "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common/goalstate"
	goalstatemocks "code.uber.internal/infra/peloton/common/goalstate/mocks"
	"code.uber.internal/infra/peloton/jobmgr/cached"
	cachedmocks "code.uber.internal/infra/peloton/jobmgr/cached/mocks"
	storemocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

type UpdateActionsTestSuite struct {
	suite.Suite
	ctrl                  *gomock.Controller
	updateStore           *storemocks.MockUpdateStore
	jobFactory            *cachedmocks.MockJobFactory
	updateFactory         *cachedmocks.MockUpdateFactory
	updateGoalStateEngine *goalstatemocks.MockEngine
	jobGoalStateEngine    *goalstatemocks.MockEngine
	goalStateDriver       *driver
	jobID                 *peloton.JobID
	updateID              *peloton.UpdateID
	updateEnt             *updateEntity
	cachedJob             *cachedmocks.MockJob
	cachedUpdate          *cachedmocks.MockUpdate
}

func TestUpdateActions(t *testing.T) {
	suite.Run(t, new(UpdateActionsTestSuite))
}

func (suite *UpdateActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.updateStore = storemocks.NewMockUpdateStore(suite.ctrl)
	suite.jobFactory = cachedmocks.NewMockJobFactory(suite.ctrl)
	suite.updateFactory = cachedmocks.NewMockUpdateFactory(suite.ctrl)
	suite.updateGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.jobGoalStateEngine = goalstatemocks.NewMockEngine(suite.ctrl)
	suite.goalStateDriver = &driver{
		updateStore:   suite.updateStore,
		jobFactory:    suite.jobFactory,
		updateFactory: suite.updateFactory,
		updateEngine:  suite.updateGoalStateEngine,
		jobEngine:     suite.jobGoalStateEngine,
		mtx:           NewMetrics(tally.NoopScope),
		cfg:           &Config{},
	}
	suite.goalStateDriver.cfg.normalize()

	suite.jobID = &peloton.JobID{Value: uuid.NewRandom().String()}
	suite.updateID = &peloton.UpdateID{Value: uuid.NewRandom().String()}
	suite.updateEnt = &updateEntity{
		id:     suite.updateID,
		jobID:  suite.jobID,
		driver: suite.goalStateDriver,
	}

	suite.cachedJob = cachedmocks.NewMockJob(suite.ctrl)
	suite.cachedUpdate = cachedmocks.NewMockUpdate(suite.ctrl)
}

func (suite *UpdateActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestUpdateCheckIfAbortedRuntimeGetFail tests if fetching the job runtime
// fails while trying to check if current job update in the goal state
// engine needs to be aborted
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedRuntimeGetFail() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateCheckIfAbortedRuntimeGetFail tests that the current update
// in the goal state engine and job runtime are the same
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedRunUpdate() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateCheckIfAbortedUpdateAbort tests that the current update in the
// goal state engine and the job runtime are not the same
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedUpdateAbort() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_ABORTED,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(updateInfo, nil)

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.True(yarpcerrors.IsAborted(err))
	suite.EqualError(err, "code:aborted message:update aborted")
}

// TestUpdateCheckIfAbortedUpdateAbortFail tests that the current update in the
// goal state engine and the job runtime are not the same and aborting the
// update fails
func (suite *UpdateActionsTestSuite) TestUpdateCheckIfAbortedUpdateAbortFail() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: &peloton.UpdateID{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), suite.updateID).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateAbortIfNeeded(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateReload tests reloading the update from the DB
func (suite *UpdateActionsTestSuite) TestUpdateReload() {
	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateReloadNotExists tests reloading an update which
// does not exist in the DB
func (suite *UpdateActionsTestSuite) TestUpdateReloadNotExists() {
	suite.updateFactory.EXPECT().
		AddUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		Recover(gomock.Any()).
		Return(yarpcerrors.NotFoundErrorf("update not found"))

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{}, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID).
		Return([]*peloton.UpdateID{}, nil)

	err := UpdateReload(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateComplete tests completing an update
func (suite *UpdateActionsTestSuite) TestUpdateComplete() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_SUCCEEDED,
			instancesTotal,
			[]uint32{}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(updateEntity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateCompleteMissingInCache tests completing an update which
// is not present in the cache
func (suite *UpdateActionsTestSuite) TestUpdateCompleteMissingInCache() {
	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(nil)

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateCompleteDBError tests failing to mark an update complete
func (suite *UpdateActionsTestSuite) TestUpdateCompleteDBError() {
	instancesTotal := []uint32{2, 3, 4, 5}

	suite.updateFactory.EXPECT().
		GetUpdate(suite.updateID).
		Return(suite.cachedUpdate)

	suite.cachedUpdate.EXPECT().
		GetGoalState().
		Return(&cached.UpdateStateVector{
			Instances: instancesTotal,
		})

	suite.cachedUpdate.EXPECT().
		WriteProgress(
			gomock.Any(),
			pbupdate.State_SUCCEEDED,
			instancesTotal,
			[]uint32{}).
		Return(fmt.Errorf("fake db error"))

	err := UpdateComplete(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateUntrackRuntimeGetFail tests failing to fetch job runtime
// while untracking an update
func (suite *UpdateActionsTestSuite) TestUpdateUntrackRuntimeGetFail() {
	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, fmt.Errorf("fake db error"))

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateUntrackRuntimeSetFail tests failing to write the job runtime
// to DB while untracking an update
func (suite *UpdateActionsTestSuite) TestUpdateUntrackRuntimeSetFail() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo,
			_ cached.UpdateRequest) {
			suite.Equal("", jobInfo.GetRuntime().GetUpdateID().GetValue())
		}).
		Return(fmt.Errorf("fake db error"))

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.EqualError(err, "fake db error")
}

// TestUpdateUntrack tests untracking an update successfully
func (suite *UpdateActionsTestSuite) TestUpdateUntrack() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_INITIALIZED,
	}

	prevUpdateIDs := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo,
			_ cached.UpdateRequest) {
			suite.Equal("", jobInfo.GetRuntime().GetUpdateID().GetValue())
		}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID).
		Return(prevUpdateIDs, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), gomock.Any()).
		Return(updateInfo, nil)
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateUntrack_TerminalJob tests untracking an update would untrack
// a terminated job
func (suite *UpdateActionsTestSuite) TestUpdateUntrack_TerminatedJob() {
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: suite.updateID,
		State:    pbjob.JobState_KILLED,
	}
	updateInfo := &models.UpdateModel{
		State: pbupdate.State_INITIALIZED,
	}

	prevUpdateIDs := []*peloton.UpdateID{
		{Value: uuid.NewRandom().String()},
		{Value: uuid.NewRandom().String()},
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.cachedJob.EXPECT().
		Update(gomock.Any(), gomock.Any(), cached.UpdateCacheAndDB).
		Do(func(_ context.Context, jobInfo *pbjob.JobInfo,
			_ cached.UpdateRequest) {
			suite.Equal("", jobInfo.GetRuntime().GetUpdateID().GetValue())
		}).
		Return(nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	suite.updateStore.EXPECT().
		GetUpdatesForJob(gomock.Any(), suite.jobID).
		Return(prevUpdateIDs, nil)

	suite.updateStore.EXPECT().
		GetUpdateProgress(gomock.Any(), gomock.Any()).
		Return(updateInfo, nil)
	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	suite.jobGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any())

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}

// TestUpdateUntrackNewUpdate tests untracking an update and job runtime
// has a new update to run
func (suite *UpdateActionsTestSuite) TestUpdateUntrackNewUpdate() {
	newID := &peloton.UpdateID{Value: uuid.NewRandom().String()}
	jobRuntime := &pbjob.RuntimeInfo{
		UpdateID: newID,
	}

	suite.jobFactory.EXPECT().
		AddJob(suite.jobID).
		Return(suite.cachedJob)

	suite.cachedJob.EXPECT().
		GetRuntime(gomock.Any()).
		Return(jobRuntime, nil)

	suite.updateGoalStateEngine.EXPECT().
		Delete(gomock.Any()).
		Do(func(updateEntity goalstate.Entity) {
			suite.Equal(suite.jobID.GetValue(), updateEntity.GetID())
		})

	suite.updateFactory.EXPECT().
		ClearUpdate(suite.updateID)

	suite.updateGoalStateEngine.EXPECT().
		Enqueue(gomock.Any(), gomock.Any()).
		Do(func(entity goalstate.Entity, deadline time.Time) {
			suite.Equal(suite.jobID.GetValue(), entity.GetID())
			updateEnt := entity.(*updateEntity)
			suite.Equal(newID.GetValue(), updateEnt.id.GetValue())
		})

	err := UpdateUntrack(context.Background(), suite.updateEnt)
	suite.NoError(err)
}
