package upgrade

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/jobmgr/tracked"
	"code.uber.internal/infra/peloton/jobmgr/tracked/mocks"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStartStopManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	upgradeStoreMock := store_mocks.NewMockUpgradeStore(ctrl)

	m := NewManager(nil, nil, nil, upgradeStoreMock, Config{})

	var wg sync.WaitGroup
	wg.Add(1)

	upgradeStoreMock.EXPECT().GetUpgrades(context.Background()).
		Do(func(_ interface{}) {
			wg.Done()
		}).
		Return(nil, nil)

	assert.NoError(t, m.Start())
	wg.Wait()
	assert.NoError(t, m.Stop())
}

func TestManagerProcessUpgradeStateSucceeded(t *testing.T) {
	m := &manager{
		upgrades: map[string]*upgradeState{},
	}

	id := &peloton.UpgradeID{Value: uuid.New()}

	us := &upgradeState{
		status: &upgrade.Status{
			Id:    id,
			State: upgrade.State_SUCCEEDED,
		},
	}

	m.upgrades[id.Value] = us

	assert.NoError(t, m.processUpgrade(context.Background(), us))
	assert.Empty(t, m.upgrades)
}

func TestManagerProcessUpgradeStateRollingForward(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	trackedManagerMock := mocks.NewMockManager(ctrl)
	upgradeStoreMock := store_mocks.NewMockUpgradeStore(ctrl)
	jobMock := mocks.NewMockJob(ctrl)
	taskMock := mocks.NewMockTask(ctrl)

	m := &manager{
		trackedManager: trackedManagerMock,
		upgradeStore:   upgradeStoreMock,
	}

	id := &peloton.UpgradeID{Value: uuid.New()}
	jobID := &peloton.JobID{Value: uuid.New()}

	us := &upgradeState{
		status: &upgrade.Status{
			Id:    id,
			JobId: jobID,
			State: upgrade.State_ROLLING_FORWARD,
		},
		options:           &upgrade.Options{},
		instanceCount:     3,
		fromConfigVersion: 5,
		toConfigVersion:   8,
	}

	// No tracked job returns error.
	trackedManagerMock.EXPECT().GetJob(jobID).Return(nil)

	assert.EqualError(t, m.processUpgrade(context.Background(), us), "cannot upgrade untracked job")

	// No tracked job returns error.
	trackedManagerMock.EXPECT().GetJob(jobID).Return(jobMock)

	// Process done task.
	jobMock.EXPECT().GetTask(uint32(0)).Return(taskMock)
	taskMock.EXPECT().CurrentState().Return(tracked.State{ConfigVersion: 8, State: task.TaskState_RUNNING})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{ConfigVersion: 8, State: task.TaskGoalState_RUN})

	// Process missing task.
	jobMock.EXPECT().GetTask(uint32(1)).Return(nil)

	// Process upgrading task.
	jobMock.EXPECT().GetTask(uint32(2)).Return(taskMock)
	taskMock.EXPECT().CurrentState().Return(tracked.State{ConfigVersion: 7, State: task.TaskState_RUNNING})
	taskMock.EXPECT().GoalState().Return(tracked.GoalState{ConfigVersion: 8, State: task.TaskGoalState_RUN})

	upgradeStoreMock.EXPECT().
		UpdateUpgradeStatus(context.Background(), id, &upgrade.Status{
			Id:           id,
			JobId:        jobID,
			State:        upgrade.State_ROLLING_FORWARD,
			NumTasksDone: 1,
		}).
		Return(nil)

	assert.NoError(t, m.processUpgrade(context.Background(), us))
}

func TestManagerSetGoalStateVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	trackedManagerMock := mocks.NewMockManager(ctrl)

	m := &manager{
		trackedManager: trackedManagerMock,
	}

	jobID := &peloton.JobID{Value: uuid.New()}

	us := &upgradeState{
		status: &upgrade.Status{
			JobId: jobID,
		},
		toConfigVersion: 42,
	}

	// Read error return immediatly.
	trackedManagerMock.EXPECT().GetTaskRuntime(context.Background(), jobID, uint32(7)).
		Return(nil, fmt.Errorf("read error"))

	assert.EqualError(t, m.setGoalStateVersion(context.Background(), us, 7), "read error")

	// Write error returns the error.
	trackedManagerMock.EXPECT().GetTaskRuntime(context.Background(), jobID, uint32(7)).
		Return(&task.RuntimeInfo{}, nil)

	trackedManagerMock.EXPECT().
		UpdateTaskRuntime(context.Background(), jobID, uint32(7), &task.RuntimeInfo{
			DesiredConfigVersion: 42,
		}).
		Return(fmt.Errorf("write error"))

	assert.EqualError(t, m.setGoalStateVersion(context.Background(), us, 7), "write error")

	trackedManagerMock.EXPECT().GetTaskRuntime(context.Background(), jobID, uint32(7)).
		Return(&task.RuntimeInfo{}, nil)

	trackedManagerMock.EXPECT().
		UpdateTaskRuntime(context.Background(), jobID, uint32(7), &task.RuntimeInfo{
			DesiredConfigVersion: 42,
		}).
		Return(nil)

	assert.NoError(t, m.setGoalStateVersion(context.Background(), us, 7))
}

func TestManagerUpdateUpgradeStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	upgradeStoreMock := store_mocks.NewMockUpgradeStore(ctrl)

	m := &manager{
		upgradeStore: upgradeStoreMock,
	}

	id := &peloton.UpgradeID{Value: uuid.New()}

	us := &upgradeState{
		status: &upgrade.Status{
			Id:    id,
			State: upgrade.State_ROLLING_FORWARD,
		},
	}

	// Store error should not change the upgradeState.
	upgradeStoreMock.EXPECT().UpdateUpgradeStatus(context.Background(), id, &upgrade.Status{
		Id:           id,
		State:        upgrade.State_SUCCEEDED,
		NumTasksDone: 1,
	}).Return(fmt.Errorf("store error"))

	assert.EqualError(t, m.updateUpgradeStatus(context.Background(), us, upgrade.State_SUCCEEDED, 1), "store error")
	assert.Equal(t, upgrade.State_ROLLING_FORWARD, us.status.State)
	assert.Equal(t, uint32(0), us.status.NumTasksDone)

	// Successful store updates upgradeState.
	upgradeStoreMock.EXPECT().UpdateUpgradeStatus(context.Background(), id, &upgrade.Status{
		Id:           id,
		State:        upgrade.State_SUCCEEDED,
		NumTasksDone: 2,
	}).Return(nil)

	assert.NoError(t, m.updateUpgradeStatus(context.Background(), us, upgrade.State_SUCCEEDED, 2))
	assert.Equal(t, upgrade.State_SUCCEEDED, us.status.State)
	assert.Equal(t, uint32(2), us.status.NumTasksDone)
}

func TestManagerEnsureOptionsAreLoaded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	jobStoreMock := store_mocks.NewMockJobStore(ctrl)
	upgradeStoreMock := store_mocks.NewMockUpgradeStore(ctrl)

	m := &manager{
		jobStore:     jobStoreMock,
		upgradeStore: upgradeStoreMock,
	}

	id := &peloton.UpgradeID{Value: uuid.New()}
	jobID := &peloton.JobID{Value: uuid.New()}

	us := &upgradeState{
		status: &upgrade.Status{
			Id:    id,
			JobId: jobID,
		},
	}

	upgradeStoreMock.EXPECT().GetUpgradeOptions(context.Background(), id).
		Return(&upgrade.Options{}, uint64(1), uint64(2), nil)

	jobStoreMock.EXPECT().GetJobConfig(context.Background(), jobID, uint64(2)).
		Return(&job.JobConfig{InstanceCount: 42}, nil)

	assert.NoError(t, m.ensureOptionsAreLoaded(context.Background(), us))
	assert.Equal(t, uint32(42), us.instanceCount)
	assert.Equal(t, uint64(1), us.fromConfigVersion)
	assert.Equal(t, uint64(2), us.toConfigVersion)

	// Next call should be no-op.
	assert.NoError(t, m.ensureOptionsAreLoaded(context.Background(), us))
}
