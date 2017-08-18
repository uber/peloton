package task

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	resp "code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/eventstream"
	"code.uber.internal/infra/peloton/resmgr/respool"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ReconcilerTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	reconciler         *reconciler
	tracker            Tracker
	mockTaskStore      *store_mocks.MockTaskStore
	eventStreamHandler *eventstream.Handler
	task               *resmgr.Task
	respool            respool.ResPool
	hostname           string
}

func (suite *ReconcilerTestSuite) SetupTest() {
	InitTaskTracker(tally.NoopScope)
	suite.tracker = GetTracker()

	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockTaskStore = store_mocks.NewMockTaskStore(suite.mockCtrl)

	suite.reconciler = &reconciler{
		runningState:         runningStateNotStarted,
		tracker:              suite.tracker,
		taskStore:            suite.mockTaskStore,
		reconciliationPeriod: time.Hour * 1,
		stopChan:             make(chan struct{}, 1),
		metrics:              NewMetrics(tally.NoopScope),
	}

	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.hostname = "hostname"
}

func (suite *ReconcilerTestSuite) addTaskToTracker(pelotonTaskID string, trackerMesosTaskID string) {
	rmTask := suite.createTask(pelotonTaskID, trackerMesosTaskID)
	rootID := peloton.ResourcePoolID{Value: respool.RootResPoolID}
	policy := resp.SchedulingPolicy_PriorityFIFO
	respoolConfig := &resp.ResourcePoolConfig{
		Name:      "respool-1",
		Parent:    &rootID,
		Resources: suite.getResourceConfig(),
		Policy:    policy,
	}
	suite.respool, _ = respool.NewRespool(tally.NoopScope, "respool-1", nil, respoolConfig)
	suite.tracker.AddTask(rmTask, suite.eventStreamHandler, suite.respool, &Config{})
}

// Returns resource configs
func (suite *ReconcilerTestSuite) getResourceConfig() []*resp.ResourceConfig {

	resConfigs := []*resp.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 1000,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

func (suite *ReconcilerTestSuite) createTask(pelotonTaskID string, mesosTaskID string) *resmgr.Task {
	return &resmgr.Task{
		Name:     pelotonTaskID,
		Priority: 0,
		JobId:    &peloton.JobID{Value: "job1"},
		Id:       &peloton.TaskID{Value: pelotonTaskID},
		TaskId: &mesos_v1.TaskID{
			Value: &mesosTaskID,
		},
		Hostname: suite.hostname,
		Resource: &task.ResourceConfig{
			CpuLimit:    1,
			DiskLimitMb: 10,
			GpuLimit:    0,
			MemLimitMb:  100,
		},
	}
}

func TestReconciler(t *testing.T) {
	suite.Run(t, new(ReconcilerTestSuite))
}

func (suite *ReconcilerTestSuite) TestReconcile() {
	pelotonTaskID := "testPelotonTaskID"
	mesosTaskID := "testMesosTaskID"

	reconcilerTests := []struct {
		dbMesosTaskID            string
		trackerMesosTaskID       string
		pelotonTaskID            string
		stateInDB                task.TaskState
		stateInTracker           task.TaskState
		expectedTasksToReconcile map[string]string
		expectedError            error
	}{
		{
			// case 1: The task in the DB has finished but not in the tracker
			dbMesosTaskID:      mesosTaskID,
			trackerMesosTaskID: mesosTaskID,
			pelotonTaskID:      pelotonTaskID,
			stateInDB:          task.TaskState_FAILED,
			stateInTracker:     task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{
				pelotonTaskID: mesosTaskID,
			},
			expectedError: nil,
		},
		{
			// case 2: The task in the DB and tracker match
			dbMesosTaskID:            mesosTaskID,
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError:            nil,
		},
		{
			// case 3: The task in the DB and tracker have different mesos ID's
			dbMesosTaskID:            "differentFromTracker",
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError:            nil,
		},
		{
			// case 4: Error reading from db
			dbMesosTaskID:            "",
			trackerMesosTaskID:       mesosTaskID,
			pelotonTaskID:            pelotonTaskID,
			stateInDB:                task.TaskState_INITIALIZED,
			stateInTracker:           task.TaskState_INITIALIZED,
			expectedTasksToReconcile: map[string]string{},
			expectedError: errors.Errorf("1 error occurred:\n\n* unable to get "+
				"task:%s from the database", pelotonTaskID),
		},
	}

	for _, tt := range reconcilerTests {
		gomock.InOrder(
			suite.mockTaskStore.EXPECT().GetTaskByID(context.Background(), tt.pelotonTaskID).Return(
				&task.TaskInfo{
					Runtime: &task.RuntimeInfo{
						State: tt.stateInDB,
						MesosTaskId: &mesos_v1.TaskID{
							Value: &tt.dbMesosTaskID,
						},
					},
				},
				tt.expectedError))
		suite.addTaskToTracker(tt.pelotonTaskID, tt.trackerMesosTaskID)

		actualTasksToReconcile, err := suite.reconciler.getTasksToReconcile()
		if tt.expectedError == nil {
			suite.NoError(err)
		} else {
			suite.Equal(tt.expectedError.Error(), err.Error())
		}

		suite.Equal(tt.expectedTasksToReconcile, actualTasksToReconcile)
	}
}

func (suite *ReconcilerTestSuite) TearDownTest() {
	suite.tracker.Clear()
	suite.mockCtrl.Finish()
}

func (suite *ReconcilerTestSuite) TestReconciler_Start() {
	defer suite.reconciler.Stop()
	err := suite.reconciler.Start()
	suite.NoError(err)
	suite.Equal(suite.reconciler.runningState, int32(runningStateRunning))
}
