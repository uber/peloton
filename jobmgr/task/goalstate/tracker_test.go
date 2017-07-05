package goalstate

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
)

var (
	_testJobID = uuid.NewUUID().String()
)

type TrackerTestSuite struct {
	suite.Suite

	mockCtrl *gomock.Controller
	tracker  Tracker
}

func (suite *TrackerTestSuite) SetupTest() {
	suite.tracker = NewTracker()
}

func TestTrackerTestSuite(t *testing.T) {
	suite.Run(t, new(TrackerTestSuite))
}

func (suite *TrackerTestSuite) TestTrackerTasks() {
	testTaskInfo := createTaskInfo(0, task.TaskState_RUNNING)
	pelotonTaskID := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", _testJobID, 0),
	}

	err := suite.tracker.AddTask(testTaskInfo)
	suite.NoError(err)

	taskInfo := suite.tracker.GetTask(pelotonTaskID)
	suite.NotNil(taskInfo)

	err = suite.tracker.DeleteTask(pelotonTaskID)
	suite.NoError(err)

	taskInfo = suite.tracker.GetTask(pelotonTaskID)
	suite.Nil(taskInfo)
}

func createTaskInfo(i uint32, state task.TaskState) *task.TaskInfo {
	var tID = fmt.Sprintf("%s-%d-%s", _testJobID, i, uuid.NewUUID().String())
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &tID},
			State:       state,
		},
		Config: &task.TaskConfig{
			Name: tID,
		},
		InstanceId: uint32(i),
		JobId: &peloton.JobID{
			Value: _testJobID,
		},
	}
	return &taskInfo
}