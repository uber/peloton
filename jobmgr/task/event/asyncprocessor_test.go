package event

import (
	"fmt"
	"sync"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"

	pb_eventstream "code.uber.internal/infra/peloton/.gen/peloton/private/eventstream"

	"code.uber.internal/infra/peloton/util"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

var uuidStr = uuid.NewUUID().String()

type mockTaskStore struct {
	mutex   *sync.Mutex
	updates map[string][]*task.TaskInfo
}

func (m *mockTaskStore) CreateTask(id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, createdBy string) error {
	return nil
}
func (m *mockTaskStore) CreateTasks(id *peloton.JobID, taskInfos []*task.TaskInfo, createdBy string) error {
	return nil
}
func (m *mockTaskStore) GetTasksForJob(id *peloton.JobID) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTasksForJobAndState(id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTasksForJobByRange(id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTaskForJob(id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) UpdateTask(taskInfo *task.TaskInfo) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	mesosTaskID := *(taskInfo.Runtime.TaskId.Value)
	_, ok := m.updates[mesosTaskID]
	if !ok {
		m.updates[mesosTaskID] = []*task.TaskInfo{}
	}
	m.updates[mesosTaskID] = append(m.updates[mesosTaskID], taskInfo)
	return nil
}
func (m *mockTaskStore) GetTaskByID(taskID string) (*task.TaskInfo, error) {
	jobID, instanceID, _ := util.ParseTaskID(taskID)
	mesosTaskID := taskID + "-" + uuidStr
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
		},
		InstanceId: uint32(instanceID),
		JobId:      &peloton.JobID{Value: jobID},
	}, nil
}

func TestBucketEventProcessor(t *testing.T) {
	store := &mockTaskStore{
		mutex:   &sync.Mutex{},
		updates: make(map[string][]*task.TaskInfo),
	}
	handler := &statusUpdate{
		taskStore: store,
	}
	var offset uint64
	applier := newBucketEventProcessor(handler, 15, 100)

	jobID := "Test"
	n := 243
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		offset++
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		offset++
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &mesosTaskID,
			},
			State: &state,
		}
		offset++
		applier.addEvent(&pb_eventstream.Event{
			Offset:          offset,
			MesosTaskStatus: status,
			Type:            pb_eventstream.Event_MESOS_TASK_STATUS,
		})
	}

	time.Sleep(50 * time.Millisecond)

	store.mutex.Lock()
	defer store.mutex.Unlock()
	for i := 0; i < n; i++ {
		mesosTaskID := fmt.Sprintf("%s-%d-%s", jobID, i, uuidStr)
		taskUpdates := store.updates[mesosTaskID]
		assert.Equal(t, taskUpdates[0].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_STARTING).String())
		assert.Equal(t, taskUpdates[1].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_RUNNING).String())
		assert.Equal(t, taskUpdates[2].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_FINISHED).String())
	}
	for _, bucket := range applier.eventBuckets {
		assert.True(t, bucket.getProcessedCount() > 0)
	}
	applier.shutdown()

}
func (m *mockTaskStore) QueryTasks(id *peloton.JobID, offset, limit uint32) ([]*task.TaskInfo, uint32, error) {
	return nil, 0, nil
}
