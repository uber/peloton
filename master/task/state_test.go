package task

import (
	"code.uber.internal/infra/peloton/master/config"
	"code.uber.internal/infra/peloton/util"
	"fmt"
	"github.com/stretchr/testify/assert"
	mesos "mesos/v1"
	"peloton/job"
	"peloton/task"
	"sync"
	"testing"
	"time"
)

type mockTaskStore struct {
	mutex   *sync.Mutex
	updates map[string][]*task.TaskInfo
}

func (m *mockTaskStore) CreateTask(id *job.JobID, instanceID uint32, taskInfo *task.TaskInfo, createdBy string) error {
	return nil
}
func (m *mockTaskStore) CreateTasks(id *job.JobID, taskInfos []*task.TaskInfo, createdBy string) error {
	return nil
}
func (m *mockTaskStore) GetTasksForJob(id *job.JobID) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTasksForJobAndState(id *job.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTasksForJobByRange(id *job.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) GetTaskForJob(id *job.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	return nil, nil
}
func (m *mockTaskStore) UpdateTask(taskInfo *task.TaskInfo) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	taskID := *(taskInfo.Runtime.TaskId.Value)
	_, ok := m.updates[taskID]
	if !ok {
		m.updates[taskID] = []*task.TaskInfo{}
	}
	m.updates[taskID] = append(m.updates[taskID], taskInfo)
	return nil
}
func (m *mockTaskStore) GetTaskByID(taskID string) (*task.TaskInfo, error) {
	jobID, instanceID, _ := util.ParseTaskID(taskID)
	return &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{
				Value: &taskID,
			},
		},
		InstanceId: uint32(instanceID),
		JobId:      &job.JobID{Value: jobID},
	}, nil
}

func TestTaskStateUpdateApplier(t *testing.T) {
	store := &mockTaskStore{
		mutex:   &sync.Mutex{},
		updates: make(map[string][]*task.TaskInfo),
	}
	var dBWrittenCount int64
	handler := &taskStateManager{
		TaskStore: store,
		JobStore:  nil,
		client:    nil,
		config: &config.MasterConfig{
			TaskUpdateAckConcurrency: 0,
		},
		dBWrittenCount: &dBWrittenCount,
	}

	applier := newTaskStateUpdateApplier(handler, 15, 100)

	jobID := "Test"
	n := 243

	for i := 0; i < n; i++ {
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		state := mesos.TaskState_TASK_STARTING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &taskID,
			},
			State: &state,
		}
		applier.addTaskStatus(status)
	}
	for i := 0; i < n; i++ {
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		state := mesos.TaskState_TASK_RUNNING
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &taskID,
			},
			State: &state,
		}
		applier.addTaskStatus(status)
	}
	for i := 0; i < n; i++ {
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		state := mesos.TaskState_TASK_FINISHED
		status := &mesos.TaskStatus{
			TaskId: &mesos.TaskID{
				Value: &taskID,
			},
			State: &state,
		}
		applier.addTaskStatus(status)
	}

	time.Sleep(50 * time.Millisecond)

	store.mutex.Lock()
	defer store.mutex.Unlock()
	for i := 0; i < n; i++ {
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		taskUpdates := store.updates[taskID]
		assert.Equal(t, taskUpdates[0].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_STARTING).String())
		assert.Equal(t, taskUpdates[1].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_RUNNING).String())
		assert.Equal(t, taskUpdates[2].Runtime.State.String(), util.MesosStateToPelotonState(mesos.TaskState_TASK_FINISHED).String())
	}
	for _, bucket := range applier.statusBuckets {
		assert.True(t, bucket.getProcessedCount() > 0)
	}
	applier.shutdown()

}
