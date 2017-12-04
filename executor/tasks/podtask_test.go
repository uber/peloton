package task_test

import (
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	task "code.uber.internal/infra/peloton/executor/tasks"
	"code.uber.internal/infra/peloton/util"
	"github.com/stretchr/testify/require"
)

var (
	TestTaskID = &mesos.TaskID{
		Value: util.PtrPrintf("TEST_TASK_ID"),
	}
)

// TODO: mock the mesos clients once the podtask starts doing things

func TestNewPodTask(t *testing.T) {
	info := mesos.TaskInfo{TaskId: TestTaskID}
	// Makes sure the default printer does not panic
	task.NewPodTask(info, nil)
}

func TestLaunchChangesState(t *testing.T) {
	info := mesos.TaskInfo{TaskId: TestTaskID}
	task := task.NewPodTask(info, nil)
	task.Sleeper = func(time.Duration) {}

	states, err := task.AddWatcher("test")
	require.NoError(t, err)
	go func() {
		err := task.Launch()
		require.NoError(t, err)
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for state change")
	case state := <-states:
		require.Equal(t, mesos.TaskState_TASK_STARTING, state)
	}

	select {
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for state change")
	case state := <-states:
		require.Equal(t, mesos.TaskState_TASK_RUNNING, state)
	}
}

func TestKillChangesState(t *testing.T) {
	info := mesos.TaskInfo{TaskId: TestTaskID}
	task := task.NewPodTask(info, nil)
	task.Sleeper = func(time.Duration) {}

	states, err := task.AddWatcher("test")
	require.NoError(t, err)
	go func() {
		err := task.Kill()
		require.NoError(t, err)
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for state change")
	case state := <-states:
		require.Equal(t, mesos.TaskState_TASK_KILLING, state)
	}

	select {
	case <-time.After(100 * time.Millisecond):
		require.FailNow(t, "Timed out waiting for state change")
	case state := <-states:
		require.Equal(t, mesos.TaskState_TASK_KILLED, state)
	}
}

func TestWatcherLag(t *testing.T) {
	info := mesos.TaskInfo{TaskId: TestTaskID}
	channelsize := task.StateChannelSize
	task := task.NewPodTask(info, nil)
	task.Sleeper = func(time.Duration) {}

	states, err := task.AddWatcher("test")
	require.NoError(t, err)
	for i := 0; i < 2*channelsize; i++ {
		err = task.Launch()
		require.NoError(t, err)
		err = task.Kill()
		require.NoError(t, err)
	}

	count := 0
	for _ = range states {
		count++
	}
	require.Equal(t, channelsize, count)
}
