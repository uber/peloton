package executor

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
	"code.uber.internal/infra/peloton/executor/mocks"
)

type callMatcher struct {
	CallType executor.Call_Type
}

func (matcher callMatcher) Matches(x interface{}) bool {
	casted, ok := x.(*executor.Call)
	if !ok || casted == nil {
		return false
	}
	return casted.GetType() == matcher.CallType
}

func (matcher callMatcher) String() string {
	return string(matcher.CallType)
}

// TODO(pourchet): Add the following tests
// - Check that the ack of an old update makes the executor
// re-update with the latest status of the task

func TestSubscribeOnStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
}

func TestResubscribeOnReconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	exec.Sleeper = func(_ time.Duration) {}
	err := exec.Start()
	require.NoError(t, err)

	exec.Disconnected()
	require.False(t, exec.IsConnected())
}

func TestExecutorConnected(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	exec.Sleeper = func(_ time.Duration) {}
	err := exec.Start()
	require.NoError(t, err)

	exec.Connected()
	require.True(t, exec.IsConnected())
}

func TestWaitReturnsOnShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	go exec.Shutdown()
	err = exec.Wait()
	require.NoError(t, err)
}

func TestWaitReturnsFatalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	go exec.Fatal(fmt.Errorf("THIS IS A FATAL ERROR"))
	err = exec.Wait()
	require.Error(t, err)
}

func TestAckUnknownID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	tid := "UNKNOWN"
	ack := &executor.Event_Acknowledged{
		TaskId: &mesos.TaskID{
			Value: &tid,
		},
	}
	err = exec.Acknowledged(ack)
	require.Error(t, err)
}

func TestKillUnknownTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		return mocks.NewMockTask(ctrl)
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	tid := "UNKNOWN"
	kill := &executor.Event_Kill{
		TaskId: &mesos.TaskID{
			Value: &tid,
		},
	}
	err = exec.Kill(kill)
	require.Error(t, err)
}

func TestLaunchTaskCallsTaskLaunch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		task := mocks.NewMockTask(ctrl)
		task.EXPECT().GetInfo()
		task.EXPECT().AddWatcher("executor_main")
		task.EXPECT().GetState()
		task.EXPECT().Launch().Return(nil)
		task.EXPECT().Monitor(true)
		return task
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	launch := &executor.Event_Launch{}
	err = exec.Launch(launch)
	require.NoError(t, err)
}

func TestKillTaskCallsTaskKill(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)

	factory := func(taskinfo *mesos.TaskInfo) Task {
		task := mocks.NewMockTask(ctrl)
		task.EXPECT().GetInfo()
		task.EXPECT().AddWatcher("executor_main")
		task.EXPECT().GetState()
		task.EXPECT().Launch().Return(nil)
		task.EXPECT().Monitor(true)
		task.EXPECT().Kill()
		return task
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)
	launch := &executor.Event_Launch{}
	ctype := executor.Event_LAUNCH
	err = exec.HandleEvent(&executor.Event{Type: &ctype, Launch: launch})
	require.NoError(t, err)
	kill := &executor.Event_Kill{}
	ctype = executor.Event_KILL
	err = exec.HandleEvent(&executor.Event{Type: &ctype, Kill: kill})
	require.NoError(t, err)
}

func TestFullUpdateLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mocks.NewMockMesosClient(ctrl)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_SUBSCRIBE,
	}).Return(nil)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_UPDATE,
	}).Return(nil)
	client.EXPECT().ExecutorCall(callMatcher{
		CallType: executor.Call_UPDATE,
	}).Return(nil)

	statechan := make(chan mesos.TaskState, 0)
	factory := func(taskinfo *mesos.TaskInfo) Task {
		task := mocks.NewMockTask(ctrl)
		task.EXPECT().GetInfo().Return(taskinfo)
		task.EXPECT().AddWatcher("executor_main").Return(statechan, nil)
		task.EXPECT().GetState()
		task.EXPECT().Launch().Return(nil)
		task.EXPECT().Monitor(true)
		task.EXPECT().IsHealthy().Return(true)
		task.EXPECT().GetInfo().Return(taskinfo)
		task.EXPECT().GetInfo().Return(taskinfo)
		return task
	}

	exec := NewExecutor(factory, client)
	err := exec.Start()
	require.NoError(t, err)

	subscribed := &executor.Event_Subscribed{
		ExecutorInfo: &mesos.ExecutorInfo{},
	}
	err = exec.Subscribed(subscribed)
	require.NoError(t, err)

	tid := "TASKID"
	launch := &executor.Event_Launch{
		Task: &mesos.TaskInfo{
			TaskId: &mesos.TaskID{
				Value: &tid,
			},
		},
	}
	ctype := executor.Event_LAUNCH
	err = exec.HandleEvent(&executor.Event{
		Type:   &ctype,
		Launch: launch,
	})
	require.NoError(t, err)
	statechan <- mesos.TaskState_TASK_STARTING
	statechan <- mesos.TaskState_TASK_RUNNING
	time.Sleep(10 * time.Millisecond)

	ack := &executor.Event_Acknowledged{
		TaskId: &mesos.TaskID{
			Value: &tid,
		},
	}
	err = exec.Acknowledged(ack)
	require.NoError(t, err)
}
