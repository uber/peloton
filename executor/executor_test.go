package executor

import (
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
// - Check that a shutdown message makes "wait" return
// - Check that the ack of an old update makes the executor
// re-update with the latest status of the task
// - Check that an ack for an unknown task ID does not break anything
// - Check that launch calls launch on the task
// - Check that kill calls kill on the task
// - Check that a kill before a launch does nothing

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
}
