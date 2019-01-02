package task

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type fakeTimer struct {
	duration      time.Duration
	doneRecording chan struct{}
}

func (fr *fakeTimer) Record(duration time.Duration) { fr.duration = duration }

func (fr *fakeTimer) Start() tally.Stopwatch { return tally.Stopwatch{} }

var fr = &fakeTimer{
	doneRecording: make(chan struct{}),
}

func fakeTimerGenerator(_ tally.Scope) tally.Timer {
	return fr
}

func TestTransObs_Observe(t *testing.T) {
	tobs := newTransitionObserver(
		map[string]string{},
		tally.NoopScope,
		defaultRules,
		true,
	)
	tobs.timerGenerator = fakeTimerGenerator

	assert.Equal(t, fr.duration, time.Duration(0))

	tt := []struct {
		currentState   statemachine.State
		expectDuration bool
	}{
		{
			// This is starting state to record so the duration should be zero
			currentState:   statemachine.State(task.TaskState_READY.String()),
			expectDuration: false,
		},
		{
			// The rules don't include LAUNCHING to be recorded so it should
			// be zero
			currentState:   statemachine.State(task.TaskState_LAUNCHING.String()),
			expectDuration: false,
		},
		{
			// This is the end state to record so the duration > 0
			currentState:   statemachine.State(task.TaskState_RUNNING.String()),
			expectDuration: true,
		},
	}

	for _, test := range tt {
		tobs.Observe(test.currentState)
		if test.expectDuration {
			assert.NotEqual(t, fr.duration, time.Duration(0),
				"duration should be greater than zero")
		} else {
			assert.Equal(t, fr.duration, time.Duration(0),
				"duration should be equal to zero")
		}
	}
}

func TestNewTransitionObserver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResPool := mocks.NewMockResPool(ctrl)
	mockResPool.EXPECT().GetPath().Return("/").Times(1)

	dto := NewTransitionObserver(true, tally.NoopScope, mockResPool.GetPath())
	assert.NotNil(t, dto)
}
