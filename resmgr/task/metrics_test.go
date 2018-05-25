package task

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

	"code.uber.internal/infra/peloton/common/statemachine"

	"github.com/stretchr/testify/assert"
)

type fakeRecorder struct {
	duration      time.Duration
	doneRecording chan struct{}
}

func (fr *fakeRecorder) Record(duration time.Duration) {
	fr.duration = duration
}

func TestTransObs_Observe(t *testing.T) {
	fr := &fakeRecorder{
		doneRecording: make(chan struct{}),
	}
	tobs := NewTransitionObserver(withLocalRecorder(fr))

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
