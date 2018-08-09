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
	tobs := newTransitionObserverWithOptions(withLocalRecorder(fr))

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

// Test parsing the first 8 characters from a job uuid
func TestParseJobUUIDPrefix(t *testing.T) {
	tt := []struct {
		uuid       string
		firstEight string
	}{
		{
			uuid:       "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99",
			firstEight: "46014e1e",
		},
		{
			uuid:       "",
			firstEight: "",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.firstEight, getJobUUIDPrefix(test.uuid))
	}
}

// Tests parsing the runid from the mesos task ID
func TestParseRunIDPrefix(t *testing.T) {
	tt := []struct {
		uuid  string
		runID string
	}{
		{
			uuid:  "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-1",
			runID: "1",
		},
		{
			uuid:  "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-46014e1e-4aa1-4fcc-b22b-e1d58f02ba99",
			runID: "46014e1e",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.runID, getRunIDPrefix(test.uuid))
	}
}

func TestParseInstanceID(t *testing.T) {
	tt := []struct {
		uuid       string
		instanceID string
	}{
		{
			uuid:       "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-1",
			instanceID: "0",
		},
		{
			uuid: "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-1-46014e1e-4aa1" +
				"-4fcc-b22b-e1d58f02ba99",
			instanceID: "1",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.instanceID, getInstanceID(test.uuid))
	}
}
