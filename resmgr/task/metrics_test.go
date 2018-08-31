package task

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

type fakeRecorder struct {
	duration      time.Duration
	doneRecording chan struct{}
}

func (fr *fakeRecorder) Record(duration time.Duration) {
	fr.duration = duration
}

var fr = &fakeRecorder{
	doneRecording: make(chan struct{}),
}

// returns a fake recorder for the key
func newFakeRecorder(_ tally.Scope) recorder {
	return fr
}

func TestTransObs_Observe(t *testing.T) {
	tobs := newTransitionObserver(
		map[string]string{},
		tally.NoopScope,
		defaultRules,
	)
	tobs.recorderGenerator = newFakeRecorder

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

func TestNewTimerRecorder(t *testing.T) {
	tr := timerRecorder(tally.NoopScope)
	assert.NotNil(t, tr)
}

func TestNewDefaultTransitionObserver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockResPool := mocks.NewMockResPool(ctrl)
	mockResPool.EXPECT().GetPath().Return("/").Times(1)

	tid := "abcdef12-abcd-1234-5678-1234567890ab-0-abcdef12-abcd-1234-5678" +
		"-1234567890ab"
	jid := "abcdef12-abcd-1234-5678-1234567890ab"
	fakeTask := &resmgr.Task{
		JobId: &peloton.JobID{
			Value: jid,
		},
		TaskId: &mesos_v1.TaskID{
			Value: &tid,
		},
	}

	dto := DefaultTransitionObserver(tally.NoopScope, fakeTask, mockResPool)
	assert.NotNil(t, dto)
}

// Test parsing the first 8 characters from a job uuid
func TestParseJobUUIDPrefix(t *testing.T) {
	tt := []struct {
		name       string
		uuid       string
		firstEight string
	}{
		{
			name:       "parse uuid prefix successful",
			uuid:       "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99",
			firstEight: "46014e1e",
		},
		{
			name:       "parse empty uuid prefix successful",
			uuid:       "",
			firstEight: "",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.firstEight, getJobUUIDPrefix(test.uuid), test.name)
	}
}

// Tests parsing the runid from the mesos task ID
func TestParseRunIDPrefix(t *testing.T) {
	tt := []struct {
		name  string
		uuid  string
		runID string
	}{
		{
			name:  "parse runid seq successful",
			uuid:  "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-1",
			runID: "1",
		},
		{
			name:  "parse runid uuid successful",
			uuid:  "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-46014e1e-4aa1-4fcc-b22b-e1d58f02ba99",
			runID: "46014e1e",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.runID, getRunIDPrefix(test.uuid), test.name)
	}
}

func TestParseInstanceID(t *testing.T) {
	tt := []struct {
		name       string
		uuid       string
		instanceID string
	}{
		{
			name:       "parse instance id successful",
			uuid:       "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-0-1",
			instanceID: "0",
		},
		{
			name: "parse instance id successful with uuid runid",
			uuid: "46014e1e-4aa1-4fcc-b22b-e1d58f02ba99-1-46014e1e-4aa1" +
				"-4fcc-b22b-e1d58f02ba99",
			instanceID: "1",
		},
		{
			name:       "parse instance id returns -1 with invalid uuid",
			uuid:       "xyz",
			instanceID: "-1",
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.instanceID, getInstanceID(test.uuid), test.name)
	}
}
