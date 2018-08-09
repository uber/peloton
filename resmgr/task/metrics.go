package task

import (
	"strconv"
	"strings"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common/scalar"
	"code.uber.internal/infra/peloton/common/statemachine"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/util"

	"github.com/uber-go/tally"
)

// recorder is the interface for recording the observation
type recorder interface {
	Record(duration time.Duration)
}

// TransitionObserver is the interface for observing a state transition
type TransitionObserver interface {
	Observe(transitedTo statemachine.State)
}

// the key of the recorder of the form startState_endState
type recorderKey string

// timeRecord is the recorded time when the task transitioned to a state
type timeRecord struct {
	// the state when the time was recorded
	startState statemachine.State
	// the time when the task reached the state
	startTime time.Time
}

// TransObs implements TransitionObserver
type TransObs struct {
	// rules represents the transitions to record as map of start state
	// -> list of end states
	rules map[statemachine.State][]statemachine.State
	// map of in progress transitions which need to be recorded.
	// Its keyed by the end state which it is waiting on.
	inProgress map[statemachine.State][]timeRecord
	// recorders is a map of record-key -> recorder
	recorders map[recorderKey]recorder
}

// Option a func to mutate the observer
type Option func(obs *TransObs)

// WithTallyRecorder uses timer to record the transition
func WithTallyRecorder(scope tally.Scope) Option {
	return func(obs *TransObs) {
		recorders := make(map[recorderKey]recorder)
		for from, tos := range obs.rules {
			for _, to := range tos {
				tk := getRecorderKey(from, to)
				recorders[tk] = scope.Tagged(
					map[string]string{
						"states": string(tk)}).
					Timer("duration")
			}
		}
		obs.recorders = recorders
	}
}

// withLocalRecorder uses a local recorder, used for tests
func withLocalRecorder(r recorder) Option {
	return func(obs *TransObs) {
		recorders := make(map[recorderKey]recorder)
		for from, tos := range obs.rules {
			for _, to := range tos {
				tk := getRecorderKey(from, to)
				recorders[tk] = r
			}
		}
		obs.recorders = recorders
	}
}

// DefaultTransitionObserver returns the default observers for the respool and
// the task tagged with the relevant tags
func DefaultTransitionObserver(scope tally.Scope, t *resmgr.Task,
	respool respool.ResPool) TransitionObserver {
	return newTransitionObserverWithOptions(WithTallyRecorder(
		scope.SubScope("state_transition").Tagged(
			map[string]string{
				"respool_path": respool.GetPath(),
				"job_uuid_prefix": getJobUUIDPrefix(
					t.GetJobId().GetValue()),
				"instance_id": getInstanceID(t.GetTaskId().GetValue()),
				"run_id": getRunIDPrefix(
					t.GetTaskId().GetValue()),
			},
		),
	))
}

// newTransitionObserverWithOptions returns a new transition observer based on the scope.
func newTransitionObserverWithOptions(options ...Option) *TransObs {
	to := &TransObs{
		inProgress: make(map[statemachine.State][]timeRecord),
		recorders:  make(map[recorderKey]recorder),
		rules: map[statemachine.State][]statemachine.State{
			statemachine.State(
				task.TaskState_PENDING.String()): {
				// The time it takes for the task to go from being
				// added in the pending queue to being admitted in a resource
				// pool.
				statemachine.State(task.TaskState_READY.String()),
			},
			statemachine.State(
				task.TaskState_READY.String()): {
				// The time it takes for the task to go from being
				// admitted in the resource manager to actually running as
				// reported by Mesos.
				statemachine.State(task.TaskState_RUNNING.String()),
				// The time it takes for the task to go from being
				// admitted in the resource manager to being placed.
				statemachine.State(task.TaskState_PLACED.String()),
			},
			statemachine.State(
				task.TaskState_PREEMPTING.String()): {
				// The time it takes for the task to go from being
				// picked for preemption to being killed as reported by Mesos.
				statemachine.State(
					task.TaskState_KILLED.String())}},
	}

	for _, o := range options {
		o(to)
	}

	return to
}

// The first 8 chars of the UUID are used to control the cardinality of metrics
func getJobUUIDPrefix(uuid string) string {
	if len(uuid) >= util.UUIDLength {
		return uuid[0:8]
	}
	return ""
}

// The instance id of the task
func getInstanceID(mesosTaskID string) string {
	_, id, err := util.ParseJobAndInstanceID(mesosTaskID)
	if err != nil {
		return "-1"
	}
	return strconv.Itoa(id)
}

// TODO: Update this once mesos task id migration is complete from
// uuid-int-uuid -> uuid(job ID)-int(instance ID)-int(monotonically incremental)
func getRunIDPrefix(mesosTaskID string) string {
	if len(mesosTaskID) > 2*util.UUIDLength {
		// uuid-int-uuid
		// The first 8 chars of the UUID are used to control the cardinality of
		// metrics
		return mesosTaskID[39:47]
	}
	// uuid-int-int
	return mesosTaskID[strings.LastIndex(mesosTaskID, "-")+1:]
}

// Observe implements TransitionObserver
func (obs *TransObs) Observe(currentState statemachine.State) {
	// go through all the rules and see if currentState is the start state
	// for any of the rules.
	if endStates, ok := obs.rules[currentState]; ok {
		for _, endState := range endStates {
			obs.inProgress[endState] = append(obs.inProgress[endState],
				timeRecord{
					startState: currentState,
					startTime:  time.Now(),
				})
		}
	}

	// go through all the in progress transitions and see if any one is
	// waiting on the currentState, if so record it and remove it from the map.
	if inProgressRecords, ok := obs.inProgress[currentState]; ok {
		for _, inProgressRecord := range inProgressRecords {
			obs.recorders[getRecorderKey(
				inProgressRecord.startState, currentState)].Record(
				time.Now().Sub(inProgressRecord.startTime),
			)
		}
		delete(obs.inProgress, currentState)
	}
}

func getRecorderKey(from statemachine.State, to statemachine.State) recorderKey {
	return recorderKey(string(from) + "_" + string(to))
}

// Metrics is a placeholder for all metrics in task.
type Metrics struct {
	ReadyQueueLen tally.Gauge

	TasksCountInTracker tally.Gauge

	TaskStatesGauge map[task.TaskState]tally.Gauge

	LeakedResources scalar.GaugeMaps

	ReconciliationSuccess tally.Counter
	ReconciliationFail    tally.Counter
}

// NewMetrics returns a new instance of task.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	readyScope := scope.SubScope("ready")
	trackerScope := scope.SubScope("tracker")
	taskStateScope := scope.SubScope("tasks_state")

	reconcilerScope := scope.SubScope("reconciler")
	leakScope := reconcilerScope.SubScope("leaks")
	successScope := reconcilerScope.Tagged(map[string]string{"result": "success"})
	failScope := reconcilerScope.Tagged(map[string]string{"result": "fail"})

	return &Metrics{
		ReadyQueueLen:       readyScope.Gauge("ready_queue_length"),
		TasksCountInTracker: trackerScope.Gauge("task_len_tracker"),
		TaskStatesGauge: map[task.TaskState]tally.Gauge{
			task.TaskState_PENDING: taskStateScope.Gauge(
				"task_state_pending"),
			task.TaskState_READY: taskStateScope.Gauge(
				"task_state_ready"),
			task.TaskState_PLACING: taskStateScope.Gauge(
				"task_state_placing"),
			task.TaskState_PLACED: taskStateScope.Gauge(
				"task_state_placed"),
			task.TaskState_LAUNCHING: taskStateScope.Gauge(
				"task_state_launching"),
			task.TaskState_LAUNCHED: taskStateScope.Gauge(
				"task_state_launched"),
			task.TaskState_RUNNING: taskStateScope.Gauge(
				"task_state_running"),
			task.TaskState_SUCCEEDED: taskStateScope.Gauge(
				"task_state_succeeded"),
			task.TaskState_FAILED: taskStateScope.Gauge(
				"task_state_failed"),
			task.TaskState_KILLED: taskStateScope.Gauge(
				"task_state_pending"),
			task.TaskState_LOST: taskStateScope.Gauge(
				"task_state_lost"),
			task.TaskState_PREEMPTING: taskStateScope.Gauge(
				"task_state_preempting"),
		},
		LeakedResources:       scalar.NewGaugeMaps(leakScope),
		ReconciliationSuccess: successScope.Counter("run"),
		ReconciliationFail:    failScope.Counter("run"),
	}
}
