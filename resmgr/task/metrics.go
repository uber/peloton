package task

import (
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/common/scalar"
	"code.uber.internal/infra/peloton/common/statemachine"
	"github.com/uber-go/tally"
)

// tags for state transition metrics
const (
	_respoolPath = "respool_path"
	_states      = "states"
)

// TransitionObserver is the interface for observing a state transition
type TransitionObserver interface {
	Observe(transitedTo statemachine.State)
}

// the key of the timer of the form startState_endState Eg PENDING_READY
type timerKey string

// timeRecord is the recorded time when the task transitioned to a state
type timeRecord struct {
	// the state when the time was recorded
	startState statemachine.State
	// the time when the task reached the state
	startTime time.Time
}

// TransObs implements TransitionObserver
type TransObs struct {
	// boolean to enable/disable the observer
	enabled bool

	// rules represents the transitions to record as map of start state
	// -> list of end states
	rules map[statemachine.State][]statemachine.State

	// map of in progress transitions which need to be recorded.
	// Its keyed by the end state which it is waiting on.
	inProgress map[statemachine.State][]timeRecord

	// generator for timer
	timerGenerator timerGenerator
	// timers is a map of record-key -> timers
	timers map[timerKey]tally.Timer

	// the metrics scope
	scope tally.Scope
	// the tags for the metrics which represent the task uniquely
	tags map[string]string
}

// represents the states for which the duration needs to be recorded
var defaultRules = map[statemachine.State][]statemachine.State{
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
			task.TaskState_KILLED.String())}}

// NewTransitionObserver returns the a new observer for the respool and
// the task tagged with the relevant tags
func NewTransitionObserver(
	enabled bool,
	scope tally.Scope,
	respoolPath string,
) TransitionObserver {

	tags := make(map[string]string)
	if enabled {
		// only get the tags only if the observer is enabled otherwise
		// there's no point
		tags = map[string]string{
			_respoolPath: respoolPath,
		}
	}

	return newTransitionObserver(tags, scope, defaultRules, enabled)
}

// newTransitionObserver returns a new transition observer based on
// the tags, scope and the rules.
func newTransitionObserver(
	tags map[string]string,
	scope tally.Scope,
	rules map[statemachine.State][]statemachine.State,
	enabled bool) *TransObs {

	return &TransObs{
		inProgress:     make(map[statemachine.State][]timeRecord),
		timers:         make(map[timerKey]tally.Timer),
		rules:          rules,
		tags:           tags,
		scope:          scope.SubScope("state_transition"),
		timerGenerator: tallyTimerGenerator,
		enabled:        enabled,
	}
}

// Observe implements TransitionObserver
func (obs *TransObs) Observe(currentState statemachine.State) {
	if !obs.enabled {
		return
	}

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
	// waiting on the current_state, if so record it and remove it from the map.
	if inProgressRecords, ok := obs.inProgress[currentState]; ok {
		for _, inProgressRecord := range inProgressRecords {
			key := getRecorderKey(
				inProgressRecord.startState, currentState)

			timer, ok := obs.timers[key]
			if !ok {
				// lazily instantiate the recorder
				obs.tags[_states] = string(key)
				timer = obs.timerGenerator(obs.scope.Tagged(obs.tags))
				obs.timers[key] = timer
			}

			timer.Record(
				time.Now().Sub(inProgressRecord.startTime),
			)
		}
		// not in progress anymore
		delete(obs.inProgress, currentState)
	}
}

func getRecorderKey(from statemachine.State, to statemachine.State) timerKey {
	return timerKey(string(from) + "_" + string(to))
}

type timerGenerator func(scope tally.Scope) tally.Timer

func tallyTimerGenerator(scope tally.Scope) tally.Timer {
	return scope.Timer("duration")
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
