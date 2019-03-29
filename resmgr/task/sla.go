// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// 22 buckets starting at 50 ms
//
// 1   0.05
// 2   0.1
// 3   0.2
// 4   0.4
// 5   0.8
// 6   1.6
// 7   3.2
// 8   6.4
// 9   12.8
// 10  25.6
// 11  51.2
// 12  102.4
// 13  204.8
// 14  409.6
// 15  819.2
// 16  1638.4
// 17  3276.8
// 18  6553.6
// 19  13107.2
// 20  26214.4
// 21  52428.8
// 22  104857.6
var _buckets = tally.MustMakeExponentialDurationBuckets(
	50*time.Millisecond, 2, 22)

// Default SLAs for different transitions. RM Tasks which breach these will be
// logged.
const (
	_pendingToReady     = 1 * time.Hour
	_readyToRunning     = 30 * time.Minute
	_readyToPlaced      = 15 * time.Minute
	_preemptingToKilled = 10 * time.Minute
)

// Represents the states for which the duration needs to be recorded along with
// an optional SLA for those transitions.
var defaultRules = map[task.TaskState]map[task.TaskState]time.Duration{
	task.TaskState_PENDING: {
		// The time it takes for the task to go from being
		// added in the pending queue to being admitted in a resource
		// pool.
		task.TaskState_READY: _pendingToReady,
	},
	task.TaskState_READY: {
		// The time it takes for the task to go from being
		// admitted in the resource manager to actually running as
		// reported by Mesos.
		task.TaskState_RUNNING: _readyToRunning,
		// The time it takes for the task to go from being
		// admitted in the resource manager to being placed.
		task.TaskState_PLACED: _readyToPlaced,
	},
	task.TaskState_PREEMPTING: {
		// The time it takes for the task to go from being
		// picked for preemption to being killed as reported by Mesos.
		task.TaskState_KILLED: _preemptingToKilled,
	},
}

// Tags for state transition metrics
const (
	_respoolPath = "respool_path"
	_from        = "from"
	_to          = "to"
	_jobID       = "job_id"
)

// TransitionObserver is the interface for observing a state transition
type TransitionObserver interface {
	Observe(taskID string, transitedTo task.TaskState)
}

// The key of the recorder of the form startState_endState Eg PENDING_READY
type recordKey string

// record is the recorded time when the rm task transitioned to a state
type record struct {
	// The Mesos task ID when the state was recorded. This can change during the
	// lifetime of an rmtask so we need to keep a track of it.
	mesosTaskID string
	// The state when the time was recorded
	startState task.TaskState
	// The time when the task reached the state
	startTime time.Time
}

type recorder interface {
	// RecordDuration records a specific duration directly.
	RecordDuration(value time.Duration)
}

type recorderGenerator func(scope tally.Scope) recorder

func tallyHistogramGenerator(scope tally.Scope) recorder {
	return scope.Histogram("duration", _buckets)
}

// TransObs implements TransitionObserver
type TransObs struct {
	// boolean to enable/disable the observer
	enabled bool

	// Represents the transitions to record as map of start state
	// -> list of end states
	rules map[task.TaskState]map[task.TaskState]time.Duration

	// map of in progress transitions which need to be recorded.
	// Its keyed by the end state which it is waiting on.
	inProgress map[task.TaskState][]record

	// Generator for recorder
	recorderGenerator recorderGenerator
	recorders         map[recordKey]recorder

	// The metrics scope
	scope tally.Scope
	// The tags for the metrics which represent the task uniquely
	tags map[string]string
}

// NewTransitionObserver returns the a new observer for the respool and
// the task tagged with the relevant tags
func NewTransitionObserver(
	enabled bool,
	scope tally.Scope,
	respoolPath string,
) TransitionObserver {
	tags := make(map[string]string)
	if enabled {
		// These tags don't change during the lifetime of the observer.
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
	rules map[task.TaskState]map[task.TaskState]time.Duration,
	enabled bool) *TransObs {
	return &TransObs{
		inProgress:        make(map[task.TaskState][]record),
		recorders:         make(map[recordKey]recorder),
		rules:             rules,
		tags:              tags,
		scope:             scope.SubScope("state_transition"),
		recorderGenerator: tallyHistogramGenerator,
		enabled:           enabled,
	}
}

func getRecorderKey(from task.TaskState, to task.TaskState) recordKey {
	return recordKey(from.String() + "_" + to.String())
}

// Observe implements TransitionObserver
func (obs *TransObs) Observe(mesosTaskID string, currentState task.TaskState) {
	if !obs.enabled {
		return
	}

	// Go through all the rules and see if currentState is the start state
	// for any of the rules.
	if endStates, ok := obs.rules[currentState]; ok {
		for endState := range endStates {
			obs.inProgress[endState] = append(obs.inProgress[endState],
				record{
					mesosTaskID: mesosTaskID,
					startState:  currentState,
					startTime:   time.Now(),
				})
		}
	}

	// Go through all the in progress transitions and see if any one is
	// waiting on the current_state, if so record it and remove it from the map.
	if inProgressRecords, ok := obs.inProgress[currentState]; ok {
		for _, inProgressRecord := range inProgressRecords {
			key := getRecorderKey(
				inProgressRecord.startState, currentState)

			recorder, ok := obs.recorders[key]
			if !ok {
				// Add the state tags
				obs.tags[_from] = inProgressRecord.startState.String()
				obs.tags[_to] = currentState.String()

				// lazily instantiate the recorders
				recorder = obs.recorderGenerator(obs.scope.Tagged(obs.tags))
				obs.recorders[key] = recorder
			}

			// get the time and record it
			sub := time.Now().Sub(inProgressRecord.startTime)
			recorder.RecordDuration(sub)

			// if time elapsed breached sla lets log it.
			sla := obs.rules[inProgressRecord.startState][currentState]
			if sub > sla {
				log.WithField("start_mesos_task_id", inProgressRecord.mesosTaskID).
					WithField("current_mesos_task_id", mesosTaskID).
					WithField("to_state", currentState.String()).
					WithField("from_state", inProgressRecord.startState.String()).
					WithField("sla_minutes", sla.Minutes()).
					WithField("time_elapsed", sub.Minutes()).
					Info("RMTask SLA breached")
			}
		}
		// Not in progress anymore
		delete(obs.inProgress, currentState)
	}
}
