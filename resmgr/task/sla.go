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

// default SLAs for different transitions. RM Tasks which breach these will be
// logged.
const (
	_pendingToReady     = 1 * time.Hour
	_readyToRunning     = 30 * time.Minute
	_readyToPlaced      = 15 * time.Minute
	_preemptingToKilled = 10 * time.Minute
)

// represents the states for which the duration needs to be recorded along with
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

// tags for state transition metrics
const (
	_respoolPath = "respool_path"
	_states      = "states"
)

// TransitionObserver is the interface for observing a state transition
type TransitionObserver interface {
	Observe(taskID string, transitedTo task.TaskState)
}

// the key of the timer of the form startState_endState Eg PENDING_READY
type timerKey string

// timeRecord is the recorded time when the task transitioned to a state
type timeRecord struct {
	mesosTaskID string
	// the state when the time was recorded
	startState task.TaskState
	// the time when the task reached the state
	startTime time.Time
}

// TransObs implements TransitionObserver
type TransObs struct {
	// boolean to enable/disable the observer
	enabled bool

	// rules represents the transitions to record as map of start state
	// -> list of end states
	rules map[task.TaskState]map[task.TaskState]time.Duration

	// map of in progress transitions which need to be recorded.
	// Its keyed by the end state which it is waiting on.
	inProgress map[task.TaskState][]timeRecord

	// generator for timer
	timerGenerator timerGenerator
	// timers is a map of record-key -> timers
	timers map[timerKey]tally.Timer

	// the metrics scope
	scope tally.Scope
	// the tags for the metrics which represent the task uniquely
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
	rules map[task.TaskState]map[task.TaskState]time.Duration,
	enabled bool) *TransObs {

	return &TransObs{
		inProgress:     make(map[task.TaskState][]timeRecord),
		timers:         make(map[timerKey]tally.Timer),
		rules:          rules,
		tags:           tags,
		scope:          scope.SubScope("state_transition"),
		timerGenerator: tallyTimerGenerator,
		enabled:        enabled,
	}
}

// Observe implements TransitionObserver
func (obs *TransObs) Observe(mesosTaskID string, currentState task.TaskState) {
	if !obs.enabled {
		return
	}

	// go through all the rules and see if currentState is the start state
	// for any of the rules.
	if endStates, ok := obs.rules[currentState]; ok {
		for endState := range endStates {
			obs.inProgress[endState] = append(obs.inProgress[endState],
				timeRecord{
					mesosTaskID: mesosTaskID,
					startState:  currentState,
					startTime:   time.Now(),
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

			// get the time and record it
			sub := time.Now().Sub(inProgressRecord.startTime)
			timer.Record(sub)

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
		// not in progress anymore
		delete(obs.inProgress, currentState)
	}
}

func getRecorderKey(from task.TaskState, to task.TaskState) timerKey {
	return timerKey(string(from) + "_" + string(to))
}

type timerGenerator func(scope tally.Scope) tally.Timer

func tallyTimerGenerator(scope tally.Scope) tally.Timer {
	return scope.Timer("duration")
}
