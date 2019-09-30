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

package goalstate

import (
	"context"

	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common/goalstate"

	log "github.com/sirupsen/logrus"
)

// HostAction is a string for host actions.
type HostAction string

const (
	// NoAction implies do not take any action
	NoAction HostAction = "noop"
	// DrainAction enqueues the host for draining its tasks
	DrainAction HostAction = "drain"
	// DownAction registers the host as DOWN, in maintenance
	DownAction HostAction = "down"
	// UpAction registers the host as UP, out of maintenance
	UpAction HostAction = "up"
	// RequeueAction re-enqueues the host into the goal state engine
	// This is needed as host entity GetState() & GetGoalState() are reads from DB
	// which can error out (e.g. DB unavailable), the retry mechanism is handled by
	// re-enqueing into the goal state engine.
	// TODO: remove once host state & goalState backed by host cache
	RequeueAction HostAction = "reenqueue"
	// UntrackAction untracks the host by removing it
	// from the goal state engine's entity map
	UntrackAction HostAction = "untrack"

	// ChangePoolAction is to change the host pool of the
	// host. This action will do two things
	// 1. Change the current pool in db as desired pool
	// 2. Make the goal state to Up and reenqueue
	ChangePoolAction HostAction = "ChangePool"

	// StartMaintenanceAction is to start the Maintenance
	// if we find pool is not equal to desired pool
	StartMaintenanceAction HostAction = "StartMaintenance"

	// InvalidAction is a invalid action and this should not occur
	InvalidAction HostAction = "InvalidAction"
)

// _hostActionsMap maps the HostAction string to the Action function.
// TODO: remove RequeueAction once host state & goalState
// backed by host cache instead of DB
var (
	_hostActionsMap = map[HostAction]goalstate.ActionExecute{
		NoAction:               nil,
		DrainAction:            HostDrain,
		DownAction:             HostDown,
		UpAction:               HostUp,
		RequeueAction:          HostRequeue,
		UntrackAction:          HostUntrack,
		StartMaintenanceAction: HostTriggerMaintenance,
		ChangePoolAction:       HostChangePool,
		InvalidAction:          HostInvalidAction,
	}
)

// _hostRules maps current states to action, given a goal state:
// goal-state -> current-state -> action.
// TODO: remove RequeueAction once host state & goalState
// backed by host cache instead of DB
var (
	_hostRules = map[hpb.HostState]map[hpb.HostState]HostAction{
		hpb.HostState_HOST_STATE_DOWN: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       DrainAction,
			hpb.HostState_HOST_STATE_DRAINING: DrainAction,
			hpb.HostState_HOST_STATE_DRAINED:  DownAction,
			hpb.HostState_HOST_STATE_DOWN:     UntrackAction,
		},
		hpb.HostState_HOST_STATE_UP: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       UntrackAction,
			hpb.HostState_HOST_STATE_DOWN:     UpAction,
			hpb.HostState_HOST_STATE_DRAINING: InvalidAction,
		},
		hpb.HostState_HOST_STATE_INVALID: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       RequeueAction,
			hpb.HostState_HOST_STATE_DRAINING: RequeueAction,
			hpb.HostState_HOST_STATE_DRAINED:  RequeueAction,
			hpb.HostState_HOST_STATE_DOWN:     RequeueAction,
		},
	}

	// This is the action list if pool is different then current pool
	_hostRulesDifferentPool = map[hpb.HostState]map[hpb.HostState]HostAction{
		hpb.HostState_HOST_STATE_DOWN: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       DrainAction,
			hpb.HostState_HOST_STATE_DRAINING: DrainAction,
			hpb.HostState_HOST_STATE_DRAINED:  DownAction,
			hpb.HostState_HOST_STATE_DOWN:     ChangePoolAction,
		},
		hpb.HostState_HOST_STATE_UP: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       StartMaintenanceAction,
			hpb.HostState_HOST_STATE_DOWN:     ChangePoolAction,
			hpb.HostState_HOST_STATE_DRAINING: InvalidAction,
		},
		hpb.HostState_HOST_STATE_INVALID: {
			hpb.HostState_HOST_STATE_INVALID:  RequeueAction,
			hpb.HostState_HOST_STATE_UP:       RequeueAction,
			hpb.HostState_HOST_STATE_DRAINING: RequeueAction,
			hpb.HostState_HOST_STATE_DRAINED:  RequeueAction,
			hpb.HostState_HOST_STATE_DOWN:     RequeueAction,
		},
	}
)

type hostEntity struct {
	hostname string           // host hostname used as identifier
	driver   *driver          // goal state driver
	state    *hostEntityState // host entity state
}

// hostEntityState will be the composite state of
// 1. Hoststate
// 2. hostpool
type hostEntityState struct {
	hostState hpb.HostState // Host states
	hostPool  string        // host pool
}

// NewHostEntity implements the goal state Entity interface for hosts.
func NewHostEntity(hostname string, driver *driver) goalstate.Entity {
	return &hostEntity{
		hostname: hostname,
		driver:   driver,
	}
}

// GetID returns the entity ID, for a host its hostname
func (h *hostEntity) GetID() string {
	return h.hostname
}

// GetState returns the entity's state
func (h *hostEntity) GetState() interface{} {
	// Read the hostInfo from DB
	hostInfo, err := h.driver.hostInfoOps.Get(context.Background(), h.hostname)
	if err != nil {
		// By convention return INVALID state on DB read failure
		// which is specifically handled with retries
		return &hostEntityState{
			hostState: hpb.HostState_HOST_STATE_INVALID,
			hostPool:  "",
		}
	}
	return &hostEntityState{
		hostState: hostInfo.GetState(),
		hostPool:  hostInfo.GetCurrentPool(),
	}
}

// GetGoalState returns the entity's goal state
func (h *hostEntity) GetGoalState() interface{} {
	// Read the hostInfo from DB
	hostInfo, err := h.driver.hostInfoOps.Get(context.Background(), h.hostname)
	if err != nil {
		// By convention return INVALID state on DB read failure
		// which is specifically handled with retries
		return &hostEntityState{
			hostState: hpb.HostState_HOST_STATE_INVALID,
			hostPool:  "",
		}
	}
	return &hostEntityState{
		hostState: hostInfo.GetGoalState(),
		hostPool:  hostInfo.GetDesiredPool(),
	}
}

// GetActionList returns the list of actions
// to be executed based on the current state and goal state
func (h *hostEntity) GetActionList(
	state,
	goalState interface{},
) (context.Context, context.CancelFunc, []goalstate.Action) {
	var actions []goalstate.Action

	// Retrieve action based on goal state and current state
	hostState := state.(*hostEntityState)
	hostGoalState := goalState.(*hostEntityState)
	_actionRules := _hostRules
	if hostState.hostPool != hostGoalState.hostPool {
		_actionRules = _hostRulesDifferentPool
	}

	if tr, ok := _actionRules[hostGoalState.hostState]; ok {
		if a, ok := tr[hostState.hostState]; ok {
			action := goalstate.Action{
				Name:    string(a),
				Execute: _hostActionsMap[a],
			}

			log.WithFields(log.Fields{
				"hostname":     h.hostname,
				"state":        hostState.hostState.String(),
				"pool":         hostState.hostPool,
				"goal_state":   hostGoalState.hostState.String(),
				"desired_pool": hostGoalState.hostPool,
				"action":       action.Name,
			}).Info("running host action")

			actions = append(actions, action)
		}
	}

	return context.Background(), nil, actions
}
