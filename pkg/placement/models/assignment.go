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

package models

import (
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/plugins"
)

const (
	_defaultMaxHosts = 1
)

var _ plugins.Task = &Assignment{}

// AssignmentsToTasks converts a slice of assignments into a slice
// of tasks for plugin use.
func AssignmentsToTasks(assignments []*Assignment) []plugins.Task {
	tasks := []plugins.Task{}
	for _, a := range assignments {
		tasks = append(tasks, a)
	}
	return tasks
}

// Assignment represents the assignment of a task to a host.
// One host can be used in multiple assignments.
type Assignment struct {
	HostOffers *HostOffers `json:"host"`
	Task       *Task       `json:"task"`
	Reason     string
}

// NewAssignment will create a new empty assignment from a task.
func NewAssignment(task *Task) *Assignment {
	return &Assignment{
		Task: task,
	}
}

// GetHost returns the host that the task was assigned to.
func (a *Assignment) GetHost() *HostOffers {
	return a.HostOffers
}

// SetHost sets the host in the assignment to the given host.
func (a *Assignment) SetHost(host *HostOffers) {
	a.HostOffers = host
}

// GetTask returns the task of the assignment.
func (a *Assignment) GetTask() *Task {
	return a.Task
}

// SetTask sets the task in the assignment to the given task.
func (a *Assignment) SetTask(task *Task) {
	a.Task = task
}

// GetReason returns the reason why the assignment was unsuccessful
func (a *Assignment) GetReason() string {
	return a.Reason
}

// SetReason sets the reason for the failed assignment
func (a *Assignment) SetReason(reason string) {
	a.Reason = reason
}

// GetUsage returns the resource and port usage of this assignment.
func (a *Assignment) GetUsage() (res scalar.Resources, ports uint64) {
	res = scalar.FromResourceConfig(a.GetTask().GetTask().GetResource())
	ports = uint64(a.GetTask().GetTask().GetNumPorts())
	return
}

// Fits returns true if the given resources fit in the assignment.
func (a *Assignment) Fits(
	resLeft scalar.Resources,
	portsLeft uint64,
) (scalar.Resources, uint64, bool) {
	resNeeded, portsUsed := a.GetUsage()
	if portsLeft < portsUsed {
		log.WithFields(log.Fields{
			"resmgr_task":         a.GetTask().GetTask(),
			"num_available_ports": portsLeft,
		}).Debug("Insufficient ports resources.")
		return resLeft, portsLeft, false
	}

	remains, ok := resLeft.TrySubtract(resNeeded)
	if !ok {
		log.WithFields(log.Fields{
			"remain": resLeft,
			"usage":  resNeeded,
		}).Debug("Insufficient resources remain")
		return resLeft, portsLeft, false
	}
	return remains, portsLeft - portsUsed, true
}

// ID returns the id of the task that the assignment represents.
func (a *Assignment) ID() string {
	return a.GetTask().GetTask().GetId().GetValue()
}

// NeedsSpread returns whether this task was asked to be spread
// onto the hosts in its gang.
func (a *Assignment) NeedsSpread() bool {
	spreadStrat := job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB
	return a.GetTask().GetTask().GetPlacementStrategy() == spreadStrat
}

// PreferredHost returns the host preference for this task.
func (a *Assignment) PreferredHost() string {
	return a.GetTask().GetTask().GetDesiredHost()
}

// GetPlacementNeeds returns the placement needs of this task.
func (a *Assignment) GetPlacementNeeds() plugins.PlacementNeeds {
	rmTask := a.GetTask().GetTask()
	needs := plugins.PlacementNeeds{
		Resources:  scalar.FromResourceConfig(rmTask.GetResource()),
		Ports:      uint64(rmTask.GetNumPorts()),
		Revocable:  rmTask.Revocable,
		FDs:        rmTask.GetResource().GetFdLimit(),
		MaxHosts:   _defaultMaxHosts,
		HostHints:  map[string]string{},
		Constraint: rmTask.Constraint,
	}
	if a.PreferredHost() != "" {
		needs.HostHints[a.ID()] = a.PreferredHost()
	}
	// To spread out tasks over hosts, request host-manager
	// to rank hosts randomly instead of a predictable order such
	// as most-loaded.
	if rmTask.GetPlacementStrategy() == job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB {
		needs.RankHint = hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM
	}
	return needs
}
