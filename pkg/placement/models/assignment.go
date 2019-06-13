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
)

// Assignment represents the assignment of a task to a host.
// One host can be used in multiple assignments.
type Assignment struct {
	HostOffers *HostOffers `json:"host"`
	Task       *Task       `json:"task"`
	Reason     string
}

// Assignments is a list of Assignments.
type Assignments []*Assignment

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

// GetHostHints returns the host hints for the host manager service
// that matches this assignment.
func (a *Assignment) GetHostHints() []*hostsvc.FilterHint_Host {
	resmgrTask := a.GetTask().GetTask()
	if len(resmgrTask.GetDesiredHost()) == 0 {
		return nil
	}
	return []*hostsvc.FilterHint_Host{
		{
			Hostname: resmgrTask.GetDesiredHost(),
			TaskID:   resmgrTask.GetId(),
		},
	}
}

// GetSimpleHostFilter returns the simplest host filter that matches
// this assignment. It includes a ResourceConstraint and a
// SchedulingConstraint.
func (a *Assignment) GetSimpleHostFilter() *hostsvc.HostFilter {
	rmTask := a.GetTask().GetTask()
	result := &hostsvc.HostFilter{
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum:   rmTask.Resource,
			NumPorts:  rmTask.NumPorts,
			Revocable: rmTask.Revocable,
		},
		Hint: &hostsvc.FilterHint{},
	}
	if constraint := rmTask.Constraint; constraint != nil {
		result.SchedulingConstraint = constraint
	}
	// To spread out tasks over hosts, request host-manager
	// to rank hosts randomly instead of a predictable order such
	// as most-loaded.
	if rmTask.GetPlacementStrategy() == job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB {
		result.Hint.RankHint = hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM
	}
	return result
}

// GetFullHostFilter returns the full host filter complete with quantity
// and hints.
func (a *Assignment) GetFullHostFilter() *hostsvc.HostFilter {
	filter := a.GetSimpleHostFilter()
	filter.Quantity = &hostsvc.QuantityControl{MaxHosts: 1}
	filter.Hint = &hostsvc.FilterHint{
		RankHint: filter.Hint.RankHint,
		HostHint: a.GetHostHints(),
	}
	return filter
}

// GroupByHostFilter groups the assignments according to the result of
// a function that takes in an assignment.
func (as Assignments) GroupByHostFilter(
	fn func(a *Assignment) *hostsvc.HostFilter,
) map[*hostsvc.HostFilter]Assignments {
	groups := map[string]*hostsvc.HostFilter{}
	filters := map[*hostsvc.HostFilter]Assignments{}
	for _, a := range as {
		filter := fn(a)
		s := filter.String()
		if _, exists := groups[s]; !exists {
			groups[s] = filter
		}
		filter = groups[s]
		filters[filter] = append(filters[filter], a)
	}
	return filters
}

// GetPlacementStrategy returns the placement strategy for this list
// of assignments.
func (as Assignments) GetPlacementStrategy() job.PlacementStrategy {
	return as[0].GetTask().GetTask().GetPlacementStrategy()
}
