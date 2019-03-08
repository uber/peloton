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

package mimir

import (
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

var _offersFactor = map[resmgr.TaskType]float64{
	resmgr.TaskType_UNKNOWN:   1.0,
	resmgr.TaskType_BATCH:     1.0,
	resmgr.TaskType_STATELESS: 1.0,
	resmgr.TaskType_DAEMON:    1.0,
	resmgr.TaskType_STATEFUL:  1.0,
}

// New will create a new strategy using Mimir-lib to do the placement logic.
func New(placer algorithms.Placer, config *config.PlacementConfig) plugins.Strategy {
	log.Info("Using Mimir placement strategy.")
	return &mimir{
		placer: placer,
		config: config,
	}
}

// mimir is a placement strategy that uses the mimir library to decide on how to assign tasks to offers.
type mimir struct {
	placer algorithms.Placer
	config *config.PlacementConfig
}

func (mimir *mimir) convertAssignments(
	pelotonAssignments []*models.Assignment) (
	[]*placement.Assignment,
	map[*placement.Entity]*models.Assignment) {
	// Convert the Peloton assignments to mimir assignments and keep a map
	// from entities to Peloton assignments.
	assignments := make([]*placement.Assignment, 0, len(pelotonAssignments))
	entitiesToAssignments := make(map[*placement.Entity]*models.Assignment, len(pelotonAssignments))
	for _, p := range pelotonAssignments {
		data := p.GetTask().Data()
		if data == nil {
			entity := TaskToEntity(p.GetTask().GetTask(), false)
			p.GetTask().SetData(entity)
			data = entity
		}
		entity := data.(*placement.Entity)
		assignments = append(assignments, placement.NewAssignment(entity))
		entitiesToAssignments[entity] = p
	}
	return assignments, entitiesToAssignments
}

func (mimir *mimir) convertHosts(hosts []*models.HostOffers) ([]*placement.Group,
	map[*placement.Group]*models.HostOffers) {
	// Convert the hosts to groups and keep a map from groups to hosts
	groups := make([]*placement.Group, 0, len(hosts))
	groupsToHosts := make(map[*placement.Group]*models.HostOffers, len(hosts))
	for _, host := range hosts {
		data := host.Data()
		if data == nil {
			group := OfferToGroup(host.GetOffer())
			entities := placement.Entities{}
			for _, task := range host.GetTasks() {
				entity := TaskToEntity(task, true)
				entities.Add(entity)
			}
			group.Entities = entities
			group.Update()
			host.SetData(group)
			data = group
		}
		group := data.(*placement.Group)
		groupsToHosts[group] = host
		groups = append(groups, group)
	}
	return groups, groupsToHosts
}

func (mimir *mimir) updateAssignments(
	assignments []*placement.Assignment,
	entitiesToAssignments map[*placement.Entity]*models.Assignment,
	groupsToHosts map[*placement.Group]*models.HostOffers) {
	// Update the Peloton assignments from the mimir assignments
	for _, assignment := range assignments {
		pelotonAssignment := entitiesToAssignments[assignment.Entity]
		if assignment.Failed {
			pelotonAssignment.SetReason(assignment.Transcript.String())
			continue
		}
		host := groupsToHosts[assignment.AssignedGroup]
		pelotonAssignment.SetHost(host)
	}
}

// PlaceOnce is an implementation of the placement.Strategy interface.
func (mimir *mimir) PlaceOnce(
	pelotonAssignments []*models.Assignment,
	hosts []*models.HostOffers) {
	assignments, entitiesToAssignments := mimir.convertAssignments(pelotonAssignments)
	groups, groupsToHosts := mimir.convertHosts(hosts)
	scopeSet := placement.NewScopeSet(groups)

	log.WithFields(log.Fields{
		"peloton_assignments": pelotonAssignments,
		"peloton_hosts":       hosts,
	}).Debug("PlaceOnce Mimir strategy called")

	// Place the assignments onto the groups
	mimir.placer.Place(assignments, groups, scopeSet)

	for _, assignment := range assignments {
		if assignment.AssignedGroup != nil {
			log.WithField("group", dumpGroup(assignment.AssignedGroup)).
				WithField("entity", dumpEntity(assignment.Entity)).
				WithField("transcript", assignment.Transcript.String()).
				Debug("Placed Mimir assignment")
		} else {
			log.WithField("entity", dumpEntity(assignment.Entity)).
				WithField("transcript", assignment.Transcript.String()).
				Debug("Did not place Mimir assignment")
		}
	}

	mimir.updateAssignments(assignments, entitiesToAssignments, groupsToHosts)

	log.WithFields(log.Fields{
		"assignments": pelotonAssignments,
		"hosts":       hosts,
	}).Debug("PlaceOnce Mimir strategy returned")
}

// Filters is an implementation of the placement.Strategy interface.
func (mimir *mimir) Filters(assignments []*models.Assignment) map[*hostsvc.HostFilter][]*models.Assignment {
	assignmentsCopy := make([]*models.Assignment, 0, len(assignments))
	var maxCPU, maxGPU, maxMemory, maxDisk, maxPorts float64
	var revocable bool
	var hostHints []*hostsvc.FilterHint_Host
	for _, assignment := range assignments {
		assignmentsCopy = append(assignmentsCopy, assignment)
		resmgrTask := assignment.GetTask().GetTask()
		maxCPU = math.Max(maxCPU, resmgrTask.Resource.CpuLimit)
		maxGPU = math.Max(maxGPU, resmgrTask.Resource.GpuLimit)
		maxMemory = math.Max(maxMemory, resmgrTask.Resource.MemLimitMb)
		maxDisk = math.Max(maxDisk, resmgrTask.Resource.DiskLimitMb)
		maxPorts = math.Max(maxPorts, float64(resmgrTask.NumPorts))
		revocable = resmgrTask.Revocable
		if len(resmgrTask.GetDesiredHost()) != 0 {
			hostHints = append(hostHints, &hostsvc.FilterHint_Host{
				Hostname: resmgrTask.GetDesiredHost(),
				TaskID:   resmgrTask.GetId(),
			})
		}
	}
	maxOffers := mimir.config.OfferDequeueLimit
	factor := _offersFactor[mimir.config.TaskType]
	neededOffers := math.Ceil(float64(len(assignments)) * factor)
	if float64(maxOffers) > neededOffers {
		maxOffers = int(neededOffers)
	}
	return map[*hostsvc.HostFilter][]*models.Assignment{
		{
			ResourceConstraint: &hostsvc.ResourceConstraint{
				NumPorts: uint32(maxPorts),
				Minimum: &task.ResourceConfig{
					CpuLimit:    maxCPU,
					GpuLimit:    maxGPU,
					MemLimitMb:  maxMemory,
					DiskLimitMb: maxDisk,
				},
				Revocable: revocable,
			},
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(maxOffers),
			},
			Hint: &hostsvc.FilterHint{
				HostHint: hostHints,
			},
		}: assignmentsCopy,
	}
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (mimir *mimir) ConcurrencySafe() bool {
	return false
}
