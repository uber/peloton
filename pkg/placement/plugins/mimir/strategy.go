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

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/v0"
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
			entity := v0_mimir.TaskToEntity(p.GetTask().GetTask(), false)
			p.GetTask().SetData(entity)
			data = entity
		}
		entity := data.(*placement.Entity)
		assignments = append(assignments, placement.NewAssignment(entity))
		entitiesToAssignments[entity] = p
	}
	return assignments, entitiesToAssignments
}

func (mimir *mimir) convertHosts(hosts []*models.HostOffers) (
	[]*placement.Group,
	map[*placement.Group]int,
) {
	// Convert the hosts to groups and keep a map from groups to hosts
	groups := make([]*placement.Group, 0, len(hosts))
	groupsToHosts := make(map[*placement.Group]int, len(hosts))
	for i, host := range hosts {
		data := host.Data()
		if data == nil {
			group := v0_mimir.OfferToGroup(host.GetOffer())
			entities := placement.Entities{}
			for _, task := range host.GetTasks() {
				entity := v0_mimir.TaskToEntity(task, true)
				entities.Add(entity)
			}
			group.Entities = entities
			group.Update()
			host.SetData(group)
			data = group
		}
		group := data.(*placement.Group)
		groupsToHosts[group] = i
		groups = append(groups, group)
	}
	return groups, groupsToHosts
}

func (mimir *mimir) getPlacements(
	assignments []*placement.Assignment,
	entitiesToAssignments map[*placement.Entity]*models.Assignment,
	groupsToHosts map[*placement.Group]int,
) map[int]int {
	placements := map[int]int{}
	for i, assignment := range assignments {
		pelotonAssignment := entitiesToAssignments[assignment.Entity]
		if assignment.Failed {
			pelotonAssignment.SetReason(assignment.Transcript.String())
			placements[i] = -1
			continue
		}
		hostIndex := groupsToHosts[assignment.AssignedGroup]
		placements[i] = hostIndex
	}
	return placements
}

// GetTaskPlacements is an implementation of the placement.Strategy interface.
func (mimir *mimir) GetTaskPlacements(
	pelotonAssignments []*models.Assignment,
	hosts []*models.HostOffers,
) map[int]int {
	assignments, entitiesToAssignments := mimir.convertAssignments(pelotonAssignments)
	groups, groupsToHosts := mimir.convertHosts(hosts)
	scopeSet := placement.NewScopeSet(groups)

	log.WithFields(log.Fields{
		"peloton_assignments": pelotonAssignments,
		"peloton_hosts":       hosts,
	}).Debug("GetTaskPlacements Mimir strategy called")

	// Place the assignments onto the groups
	mimir.placer.Place(assignments, groups, scopeSet)

	for _, assignment := range assignments {
		if assignment.AssignedGroup != nil {
			log.WithField("group", common.DumpGroup(assignment.AssignedGroup)).
				WithField("entity", common.DumpEntity(assignment.Entity)).
				WithField("transcript", assignment.Transcript.String()).
				Debug("Placed Mimir assignment")
		} else {
			log.WithField("entity", common.DumpEntity(assignment.Entity)).
				WithField("transcript", assignment.Transcript.String()).
				Debug("Did not place Mimir assignment")
		}
	}

	placements := mimir.getPlacements(assignments, entitiesToAssignments, groupsToHosts)

	log.WithFields(log.Fields{
		"placements":  placements,
		"assignments": pelotonAssignments,
		"hosts":       hosts,
	}).Debug("GetTaskPlacements Mimir strategy returned")
	return placements
}

// Filters is an implementation of the placement.Strategy interface.
func (mimir *mimir) Filters(
	assignments []*models.Assignment,
) map[*hostsvc.HostFilter][]*models.Assignment {
	// Batch assignments by their scheduling constraints. For each batch,
	// create a host filter that uses those scheduling constraints.
	assignmentsByConstraint := make(map[string][]*models.Assignment)
	for _, assignment := range assignments {
		// String() function on protobuf message is nil-safe.
		s := assignment.GetConstraint().String()
		batch := assignmentsByConstraint[s]
		batch = append(batch, assignment)
		assignmentsByConstraint[s] = batch
	}
	result := make(map[*hostsvc.HostFilter][]*models.Assignment)
	for _, batch := range assignmentsByConstraint {
		if f := mimir.getFilterForEquivalentAssignments(batch); f != nil {
			result[f] = batch
		}
	}
	return result
}

// Constructs host-filter for a set of assignments that have the same
// scheduling constraints and revocability. Resource constraints in the
// filter are set to maximum resource requirements of all assignments.
func (mimir *mimir) getFilterForEquivalentAssignments(
	assignments []*models.Assignment,
) *hostsvc.HostFilter {
	if len(assignments) == 0 {
		return nil
	}

	maxOffers := mimir.config.OfferDequeueLimit
	factor := _offersFactor[mimir.config.TaskType]
	neededOffers := math.Ceil(float64(len(assignments)) * factor)
	if float64(maxOffers) > neededOffers {
		maxOffers = int(neededOffers)
	}

	filter := (models.Assignments(assignments)).MergeHostFilters()
	filter.Quantity = &hostsvc.QuantityControl{
		MaxHosts: uint32(maxOffers),
	}
	return filter
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (mimir *mimir) ConcurrencySafe() bool {
	return false
}
