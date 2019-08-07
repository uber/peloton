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

	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/plugins"
	common "github.com/uber/peloton/pkg/placement/plugins/mimir/common"
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
// TODO: mimir plugin should use plugins.Config as what batch plugin does,
//  instead of directly using config.PlacementConfig.
type mimir struct {
	placer algorithms.Placer
	config *config.PlacementConfig
}

func (mimir *mimir) convertAssignments(
	tasks []plugins.Task) (
	[]*placement.Assignment,
	map[*placement.Entity]int) {
	// Convert the Peloton assignments to mimir assignments and keep a map
	// from entities to Peloton assignments.
	assignments := make([]*placement.Assignment, 0, len(tasks))
	entitiesToAssignments := make(map[*placement.Entity]int, len(tasks))
	for idx, p := range tasks {
		entity := p.ToMimirEntity()
		assignments = append(assignments, placement.NewAssignment(entity))
		entitiesToAssignments[entity] = idx
	}
	return assignments, entitiesToAssignments
}

func (mimir *mimir) convertHosts(hosts []plugins.Host) (
	[]*placement.Group,
	map[*placement.Group]int,
) {
	// Convert the hosts to groups and keep a map from groups to hosts
	groups := make([]*placement.Group, 0, len(hosts))
	groupsToHosts := make(map[*placement.Group]int, len(hosts))
	for i, host := range hosts {
		group := host.ToMimirGroup()
		groupsToHosts[group] = i
		groups = append(groups, group)
	}
	return groups, groupsToHosts
}

func (mimir *mimir) getPlacements(
	tasks []plugins.Task,
	assignments []*placement.Assignment,
	entitiesToAssignments map[*placement.Entity]int,
	groupsToHosts map[*placement.Group]int,
) map[int]int {
	placements := map[int]int{}
	for i, assignment := range assignments {
		taskIdx := entitiesToAssignments[assignment.Entity]
		task := tasks[taskIdx]
		if assignment.Failed {
			task.SetPlacementFailure(assignment.Transcript.String())
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
	tasks []plugins.Task,
	hosts []plugins.Host,
) map[int]int {
	assignments, entitiesToAssignments := mimir.convertAssignments(tasks)
	groups, groupsToHosts := mimir.convertHosts(hosts)
	scopeSet := placement.NewScopeSet(groups)

	log.WithFields(log.Fields{
		"peloton_assignments": tasks,
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

	placements := mimir.getPlacements(tasks, assignments, entitiesToAssignments, groupsToHosts)

	log.WithFields(log.Fields{
		"placements":  placements,
		"assignments": tasks,
		"hosts":       hosts,
	}).Debug("GetTaskPlacements Mimir strategy returned")
	return placements
}

// GroupTasksByPlacementNeeds is an implementation of the placement.Strategy interface.
// Constructs host-filter for a set of assignments that have the same
// scheduling constraints, resource constraints and revocability.
func (mimir *mimir) GroupTasksByPlacementNeeds(
	tasks []plugins.Task,
) []*plugins.TasksByPlacementNeeds {
	if len(tasks) == 0 {
		return nil
	}

	pluginsConfig := &plugins.Config{
		TaskType:    mimir.config.TaskType,
		UseHostPool: mimir.config.UseHostPool,
	}

	tasksByNeeds := plugins.GroupByPlacementNeeds(tasks, pluginsConfig)
	factor := _offersFactor[mimir.config.TaskType]
	for _, group := range tasksByNeeds {
		maxOffers := mimir.config.OfferDequeueLimit
		neededOffers := math.Ceil(float64(len(group.Tasks)) * factor)
		if float64(maxOffers) > neededOffers {
			maxOffers = int(neededOffers)
		}
		group.PlacementNeeds.MaxHosts = uint32(maxOffers)

		for _, taskIdx := range group.Tasks {
			task := tasks[taskIdx]
			if hostname := task.PreferredHost(); hostname != "" {
				group.PlacementNeeds.HostHints[task.PelotonID()] = hostname
			}
		}
	}
	return tasksByNeeds
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (mimir *mimir) ConcurrencySafe() bool {
	return false
}
