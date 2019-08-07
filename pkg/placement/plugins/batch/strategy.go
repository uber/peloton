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

package batch

import (
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// New creates a new batch placement strategy.
func New(config *config.PlacementConfig) plugins.Strategy {
	log.Info("Using batch placement strategy.")
	return &batch{
		config: &plugins.Config{
			TaskType:    config.TaskType,
			UseHostPool: config.UseHostPool,
		},
	}
}

// batch is the batch placement strategy which just fills up offers with tasks one at a time.
type batch struct {
	config *plugins.Config
}

// GetTaskPlacements is an implementation of the placement.Strategy interface.
func (batch *batch) GetTaskPlacements(
	unassigned []plugins.Task,
	hosts []plugins.Host,
) map[int]int {
	// All tasks are identical in their requirements, including
	// placementHint (see method GroupTasksByPlacementNeeds())). So just looking at
	// the first one is sufficient.
	if len(unassigned) == 0 {
		return map[int]int{}
	}

	var placements map[int]int
	if unassigned[0].NeedsSpread() {
		placements = batch.spreadTasksOnHost(unassigned, hosts)
	} else {
		// the default host task strategy is PACK
		placements = batch.packTasksOnHost(unassigned, hosts)
	}

	var leftOver []interface{}
	for taskIdx, task := range unassigned {
		if _, isAssigned := placements[taskIdx]; !isAssigned {
			placements[taskIdx] = -1
			leftOver = append(leftOver, task)
		}
	}

	log.WithFields(log.Fields{
		"failed_tasks": leftOver,
		"hosts":        hosts,
		"strategy":     "batch",
	}).Info("GetTaskPlacements batch strategy returned")
	return placements
}

// Assign hosts to tasks by trying to pack as many tasks as possible
// on a single host. Returns any tasks that could not be assigned to
// a host.
// The output is a map[taskIndex]HostIndex, as defined by the
// GetTaskPlacements function signature.
func (batch *batch) packTasksOnHost(
	unassigned []plugins.Task,
	hosts []plugins.Host,
) map[int]int {
	placements := map[int]int{}
	totalAssignedCount := 0
	for hostIdx, host := range hosts {
		log.WithFields(log.Fields{
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("GetTaskPlacements batch strategy called")

		assignedCount := batch.getTasksForHost(host, unassigned[totalAssignedCount:])
		for taskIdx := 0; taskIdx < assignedCount; taskIdx++ {
			placementIdx := totalAssignedCount + taskIdx
			placements[placementIdx] = hostIdx
		}
		totalAssignedCount += assignedCount
	}
	return placements
}

// Assign exactly one task to a host, and return all tasks that
// could not be assigned (in case there are fewer hosts than tasks).
// Note that all task have identical resource and scheduling
// constraints, and each host satisifies these constraints.
// So a simple index-by-index task is just fine.
// The output is a map[taskIndex]HostIndex, as defined by the
// GetTaskPlacements function signature.
func (batch *batch) spreadTasksOnHost(
	unassigned []plugins.Task,
	hosts []plugins.Host,
) map[int]int {
	numTasks := len(hosts)
	if len(unassigned) < numTasks {
		numTasks = len(unassigned)
	}

	placements := map[int]int{}
	for i := 0; i < numTasks; i++ {
		placements[i] = i
	}
	return placements
}

// getTasksForHost tries to fit in sequence as many tasks as possible
// to the given offers in a host, and returns the indices of the
// tasks that fit on that host. getTasksForHost does not call mutate its
// inputs in any way.
// NOTE: getTasksForHost stops at the first task that failed to fit on
// the host.
// TODO (pourchet): Is the above note a bug? Or is it deliberate?
func (batch *batch) getTasksForHost(
	host plugins.Host,
	unassigned []plugins.Task,
) int {
	resLeft, portsLeft := host.GetAvailableResources()
	for i, task := range unassigned {
		var ok bool
		resLeft, portsLeft, ok = task.Fits(resLeft, portsLeft)
		if !ok {
			return i
		}
	}
	return len(unassigned)
}

// GroupTasksByPlacementNeeds is an implementation of the placement.Strategy interface.
func (batch *batch) GroupTasksByPlacementNeeds(
	tasks []plugins.Task,
) []*plugins.TasksByPlacementNeeds {
	if len(tasks) == 0 {
		return nil
	}

	// No alteration of the task needs.
	tasksByNeeds := plugins.GroupByPlacementNeeds(tasks, batch.config)
	for _, group := range tasksByNeeds {
		group.PlacementNeeds.MaxHosts = uint32(len(group.Tasks))
	}
	return tasksByNeeds
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (batch *batch) ConcurrencySafe() bool {
	return true
}
