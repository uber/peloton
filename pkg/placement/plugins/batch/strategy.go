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

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// New creates a new batch placement strategy.
func New() plugins.Strategy {
	log.Info("Using batch placement strategy.")
	return &batch{}
}

// batch is the batch placement strategy which just fills up offers with tasks one at a time.
type batch struct{}

// PlaceOnce is an implementation of the placement.Strategy interface.
func (batch *batch) PlaceOnce(unassigned []*models.Assignment, hosts []*models.HostOffers) {
	// All tasks are identical in their requirements, including
	// placementHint (see method Filters())). So just looking at
	// the first one is sufficient.
	if len(unassigned) == 0 {
		return
	}
	ph := unassigned[0].GetTask().GetTask().GetPlacementStrategy()
	if ph == job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB {
		unassigned = batch.spreadTasksOnHost(unassigned, hosts)
	} else {
		// the default host assignment strategy is PACK
		unassigned = batch.packTasksOnHost(unassigned, hosts)
	}

	log.WithFields(log.Fields{
		"unassigned":     unassigned,
		"hosts":          hosts,
		"placement_hint": ph.String(),
		"strategy":       "batch",
	}).Info("PlaceOnce batch strategy returned")
}

func (batch *batch) availablePorts(resources []*mesosv1.Resource) uint64 {
	var ports uint64
	for _, resource := range resources {
		if resource.GetName() != "ports" {
			continue
		}
		for _, portRange := range resource.GetRanges().GetRange() {
			ports += portRange.GetEnd() - portRange.GetBegin() + 1
		}
	}
	return ports
}

// Assign hosts to tasks by trying to pack as many tasks as possible
// on a single host. Returns any tasks that could not be assigned to
// a host.
func (batch *batch) packTasksOnHost(
	unassigned []*models.Assignment,
	hosts []*models.HostOffers,
) []*models.Assignment {
	for _, host := range hosts {
		log.WithFields(log.Fields{
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("PlaceOnce batch strategy called")

		unassigned = batch.fillOffer(host, unassigned)
	}
	return unassigned
}

// Assign exactly one task to a host, and return all tasks that
// could not be assigned (in case there are fewer hosts than tasks).
// Note that all task have identical resource and scheduling
// constraints, and each host satisifies these constraints.
// So a simple index-by-index assignment is just fine.
func (batch *batch) spreadTasksOnHost(
	unassigned []*models.Assignment,
	hosts []*models.HostOffers,
) []*models.Assignment {
	numTasks := len(hosts)
	if len(unassigned) < numTasks {
		numTasks = len(unassigned)
	}
	next := 0
	for ; next < numTasks; next++ {
		unassigned[next].SetHost(hosts[next])
	}
	if next < len(unassigned) {
		return unassigned[next:]
	}
	return nil
}

// fillOffer assigns in sequence as many tasks as possible to the given offers in a host,
// and returns a list of tasks not assigned to that host.

func (batch *batch) fillOffer(host *models.HostOffers, unassigned []*models.Assignment) []*models.Assignment {
	remainPorts := batch.availablePorts(host.GetOffer().GetResources())
	remain := scalar.FromMesosResources(host.GetOffer().GetResources())
	for i, placement := range unassigned {
		resmgrTask := placement.GetTask().GetTask()
		usedPorts := uint64(resmgrTask.GetNumPorts())
		if usedPorts > remainPorts {
			log.WithFields(log.Fields{
				"resmgr_task":         resmgrTask,
				"num_available_ports": remainPorts,
			}).Debug("Insufficient ports resources.")
			return unassigned[i:]
		}

		usage := scalar.FromResourceConfig(placement.GetTask().GetTask().GetResource())
		trySubtract, ok := remain.TrySubtract(usage)
		if !ok {
			log.WithFields(log.Fields{
				"remain": remain,
				"usage":  usage,
			}).Debug("Insufficient resources remain")
			return unassigned[i:]
		}

		remainPorts -= usedPorts
		remain = trySubtract
		placement.SetHost(host)
	}
	return nil
}

func (batch *batch) getHostFilter(assignment *models.Assignment) *hostsvc.HostFilter {
	rmTask := assignment.GetTask().GetTask()
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

// Filters is an implementation of the placement.Strategy interface.
func (batch *batch) Filters(assignments []*models.Assignment) map[*hostsvc.HostFilter][]*models.Assignment {
	groups := map[string]*hostsvc.HostFilter{}
	filters := map[*hostsvc.HostFilter][]*models.Assignment{}
	for _, assignment := range assignments {
		filter := batch.getHostFilter(assignment)
		// String() function on protobuf message should be nil-safe.
		s := filter.String()
		if _, exists := groups[s]; !exists {
			groups[s] = filter
		}
		batch := filters[groups[s]]
		batch = append(batch, assignment)
		filters[groups[s]] = batch
	}

	// Add quantity control to hostfilter.
	result := map[*hostsvc.HostFilter][]*models.Assignment{}
	for filter, assignments := range filters {
		filterWithQuantity := &hostsvc.HostFilter{
			ResourceConstraint:   filter.GetResourceConstraint(),
			SchedulingConstraint: filter.GetSchedulingConstraint(),
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(len(assignments)),
			},
			Hint: filter.GetHint(),
		}
		result[filterWithQuantity] = assignments
	}

	return result
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (batch *batch) ConcurrencySafe() bool {
	return true
}
