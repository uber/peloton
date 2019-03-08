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

	"github.com/uber/peloton/.gen/mesos/v1"
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
	for _, host := range hosts {
		log.WithFields(log.Fields{
			"unassigned": unassigned,
			"hosts":      hosts,
		}).Debug("PlaceOnce batch strategy called")

		unassigned = batch.fillOffer(host, unassigned)
	}

	log.WithFields(log.Fields{
		"unassigned": unassigned,
		"hosts":      hosts,
		"strategy":   "batch",
	}).Info("PlaceOnce batch strategy returned")
}

func (batch *batch) availablePorts(resources []*mesos_v1.Resource) uint64 {
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
	result := &hostsvc.HostFilter{
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum:   assignment.GetTask().GetTask().Resource,
			NumPorts:  assignment.GetTask().GetTask().NumPorts,
			Revocable: assignment.GetTask().GetTask().Revocable,
		},
	}
	if constraint := assignment.GetTask().GetTask().Constraint; constraint != nil {
		result.SchedulingConstraint = constraint
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
		}
		result[filterWithQuantity] = assignments
	}

	return result
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (batch *batch) ConcurrencySafe() bool {
	return true
}
