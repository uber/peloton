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

package entitlement

import (
	"math"

	log "github.com/sirupsen/logrus"

	pb_res "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
)

// Setting total entitlement is a 3 phase process for non-revocable resources
// 1 Calculate assignments based on reservation and limit non-revocable tasks
//
// 2 For non-revocable tasks, distribute rest of the free resources
//	 based on share and demand
//
// 3 Once the demand is zero, distribute remaining based on share
//   for non-revocable tasks
func (c *Calculator) setEntitlementForChildren(
	resp respool.ResPool) {
	if resp == nil {
		return
	}

	childs := resp.Children()

	entitlement := resp.GetEntitlement().Clone()
	assignments := make(map[string]*scalar.Resources)
	demands := make(map[string]*scalar.Resources)
	totalShare := make(map[string]float64)

	log.WithFields(log.Fields{
		"respool_name": resp.Name(),
		"respool_id":   resp.ID(),
		"entitlement":  entitlement.String(),
	}).Info("Starting Entitlement cycle for respool")

	// This is the first phase for assignment
	c.calculateAssignmentsFromReservation(
		resp,
		demands,
		entitlement,
		assignments,
		totalShare)

	// This is second phase for distributing remaining resources
	c.distributeRemainingResources(
		resp,
		demands,
		entitlement,
		assignments,
		totalShare)

	// This is the third phase for the entitlement cycle. here after all the
	// assigmenets based on demand, rest of the resources are being
	// distributed in all the resource pools.
	c.distributeUnclaimedResources(
		resp,
		entitlement,
		assignments)

	log.WithFields(log.Fields{
		"respool_name": resp.Name(),
		"respool_id":   resp.ID(),
	}).Info("Completed Entitlement cycle for respool")

	// Update each resource pool metrics post entitlement calculation
	resp.UpdateResourceMetrics()

	// Now setting entitlement for all the children and call
	// for their children recursively
	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)

		n.SetEntitlement(assignments[n.ID()])
		c.setEntitlementForChildren(n)
	}
}

// calculateAssignmentsFromReservation calculates assigments
// based on demand and reservation
// as well as return the remaining entitlement for redistribution
func (c *Calculator) calculateAssignmentsFromReservation(
	resp respool.ResPool,
	demands map[string]*scalar.Resources,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources,
	totalShare map[string]float64) {
	// First Pass: In the first pass of children we get the demand recursively
	// calculated And then compare with respool reservation and
	// choose the min of these two
	// assignment := min(demand,reservation) if the resource type is ELASTIC.
	// If the resourceType is Static then we need to make assignment to
	// equal to reservation.
	// We also measure the free entitlement by that we can distribute
	// it with fair share. We also need to keep track of the total share
	// of the kind of resources which demand is more then the resrevation
	// As we can ignore the other whose demands are reached as they dont
	// need to get the fare share
	childs := resp.Children()
	cloneEntitlement := entitlement.Clone()
	for e := childs.Front(); e != nil; e = e.Next() {
		assignment := new(scalar.Resources)
		n := e.Value.(respool.ResPool)

		resConfigMap := n.Resources()
		c.calculateDemandForRespool(n, demands)

		limitedDemand := demands[n.ID()].Clone()
		demand := demands[n.ID()]
		// Now checking if demand is less then reservation or not
		// Taking the assignement to the min(demand,reservation)
		for kind, cfg := range resConfigMap {
			// Checking the reservation type of the resource pool
			// If resource type is reservation type static then
			// entitlement will always be greater than equal to
			// reservation irrespective of the demand. Otherwise
			// Based on the demand assignment := min(demand,reservation)
			if cfg.Type == pb_res.ReservationType_STATIC {
				assignment.Set(kind, cfg.Reservation)
			} else {
				assignment.Set(kind, math.Min(demand.Get(kind), cfg.Reservation))
			}
			if demand.Get(kind) > cfg.Reservation {
				totalShare[kind] += cfg.Share
				demand.Set(kind, demand.Get(kind)-cfg.Reservation)
			} else {
				demand.Set(kind, 0)
			}
		}

		cloneEntitlement = cloneEntitlement.Subtract(assignment)
		assignments[n.ID()] = assignment
		demands[n.ID()] = demand
		log.WithFields(log.Fields{
			"respool_resources":                    n.Resources(),
			"respool_name":                         n.Name(),
			"demand_cap_by_limit":                  limitedDemand.String(),
			"demand_cap_by_reservation_assignment": assignment.String(),
			"demand_not_satisfied":                 demand.String(),
			"entitlement":                          cloneEntitlement.String(),
			"slack_entitlement":                    n.GetSlackEntitlement().String(),
			"total_share":                          totalShare,
		}).Info("First pass completed for respool")
	}
	entitlement.Copy(cloneEntitlement)
}

// calculateDemandForRespool calculates the demand based on number of
// tasks waiting in the queue as well as current allocation
// for non-revocable and revocable tasks.
//
// For revocable tasks total requirement is capped to slack limit.
// For non-revocable tasks totol requirement is capped on the limit of the
// resource pool for that type of resource
func (c *Calculator) calculateDemandForRespool(
	n respool.ResPool,
	demands map[string]*scalar.Resources) {

	demand := c.getNonSlackResourcesRequirement(n).Add(
		c.getSlackResourcesRequirement(n))

	log.WithFields(log.Fields{
		"respool_ID":   n.ID(),
		"respool_name": n.Name(),
		"demand":       demand,
	}).Debug("Demand for resource pool")

	resConfig := n.Resources()
	// Caping the demand with Limit for resource pool
	// If demand is less then limit then we use demand for
	// entitlement calculation otherwise we use limit as demand
	// to cap the allocation till limit.
	limitedDemand := demand
	for kind, res := range resConfig {
		limitedDemand.Set(kind, math.Min(demand.Get(kind), res.GetLimit()))
	}

	log.WithFields(log.Fields{
		"respool_ID":     n.ID(),
		"respool_name":   n.Name(),
		"actual_demand":  demand,
		"limited_demand": limitedDemand,
		"limit":          resConfig,
	}).Debug("Limited Demand for resource pool")

	// Setting the demand to cap at limit for entitlement calculation
	demands[n.ID()] = limitedDemand
}

// distributeRemainingResources distributes the remianing entitlement based
// on demand and share of the resourcepool.
func (c *Calculator) distributeRemainingResources(
	resp respool.ResPool,
	demands map[string]*scalar.Resources,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources,
	totalShare map[string]float64) {
	childs := resp.Children()
	for _, kind := range []string{
		common.CPU,
		common.GPU,
		common.MEMORY,
		common.DISK} {
		remaining := *entitlement
		log.WithFields(log.Fields{
			"kind":       kind,
			"remianing ": remaining.Get(kind),
		}).Debug("Remaining resources before second pass")
		// Second Pass: In the second pass we will distribute the
		// rest of the resources to the resource pools which have
		// higher demand than the reservation.
		// It will also cap the fair share to demand and redistribute the
		// rest of the entitlement to others.
		for remaining.Get(kind) > util.ResourceEpsilon &&
			c.demandExist(demands, kind) {
			log.WithField("remaining", remaining.Get(kind)).
				Debug("Remaining resources")

			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				log.WithFields(log.Fields{
					"respool":   n.Name(),
					"remaining": remaining.Get(kind),
					"kind":      kind,
					"demand":    demands[n.ID()].Get(kind),
				}).Debug("Evaluating respool")

				if remaining.Get(kind) < util.ResourceEpsilon {
					break
				}
				if demands[n.ID()].Get(kind) < util.ResourceEpsilon {
					continue
				}

				value := float64(n.Resources()[kind].Share * entitlement.Get(kind))
				value = float64(value / totalShare[kind])
				log.WithField("value", value).Debug(" value to evaluate ")

				// Checking if demand is less then the current share
				// if yes then cap it to demand and distribute rest
				// to others
				if value > demands[n.ID()].Get(kind) {
					value = demands[n.ID()].Get(kind)
					demands[n.ID()].Set(kind, 0)
				} else {
					demands[n.ID()].Set(
						kind, float64(demands[n.ID()].Get(kind)-value))
				}

				if remaining.Get(kind) > value {
					remaining.Set(kind, float64(remaining.Get(kind)-value))
				} else {
					// Caping the fare share with the remaining
					// resources for resource kind
					value = remaining.Get(kind)
					remaining.Set(kind, float64(0))
				}
				log.WithField("value", value).
					Debug(" value after evaluation ")

				value += assignments[n.ID()].Get(kind)
				assignments[n.ID()].Set(kind, value)
				log.WithFields(log.Fields{
					"respool":    n.Name(),
					"assignment": value,
				}).Debug("Setting assignment for this respool")

				log.WithFields(log.Fields{
					"respool_resources":              n.Resources(),
					"respool_name":                   n.Name(),
					"demand_cap_by_share_assignment": assignments[n.ID()],
					"demand_not_satisfied":           demands[n.ID()],
				}).Info("Second pass completed for respool")
			}
			*entitlement = remaining
		}
	}
}

// distributeUnclaimedResources is the THIRD PHASE of the
// entitlement calculation. Third phase is to distribute the
// free resources in the cluster which is remaining after
// distribution to all the resource pools which had demand.
// This is in anticipation of respools can get some more workloads
// in between two entitlement cycles
func (c *Calculator) distributeUnclaimedResources(
	resp respool.ResPool,
	entitlement *scalar.Resources,
	assignments map[string]*scalar.Resources) {
	childs := resp.Children()
	for _, kind := range []string{
		common.CPU,
		common.GPU,
		common.MEMORY,
		common.DISK} {
		// Third pass : Now all the demand is been satisfied
		// we need to distribute the rest of the entitlement
		// to all the nodes for the anticipation of some work
		// load and not starve everybody till next cycle
		if entitlement.Get(kind) > util.ResourceEpsilon {
			totalChildShare := c.getChildShare(resp, kind)
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				nshare := n.Resources()[kind].Share
				value := assignments[n.ID()].Get(kind)
				if nshare > 0 {
					value += float64(nshare / totalChildShare *
						entitlement.Get(kind))
				}

				// We need to cap the limit here for free resources
				// as we can not give more then limit to resource pool
				if value > n.Resources()[kind].GetLimit() {
					assignments[n.ID()].Set(
						kind,
						n.Resources()[kind].GetLimit(),
					)
				} else {
					assignments[n.ID()].Set(kind, value)
				}
				log.WithFields(log.Fields{
					"respool_name":      n.Name(),
					"respool_resources": n.Resources(),
					"final_assignment":  assignments[n.ID()],
				}).Info("Third pass completed for respool")
			}
		}
	}
}

// getNonSlackResourcesRequirement returns the total non-revocable resources
// allocated + demand (pending for launch) for non-revocable tasks
func (c *Calculator) getNonSlackResourcesRequirement(
	n respool.ResPool) *scalar.Resources {
	return n.GetNonSlackAllocatedResources().Add(n.GetDemand())
}
