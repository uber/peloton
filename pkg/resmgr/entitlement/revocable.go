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
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/scalar"
)

// Setting slack entitlement is a 3 phase process for revocable resources
// and non-revocable resources derived from total entitlement (previous step)
//
// Setting non-slack entitlement = total_entitlement - slack_entitlement
//
// 1 Calculate assignments for non-revocable resources [mem,disk,gpu]
// capped by slack limit.
//
// 2 For revocable tasks, distribute revocable resources to satisfy demand
//
// 3 Once the demand is zero, distribute remaining based on share
//   for revocable tasks
func (c *Calculator) setSlackAndNonSlackEntitlementForChildren(
	resp respool.ResPool) {
	if resp == nil {
		return
	}

	childs := resp.Children()

	slackEntitlement := resp.GetSlackEntitlement().Clone()
	slackAssignments := make(map[string]*scalar.Resources)
	slackDemands := make(map[string]*scalar.Resources)
	totalShare := make(map[string]float64)

	log.WithFields(log.Fields{
		"respool_name":      resp.Name(),
		"respool_id":        resp.ID(),
		"slack_entitlement": slackEntitlement.String(),
	}).Info("Starting Slack Entitlement cycle for respool")

	// This is first phase for assignment capped at slack limit
	c.calculateSlackAssignmentFromSlackLimit(
		resp,
		slackDemands,
		slackEntitlement,
		slackAssignments,
		totalShare)

	// This is second phase for revocable/slack resources to satisfy demand
	c.distrubuteSlackResourcesToSatisfyDemand(
		resp,
		slackDemands,
		slackEntitlement,
		slackAssignments,
		totalShare)

	// Third phase for slack entitlement calculation to distribute
	// any revocable resources [cpus] left
	c.distributeUnclaimedSlackResources(
		resp,
		slackEntitlement,
		slackAssignments)

	// Update each resource pool metrics post entitlement calculation
	resp.UpdateResourceMetrics()

	log.WithFields(log.Fields{
		"respool_name": resp.Name(),
		"respool_id":   resp.ID(),
	}).Info("Completed Slack Entitlement cycle for respool")

	// Now setting entitlement for all the children and call
	// for their children recursively
	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)

		n.SetSlackEntitlement(slackAssignments[n.ID()])
		c.setSlackAndNonSlackEntitlementForChildren(n)
	}
}

// calculateSlackAssignmentFromSlackLimit calculates assigments
// for non-revocable resources [mem,disk,gpu] to cap at slack limit
// and revocable resources [cpus] equal to allocation.
func (c *Calculator) calculateSlackAssignmentFromSlackLimit(
	resp respool.ResPool,
	slackDemands map[string]*scalar.Resources,
	slackEntitlement *scalar.Resources,
	slackAssignments map[string]*scalar.Resources,
	totalShare map[string]float64) {
	childs := resp.Children()
	cloneSlackEntitlement := slackEntitlement.Clone()

	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)

		// CPU is the only revocable resource
		slackDemands[n.ID()] = &scalar.Resources{
			CPU: n.GetSlackDemand().CPU,
		}
		totalShare[common.CPU] += n.Resources()[common.CPU].GetShare()

		// get the requirements
		// Non-Revocable resources demand & allocation can change between calculating
		// total entitlement versus setting non-slack entitlement.
		// As this will be taken care in next iteration, it is ok.
		// ToDo: Use same demand + allocation throughout entitlement calculation
		slackRequirement := c.getSlackResourcesRequirement(n)
		nonSlackRequirement := c.getNonSlackResourcesRequirement(n)

		// lets see if total-entitlement > non-revoc requirement
		// this is done to satisfy the requirements for non-revocable tasks first,
		// any resources left after are given for revocable tasks
		remainingForSlack := n.GetEntitlement().Subtract(nonSlackRequirement)

		// Here remainingForSlack could be zero, in that case we don;t give
		// any resources for revocable tasks.
		//
		// If it is non-zero we give at most the required resources and not
		// all. These resources given for revocable jobs is capped at the slackLimit
		//
		// Get MIN(available, requirement) of non-revocable resources for revocable tasks
		slackAssignment := scalar.Min(remainingForSlack, slackRequirement)

		if slackAssignment.Equal(scalar.ZeroResource) {
			// nothing left for revocable tasks.
			// set the non-revocable entitlement = total entitlement
			n.SetNonSlackEntitlement(n.GetEntitlement())
		} else {
			// non revocable requirement is less than the total entitlement
			// lets satisfy all that non-revocable requirement + elastic resources.
			n.SetNonSlackEntitlement(n.GetEntitlement().Subtract(slackAssignment))
		}

		slackAssignment.CPU = n.GetSlackAllocatedResources().GetCPU()
		slackAssignments[n.ID()] = slackAssignment.Clone()
		cloneSlackEntitlement = cloneSlackEntitlement.Subtract(slackAssignment)
		log.WithFields(log.Fields{
			"respool_id":        n.ID(),
			"respool_name":      n.Name(),
			"slack_available":   remainingForSlack.String(),
			"slack_requirement": slackRequirement.String(),
			"slack_assignment":  slackAssignment.String(),
		}).Info("First phase of slack entitlement calculation completed")
	}
	slackEntitlement.Copy(cloneSlackEntitlement)
}

// distrubuteSlackResourcesToSatisfyDemand, so far slack assignment is set for
// non-revocable resources [mem,disk,gpu] and allocation of slack/revocable cpus.
// this phase will satisfy revocable cpus demand using fair share distribution
// of remaining revocable resources after allocation.
func (c *Calculator) distrubuteSlackResourcesToSatisfyDemand(
	resp respool.ResPool,
	slackDemands map[string]*scalar.Resources,
	slackEntitlement *scalar.Resources,
	slackAssignments map[string]*scalar.Resources,
	totalShare map[string]float64) {
	remaining := *slackEntitlement
	kind := common.CPU
	childs := resp.Children()

	for remaining.Get(kind) > util.ResourceEpsilon &&
		c.demandExist(slackDemands, kind) {
		for e := childs.Front(); e != nil; e = e.Next() {
			n := e.Value.(respool.ResPool)
			if remaining.Get(kind) < util.ResourceEpsilon {
				break
			}
			if slackDemands[n.ID()].Get(kind) < util.ResourceEpsilon {
				continue
			}

			// calculate fair share value that can satisfy demand for
			// slack or revocable resources [cpus]
			value := float64(n.Resources()[kind].Share *
				slackEntitlement.Get(kind))
			value = float64(value / totalShare[kind])

			// Checking if demand is less then the current share
			// if yes then cap it to demand and distribute rest
			// to others
			if value > slackDemands[n.ID()].Get(kind) {
				value = slackDemands[n.ID()].Get(kind)
				slackDemands[n.ID()].Set(kind, 0)
			} else {
				slackDemands[n.ID()].Set(
					kind, float64(slackDemands[n.ID()].Get(kind)-value))
			}

			if remaining.Get(kind) > value {
				remaining.Set(kind, float64(remaining.Get(kind)-value))
			} else {
				// Caping the fair share with the remaining
				// resources for resource kind
				value = remaining.Get(kind)
				remaining.Set(kind, float64(0))
			}

			value += slackAssignments[n.ID()].Get(kind)
			slackAssignments[n.ID()].Set(kind, value)
			log.WithFields(log.Fields{
				"respool_name":               n.Name(),
				"respool_id":                 n.ID(),
				"slack_assignment":           slackAssignments[n.ID()],
				"slack_demand_not_satisfied": slackDemands[n.ID()],
			}).Info("Second phase of slack entitlement calculation completed")
		}
		*slackEntitlement = remaining
	}
}

// distributeUnclaimedSlackResources distributes remanining revocable cpus
// for future demand till next entitlement cycle begins.
func (c *Calculator) distributeUnclaimedSlackResources(
	resp respool.ResPool,
	slackEntitlement *scalar.Resources,
	slackAssignments map[string]*scalar.Resources) {
	childs := resp.Children()
	for _, kind := range []string{
		common.CPU} {
		// Third pass : Now all the demand is been satisfied
		// we need to distribute the rest of the entitlement
		// to all the nodes for the anticipation of some work
		// load and not starve everybody till next cycle
		if slackEntitlement.Get(kind) > util.ResourceEpsilon {
			totalChildShare := c.getChildShare(resp, kind)
			for e := childs.Front(); e != nil; e = e.Next() {
				n := e.Value.(respool.ResPool)
				nshare := n.Resources()[kind].Share
				value := slackAssignments[n.ID()].Get(kind)
				if nshare > 0 {
					value += float64(nshare / totalChildShare * slackEntitlement.Get(kind))
				}
				slackAssignments[n.ID()].Set(kind, value)
				log.WithFields(log.Fields{
					"respool_name":           n.Name(),
					"respool_id":             n.ID(),
					"final_slack_assignment": slackAssignments[n.ID()],
				}).Info("Third phase of slack entitlement calculation completed")
			}
		}
	}
}

// getSlackResourcesRequirement returns the non-revocable resources required
// for revocable tasks capped to slack limit.
func (c *Calculator) getSlackResourcesRequirement(
	n respool.ResPool) *scalar.Resources {
	// Revocable tasks are limited for non-revocable resources [mem,disk,gpu]
	// slack limited = Min(slack limit, revocable tasks: demand + allocation)
	slackRequest := n.GetSlackAllocatedResources().Add(n.GetSlackDemand())
	return scalar.Min(n.GetSlackLimit(), slackRequest)
}
