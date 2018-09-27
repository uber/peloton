package entitlement

import (
	"fmt"
	"math"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/scalar"
	"code.uber.internal/infra/peloton/util"
)

// Setting entitlement is a 3 phase process for revocable resources
// 1 Calculate assignments for non-revocable resources [mem,disk,gpu]
// capped by slack limit.
//
// 2 For revocable tasks, distribute revocable resources to satisfy demand
//
// 3 Once the demand is zero, distribute remaining based on share
//   for revocable tasks
func (c *Calculator) setSlackEntitlementForChildren(
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
		"respool_name": resp.Name(),
		"respool_id":   resp.ID(),
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

	log.WithFields(log.Fields{
		"slack_entitlement": slackEntitlement.String(),
	}).Info(fmt.Sprintf("Completed Entitlement cycle for respool: %s %s",
		resp.Name(), resp.ID()))

	// Now setting entitlement for all the children and call
	// for their children recursively
	for e := childs.Front(); e != nil; e = e.Next() {
		n := e.Value.(respool.ResPool)

		n.SetSlackEntitlementResources(slackAssignments[n.ID()])
		c.setSlackEntitlementForChildren(n)
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

		// slack demands tracks demand for revocable resources [cpus]
		// per resource pool
		slackDemands[n.ID()] = &scalar.Resources{
			CPU: n.GetSlackDemand().CPU,
		}

		// Create assignment for non-revocable resources [mem,disk,gpu]
		// used by revocable tasks
		// slack assignment = Min(slack limit, demand + allocation)
		slackAssignment := &scalar.Resources{
			MEMORY: math.Min(n.GetSlackLimit().GetMem(),
				n.GetSlackDemand().GetMem()+n.GetSlackAllocatedResources().GetMem()),
			DISK: math.Min(n.GetSlackLimit().GetDisk(),
				n.GetSlackDemand().GetDisk()+n.GetSlackAllocatedResources().GetDisk()),
			GPU: math.Min(n.GetSlackLimit().GetGPU(),
				n.GetSlackDemand().GetGPU()+n.GetSlackAllocatedResources().GetGPU()),
			// Append allocation for revocable resources as-is,
			// because they are not capped to a limit or reservation
			CPU: n.GetSlackAllocatedResources().GetCPU(),
		}

		// Update CPU shares, as Peloton supports revocable cpus only
		totalShare[common.CPU] += n.Resources()[common.CPU].GetShare()
		slackAssignments[n.ID()] = slackAssignment.Clone()
		cloneSlackEntitlement = cloneSlackEntitlement.Subtract(slackAssignment)
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
				value := float64(n.Resources()[kind].Share *
					slackEntitlement.Get(kind))
				value = float64(value / totalChildShare)

				value += slackAssignments[n.ID()].Get(kind)
				slackAssignments[n.ID()].Set(kind, value)
			}
		}
	}
}
