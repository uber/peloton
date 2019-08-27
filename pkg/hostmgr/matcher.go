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

package hostmgr

import (
	"fmt"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/common/constraints"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/util"

	log "github.com/sirupsen/logrus"
)

// Matcher keeps track of matched host offers for given constraints.
type Matcher struct {
	// Its a host filter which is been passed to Matcher to match hosts
	hostFilter *hostsvc.HostFilter
	// evaluator is evaluator for the constraints
	evaluator constraints.Evaluator
	// hostPoolManager is the manager maintains host to host pool map
	hostPoolManager manager.HostPoolManager
	// agentMap is the map of the hostname -> resources
	agentMap map[string]scalar.Resources
	// agentInfoMap is the map of hostname -> agent info
	agentInfoMap *host.AgentMap
	// Its the GetHosts result stored in the matcher object
	resultHosts map[string]*mesos.AgentInfo
}

type filterSlackResources func(resourceType string) bool

// NewMatcher returns a new instance of Matcher.
// hostFilter defines the constraints on matching a host such as resources, revocable.
// evaluator is used to validate constraints such as labels.
func NewMatcher(
	hostFilter *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	hostPoolManager manager.HostPoolManager,
	filter filterSlackResources) *Matcher {
	return &Matcher{
		hostFilter:      hostFilter,
		evaluator:       evaluator,
		hostPoolManager: hostPoolManager,
		agentMap: createAgentResourceMap(
			host.GetAgentMap(),
			hostFilter.GetResourceConstraint(),
			filter),
		agentInfoMap: host.GetAgentMap(),
		resultHosts:  make(map[string]*mesos.AgentInfo),
	}
}

// GetMatchingHosts tries to match the hosts through Host filter
// and it returns the hostname-> AgentInfo for the matched hosts.
// If the filter does not match, it returns the error
func (m *Matcher) GetMatchingHosts() (map[string]*mesos.AgentInfo, *hostsvc.GetHostsFailure) {
	result := m.matchHostsFilter(
		m.agentMap,
		m.hostFilter,
		m.evaluator,
		m.agentInfoMap)
	if result == hostsvc.HostFilterResult_MATCH {
		return m.resultHosts, nil
	}
	return nil, &hostsvc.GetHostsFailure{
		Message: fmt.Sprintf("could not return matching hosts %s", result),
	}
}

// matchHostFilter takes the host filter with the agent maps
// and tries to match the constraints and resources for the
// specified host. It returns the reason as part of
// hostsvc.HostFilterResult if it matches ot not.
func (m *Matcher) matchHostFilter(
	hostname string,
	resource scalar.Resources,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	agentMap *host.AgentMap) hostsvc.HostFilterResult {
	// tries to get the resource requirement from the host filter
	if min := c.GetResourceConstraint().GetMinimum(); min != nil {
		// Checks if the resources in the host are enough for the
		// filter , if not return INSUFFICIENT_RESOURCES
		if scalarMin := scalar.FromResourceConfig(min); !resource.Contains(scalarMin) {
			return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
		}
	}

	hc := c.GetSchedulingConstraint()
	agent := agentMap.RegisteredAgents[hostname].GetAgentInfo()

	// Insert host pool into labels for evaluation.
	var lv constraints.LabelValues
	var err error
	if m.hostPoolManager != nil {
		lv, err = manager.GetHostPoolLabelValues(m.hostPoolManager, hostname)
		if err != nil {
			log.WithError(err).
				WithField("host", hostname).
				Error("Failed to get host pool label")
		}
	}

	return util.MatchSchedulingConstraint(
		hostname, lv, agent.GetAttributes(), hc, evaluator)
}

// matchHostsFilter goes through all the list of nodes
// and matches each host with filter, if success add to result set
func (m *Matcher) matchHostsFilter(
	agentMap map[string]scalar.Resources,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	agentInfoMap *host.AgentMap) hostsvc.HostFilterResult {

	if agentMap == nil || agentInfoMap == nil {
		return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
	}

	// going through the list of nodes
	for hostname, agent := range agentInfoMap.RegisteredAgents {
		// matching the host with hostfilter
		if result := m.matchHostFilter(
			hostname,
			agentMap[hostname],
			c,
			evaluator,
			agentInfoMap); result != hostsvc.HostFilterResult_MATCH {
			continue
		}
		// adding matched host to list of returning hosts
		m.resultHosts[hostname] = agent.GetAgentInfo()
	}
	if len(m.resultHosts) > 0 {
		return hostsvc.HostFilterResult_MATCH
	}
	// returning error in case if nothing matched
	return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
}

// createAgentResourceMap takes the AgentMap, Resource Constraint
// filter Slack Resources func and returns the host to resource map
func createAgentResourceMap(
	hostMap *host.AgentMap,
	resourceConstraint *hostsvc.ResourceConstraint,
	filter filterSlackResources) map[string]scalar.Resources {
	if hostMap == nil || len(hostMap.RegisteredAgents) == 0 {
		return nil
	}

	agentResourceMap := make(map[string]scalar.Resources)
	// going through each agent and calculate resources
	for hostname, agent := range hostMap.RegisteredAgents {
		if resourceConstraint.GetRevocable() {
			revocable, _ := scalar.FilterRevocableMesosResources(
				agent.GetTotalResources())
			agentResourceMap[hostname] = scalar.FromMesosResources(revocable)

			nonRevocable, _ := scalar.FilterMesosResources(
				agent.GetTotalResources(),
				func(r *mesos.Resource) bool {
					if r.GetRevocable() != nil || filter(r.GetName()) {
						return false
					}
					return true
				})
			agentResourceMap[hostname] = agentResourceMap[hostname].
				Add(scalar.FromMesosResources(nonRevocable))
		} else {
			_, nonRevocable := scalar.FilterRevocableMesosResources(
				agent.GetTotalResources())
			agentResourceMap[hostname] = scalar.FromMesosResources(nonRevocable)
		}
	}
	return agentResourceMap
}
