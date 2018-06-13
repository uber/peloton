package host

import (
	"fmt"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common/constraints"
	"code.uber.internal/infra/peloton/hostmgr/scalar"

	log "github.com/sirupsen/logrus"
)

// Matcher keeps track of matched host offers for given constraints.
type Matcher struct {
	// Its a host filter which is been passed to Matcher to match hosts
	hostFilter *hostsvc.HostFilter
	// evaluator is evaluator for the constraints
	evaluator constraints.Evaluator
	// agentMap is the map of the agent id -> resources
	agentMap map[string]scalar.Resources
	// agentInfoMap is the Host -> AgentInfo
	agentInfoMap *AgentInfoMap
	// Its the GetHosts result stored in the matcher object
	resultHosts map[string]*mesos.AgentInfo
}

// NewMatcher returns a new instance of Matcher.
func NewMatcher(
	hostFilter *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
) *Matcher {
	return &Matcher{
		hostFilter:   hostFilter,
		evaluator:    evaluator,
		agentMap:     createAgentResourceMap(GetAgentMap()),
		agentInfoMap: GetAgentMap(),
		resultHosts:  make(map[string]*mesos.AgentInfo),
	}
}

// GetMatchingHosts tries to match the hosts through Host filter
// and it returns the agentId-> AgentInfo for the matched hosts.
// If the filter does not match, it returns the error
func (m *Matcher) GetMatchingHosts() (map[string]*mesos.AgentInfo, *hostsvc.GetHostsFailure) {
	result := m.matchHostsFilter(m.agentMap, m.hostFilter, m.evaluator, m.agentInfoMap)
	if result == hostsvc.HostFilterResult_MATCH {
		return m.resultHosts, nil
	}
	return nil, &hostsvc.GetHostsFailure{
		Message: fmt.Sprintf("could not return matching hosts %s", result.String()),
	}
}

// matchHostFilter takes the host filter with the agent maps
// and tries to match the constraints and resources for the
// specified host. It returns the reason as part of
// hostsvc.HostFilterResult if it matches ot not.
func (m *Matcher) matchHostFilter(
	agentID string,
	resource scalar.Resources,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	agentInfoMap *AgentInfoMap) hostsvc.HostFilterResult {
	// tries to get the resource requirement from the host filter
	if min := c.GetResourceConstraint().GetMinimum(); min != nil {
		// Checks if the resources in the host are enough for the
		// filter , if not return INSUFFICIENT_RESOURCES
		if scalarMin := scalar.FromResourceConfig(min); !resource.Contains(scalarMin) {
			return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
		}
	}

	// tries to get the constraints from the host filter
	if hc := c.GetSchedulingConstraint(); hc != nil {
		hostname := agentInfoMap.RegisteredAgents[agentID].GetHostname()
		lv := constraints.GetHostLabelValues(
			hostname,
			agentInfoMap.RegisteredAgents[agentID].GetAttributes(),
		)
		// evaluate the constraints
		result, err := evaluator.Evaluate(hc, lv)
		if err != nil {
			// constraints evaluation returns error
			log.WithError(err).
				Error("Error when evaluating input constraint")
			return hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS
		}
		// evaluating result of constraints evaluator
		switch result {
		case constraints.EvaluateResultMatch:
			fallthrough
		case constraints.EvaluateResultNotApplicable:
			log.WithFields(log.Fields{
				"values":     lv,
				"hostname":   hostname,
				"constraint": hc,
			}).Debug("Attributes match constraint")
		default:
			log.WithFields(log.Fields{
				"values":     lv,
				"hostname":   hostname,
				"constraint": hc,
			}).Debug("Attributes do not match constraint")
			return hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS
		}
	}

	return hostsvc.HostFilterResult_MATCH
}

// matchHostsFilter goes through all the list of nodes
// and matches each host with filter, if success add to result set
func (m *Matcher) matchHostsFilter(
	agentMap map[string]scalar.Resources,
	c *hostsvc.HostFilter,
	evaluator constraints.Evaluator,
	agentInfoMap *AgentInfoMap) hostsvc.HostFilterResult {

	if agentMap == nil || agentInfoMap == nil {
		return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
	}

	// going through the list of nodes
	for agentID, agentInfo := range agentInfoMap.RegisteredAgents {
		// matching the host with hostfilter
		if result := m.matchHostFilter(agentID, agentMap[agentID], c, evaluator, agentInfoMap); result != hostsvc.HostFilterResult_MATCH {
			continue
		}
		// adding matched host to list of returning hosts
		m.resultHosts[agentID] = agentInfo
	}
	if len(m.resultHosts) > 0 {
		return hostsvc.HostFilterResult_MATCH
	}
	// returning error in case if nothing matched
	return hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES
}

// createAgentResourceMap takes the AgentInfoMap and returs the host to resource map
func createAgentResourceMap(hostMap *AgentInfoMap) map[string]scalar.Resources {
	// host map is nil, return nil
	if hostMap == nil || len(hostMap.RegisteredAgents) <= 0 {
		return nil
	}
	agentResourceMap := make(map[string]scalar.Resources)
	// going through each agent and calculate resources
	for agentID, agentInfo := range hostMap.RegisteredAgents {
		agentResourceMap[agentID] = scalar.FromMesosResources(agentInfo.GetResources())
	}
	return agentResourceMap
}
