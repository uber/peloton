package host

import (
	"sync"
	"sync/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	"code.uber.internal/infra/peloton/common"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
	"code.uber.internal/infra/peloton/hostmgr/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
	uatomic "github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

// AgentMap is a placeholder from agent id to agent related information.
// Note that AgentInfo is immutable as of Mesos 1.3.
type AgentMap struct {
	// Registered agent details by id.
	RegisteredAgents map[string]*mesos_master.Response_GetAgents_Agent

	Capacity      scalar.Resources
	SlackCapacity scalar.Resources
}

// ReportCapacityMetrics into given metric scope.
func (a *AgentMap) ReportCapacityMetrics(scope tally.Scope) {
	scope.Gauge(common.MesosCPU).Update(a.Capacity.GetCPU())
	scope.Gauge(common.MesosMem).Update(a.Capacity.GetMem())
	scope.Gauge(common.MesosDisk).Update(a.Capacity.GetDisk())
	scope.Gauge(common.MesosGPU).Update(a.Capacity.GetGPU())
	scope.Gauge("cpus_revocable").Update(a.SlackCapacity.GetCPU())
	scope.Gauge("registered_hosts").Update(float64(len(a.RegisteredAgents)))
}

// Atomic pointer to singleton instance.
var agentInfoMap atomic.Value

// GetAgentInfo return agent info from global map.
func GetAgentInfo(hostname string) *mesos.AgentInfo {
	m := GetAgentMap()
	if m == nil {
		return nil
	}

	return m.RegisteredAgents[hostname].GetAgentInfo()
}

// GetAgentMap returns a full map of all registered agents. Note that caller
// should not mutable the content since it's not protected by any lock.
func GetAgentMap() *AgentMap {
	ptr := agentInfoMap.Load()
	if ptr == nil {
		return nil
	}

	v, ok := ptr.(*AgentMap)
	if !ok {
		return nil
	}
	return v
}

// Loader loads hostmap from Mesos and stores in global singleton.
type Loader struct {
	sync.Mutex
	OperatorClient     mpb.MasterOperatorClient
	SlackResourceTypes []string
	Scope              tally.Scope
}

// Load hostmap into singleton.
func (loader *Loader) Load(_ *uatomic.Bool) {
	agents, err := loader.OperatorClient.Agents()
	if err != nil {
		log.WithError(err).Warn("Cannot refresh agent map from master")
		return
	}

	m := &AgentMap{
		RegisteredAgents: make(map[string]*mesos_master.Response_GetAgents_Agent),
		Capacity:         scalar.Resources{},
		SlackCapacity:    scalar.Resources{},
	}

	for _, agent := range agents.GetAgents() {
		m.RegisteredAgents[agent.GetAgentInfo().GetHostname()] = agent

		revocable, nonRevocable := getResourcesByType(
			agent.GetTotalResources(),
			loader.SlackResourceTypes)
		m.SlackCapacity = m.SlackCapacity.Add(revocable)
		m.Capacity = m.Capacity.Add(nonRevocable)
	}

	agentInfoMap.Store(m)
	m.ReportCapacityMetrics(loader.Scope)
}

// getResourcesByType returns supported revocable
// and non-revocable physical resources for an agent.
func getResourcesByType(
	agentResources []*mesos.Resource,
	slackResourceTypes []string) (scalar.Resources, scalar.Resources) {
	agentRes, _ := scalar.FilterMesosResources(
		agentResources,
		func(r *mesos.Resource) bool {
			if r.GetRevocable() == nil {
				return true
			}
			return util.IsSlackResourceType(r.GetName(), slackResourceTypes)
		})

	revRes, nonrevRes := scalar.FilterRevocableMesosResources(agentRes)
	revocable := scalar.FromMesosResources(revRes)
	nonRevocable := scalar.FromMesosResources(nonrevRes)
	return revocable, nonRevocable
}
