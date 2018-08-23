package host

import (
	"sync/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"

	"code.uber.internal/infra/peloton/hostmgr/scalar"
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

	Capacity scalar.Resources
}

// ReportCapacityMetrics into given metric scope.
func (a *AgentMap) ReportCapacityMetrics(scope tally.Scope) {
	// subScope := scope.SubScope(_subScopeName)
	scope.Gauge("cpus").Update(a.Capacity.GetCPU())
	scope.Gauge("mem").Update(a.Capacity.GetMem())
	scope.Gauge("disk").Update(a.Capacity.GetDisk())
	scope.Gauge("gpus").Update(a.Capacity.GetGPU())
	scope.Gauge("registered_hosts").Update(float64(len(a.RegisteredAgents)))
}

// Atomic pointer to singleton instance.
var agentInfoMap atomic.Value

// GetAgentInfo return agent info from global map.
func GetAgentInfo(agentID *mesos.AgentID) *mesos.AgentInfo {
	m := GetAgentMap()
	if m == nil {
		return nil
	}

	return m.RegisteredAgents[agentID.GetValue()].GetAgentInfo()
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
	OperatorClient mpb.MasterOperatorClient

	Scope tally.Scope
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
	}

	for _, agent := range agents.GetAgents() {
		info := agent.GetAgentInfo()
		id := info.GetId().GetValue()
		m.RegisteredAgents[id] = agent
		resources := scalar.FromMesosResources(info.GetResources())
		m.Capacity = m.Capacity.Add(resources)
	}

	agentInfoMap.Store(m)
	m.ReportCapacityMetrics(loader.Scope)

	log.WithField("num_agents", len(m.RegisteredAgents)).
		Debug("Refreshed agent map")
}
