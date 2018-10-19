package host

import (
	"fmt"
	"sync"
	"sync/atomic"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"

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

	outchan := make(chan func() (scalar.Resources, scalar.Resources))

	for _, agent := range agents.GetAgents() {
		m.RegisteredAgents[agent.GetAgentInfo().GetHostname()] = agent

		go getResourcesByType(
			agent.GetTotalResources(),
			outchan,
			loader.SlackResourceTypes)
	}

	for i := 0; i < len(agents.GetAgents()); i++ {
		revocable, nonRevocable := (<-outchan)()
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
	outchan chan func() (scalar.Resources, scalar.Resources),
	slackResourceTypes []string) {
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
	outchan <- (func() (
		scalar.Resources,
		scalar.Resources) {
		return revocable, nonRevocable
	})
}

// MaintenanceHostInfoMap defines an interface of a map of
// HostInfos of hosts in maintenance
// TODO: remove this once GetAgents Response has maintenance state of the agents
type MaintenanceHostInfoMap interface {
	// GetDrainingHostInfos returns HostInfo of the specified DRAINING hosts.
	// If the hostFilter is empty then HostInfos of all DRAINING hosts is returned.
	GetDrainingHostInfos(hostFilter []string) []*host.HostInfo
	// GetDownHostInfos returns HostInfo of hosts in DOWN state
	// If the hostFilter is empty then HostInfos of all DOWN hosts is returned.
	GetDownHostInfos(hostFilter []string) []*host.HostInfo
	// AddHostInfos adds HostInfos to the map. AddHostInfos is called when one
	// or more hosts transition to maintenance states (DRAINING and DRAINED).
	AddHostInfos(hostInfos []*host.HostInfo)
	// RemoveHostInfos removes the HostInfos of the specified hosts from the map
	// RemoveHostInfos is needed to remove entries of hosts from the map when
	// hosts transition from one of the maintenance states to UP state.
	RemoveHostInfos(hosts []string)
	// UpdateHostState updates the HostInfo.HostState of the specified
	// host from 'from' state to 'to' state.
	UpdateHostState(hostname string, from host.HostState, to host.HostState) error
}

// maintenanceHostInfoMap implements MaintenanceHostInfoMap interface
type maintenanceHostInfoMap struct {
	lock          sync.RWMutex
	drainingHosts map[string]*host.HostInfo
	downHosts     map[string]*host.HostInfo
}

// NewMaintenanceHostInfoMap returns a new MaintenanceHostInfoMap
func NewMaintenanceHostInfoMap() MaintenanceHostInfoMap {
	return &maintenanceHostInfoMap{
		drainingHosts: make(map[string]*host.HostInfo),
		downHosts:     make(map[string]*host.HostInfo),
	}
}

// GetDrainingHostInfos returns HostInfo of the specified DRAINING hosts
// If the hostFilter is empty then HostInfos of all DRAINING hosts is returned
func (h *maintenanceHostInfoMap) GetDrainingHostInfos(hostFilter []string) []*host.HostInfo {
	h.lock.RLock()
	defer h.lock.RUnlock()

	var hostInfos []*host.HostInfo
	if len(hostFilter) == 0 {
		for _, hostInfo := range h.drainingHosts {
			hostInfos = append(hostInfos, hostInfo)
		}
		return hostInfos
	}
	for _, host := range hostFilter {
		if hostInfo, ok := h.drainingHosts[host]; ok {
			hostInfos = append(hostInfos, hostInfo)
		}
	}
	return hostInfos
}

// GetDownHosts returns HostInfo of the specified DOWN hosts
// If the hostFilter is empty then HostInfos of all DOWN hosts is returned
func (h *maintenanceHostInfoMap) GetDownHostInfos(hostFilter []string) []*host.HostInfo {
	h.lock.RLock()
	defer h.lock.RUnlock()

	var hostInfos []*host.HostInfo
	if len(hostFilter) == 0 {
		for _, hostInfo := range h.downHosts {
			hostInfos = append(hostInfos, hostInfo)
		}
		return hostInfos
	}
	for _, host := range hostFilter {
		if hostInfo, ok := h.downHosts[host]; ok {
			hostInfos = append(hostInfos, hostInfo)
		}
	}
	return hostInfos
}

// AddHostInfo adds hostInfo to the specified host state bucket
func (h *maintenanceHostInfoMap) AddHostInfos(
	hostInfos []*host.HostInfo) {
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, hostInfo := range hostInfos {
		switch hostInfo.State {
		case host.HostState_HOST_STATE_DRAINING:
			h.drainingHosts[hostInfo.GetHostname()] = hostInfo
		case host.HostState_HOST_STATE_DOWN:
			h.downHosts[hostInfo.GetHostname()] = hostInfo
		}
	}
}

// RemoveHostInfos removes the hostInfos of the specified hosts from
// the specified host state bucket
func (h *maintenanceHostInfoMap) RemoveHostInfos(hosts []string) {
	h.lock.Lock()
	defer h.lock.Unlock()

	for _, host := range hosts {
		delete(h.drainingHosts, host)
		delete(h.downHosts, host)
	}
}

// UpdateHostState updates the HostInfo.HostState of the specified
// host from 'from' state to 'to' state
func (h *maintenanceHostInfoMap) UpdateHostState(
	hostname string,
	from host.HostState,
	to host.HostState) error {
	if to != host.HostState_HOST_STATE_DRAINING &&
		to != host.HostState_HOST_STATE_DOWN {
		return fmt.Errorf("invalid target state")
	}

	var (
		hostInfo *host.HostInfo
		ok       bool
	)

	switch from {
	case host.HostState_HOST_STATE_DRAINING:
		if hostInfo, ok = h.drainingHosts[hostname]; !ok {
			return fmt.Errorf("host not in expected state")
		}
		delete(h.drainingHosts, hostname)
	case host.HostState_HOST_STATE_DOWN:
		if hostInfo, ok = h.downHosts[hostname]; !ok {
			return fmt.Errorf("host not in expected state")
		}
		delete(h.downHosts, hostname)
	default:
		return fmt.Errorf("invalid current state")
	}
	hostInfo.State = to
	switch to {
	case host.HostState_HOST_STATE_DRAINING:
		h.drainingHosts[hostname] = hostInfo
	case host.HostState_HOST_STATE_DOWN:
		h.downHosts[hostname] = hostInfo
	}
	return nil
}
