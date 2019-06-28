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

package host

import (
	"sync"
	"sync/atomic"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	host "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/hostmgr/util"

	log "github.com/sirupsen/logrus"
	uatomic "github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
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
	OperatorClient         mpb.MasterOperatorClient
	MaintenanceHostInfoMap MaintenanceHostInfoMap
	SlackResourceTypes     []string
	Scope                  tally.Scope
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

	count := 0
	for _, agent := range agents.GetAgents() {
		hostname := agent.GetAgentInfo().GetHostname()
		if len(loader.MaintenanceHostInfoMap.GetDrainingHostInfos([]string{hostname})) != 0 {
			continue
		}
		m.RegisteredAgents[hostname] = agent
		count++
		go getResourcesByType(
			agent.GetTotalResources(),
			outchan,
			loader.SlackResourceTypes)
	}

	for i := 0; i < count; i++ {
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
	// AddHostInfo adds a HostInfo to the map. AddHostInfo is called when a host
	// transitions to a maintenance states (DRAINING and DRAINED).
	AddHostInfo(hostInfo *host.HostInfo)
	// RemoveHostInfo removes the host's hostInfo from
	// the relevant host state based list of hostInfos
	// RemoveHostInfo is needed to remove entries of hosts from the map when
	// hosts transition from one of the maintenance states to UP state.
	RemoveHostInfo(host string)
	// UpdateHostState updates the HostInfo.HostState of the specified
	// host from 'from' state to 'to' state.
	UpdateHostState(hostname string, from host.HostState, to host.HostState) error
	// ClearAndFillMap clears the content of the
	// map and fills the map with the given host infos
	ClearAndFillMap(hostInfos []*host.HostInfo)
}

// maintenanceHostInfoMap implements MaintenanceHostInfoMap interface
type maintenanceHostInfoMap struct {
	lock          sync.RWMutex
	metrics       *Metrics
	drainingHosts map[string]*host.HostInfo
	downHosts     map[string]*host.HostInfo
}

// NewMaintenanceHostInfoMap returns a new MaintenanceHostInfoMap
func NewMaintenanceHostInfoMap(scope tally.Scope) MaintenanceHostInfoMap {
	return &maintenanceHostInfoMap{
		metrics:       NewMetrics(scope.SubScope("maintenance_map")),
		drainingHosts: make(map[string]*host.HostInfo),
		downHosts:     make(map[string]*host.HostInfo),
	}
}

// GetDrainingHostInfos returns HostInfo of the specified DRAINING hosts
// If the hostFilter is empty then HostInfos of all DRAINING hosts is returned
func (m *maintenanceHostInfoMap) GetDrainingHostInfos(hostFilter []string) []*host.HostInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var hostInfos []*host.HostInfo
	if len(hostFilter) == 0 {
		for _, hostInfo := range m.drainingHosts {
			hostInfos = append(hostInfos, hostInfo)
		}
		return hostInfos
	}

	for _, host := range hostFilter {
		if hostInfo, ok := m.drainingHosts[host]; ok {
			hostInfos = append(hostInfos, hostInfo)
		}
	}
	return hostInfos
}

// GetDownHosts returns HostInfo of the specified DOWN hosts
// If the hostFilter is empty then HostInfos of all DOWN hosts is returned
func (m *maintenanceHostInfoMap) GetDownHostInfos(hostFilter []string) []*host.HostInfo {
	m.lock.RLock()
	defer m.lock.RUnlock()

	var hostInfos []*host.HostInfo
	if len(hostFilter) == 0 {
		for _, hostInfo := range m.downHosts {
			hostInfos = append(hostInfos, hostInfo)
		}
		return hostInfos
	}

	for _, host := range hostFilter {
		if hostInfo, ok := m.downHosts[host]; ok {
			hostInfos = append(hostInfos, hostInfo)
		}
	}
	return hostInfos
}

// AddHostInfo adds hostInfo to the specified host state bucket
func (m *maintenanceHostInfoMap) AddHostInfo(
	hostInfo *host.HostInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()

	switch hostInfo.State {
	case host.HostState_HOST_STATE_DRAINING:
		m.drainingHosts[hostInfo.GetHostname()] = hostInfo
	case host.HostState_HOST_STATE_DOWN:
		m.downHosts[hostInfo.GetHostname()] = hostInfo
	}

	m.metrics.DrainingHosts.Update(float64(len(m.drainingHosts)))
	m.metrics.DownHosts.Update(float64(len(m.downHosts)))
}

// RemoveHostInfo removes the host's hostInfo from
// the relevant host state based list of hostInfos
func (m *maintenanceHostInfoMap) RemoveHostInfo(host string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.drainingHosts, host)
	delete(m.downHosts, host)

	m.metrics.DrainingHosts.Update(float64(len(m.drainingHosts)))
	m.metrics.DownHosts.Update(float64(len(m.downHosts)))
}

// UpdateHostState updates the HostInfo.HostState of the specified
// host from 'from' state to 'to' state
func (m *maintenanceHostInfoMap) UpdateHostState(
	hostname string,
	from host.HostState,
	to host.HostState) error {

	m.lock.Lock()
	defer m.lock.Unlock()

	if to != host.HostState_HOST_STATE_DRAINING &&
		to != host.HostState_HOST_STATE_DOWN {
		return yarpcerrors.InvalidArgumentErrorf("invalid target state")
	}

	if from == to {
		return yarpcerrors.
			InvalidArgumentErrorf("current and target states cannot be same")
	}

	var (
		hostInfo *host.HostInfo
		ok       bool
	)

	switch from {
	case host.HostState_HOST_STATE_DRAINING:
		if hostInfo, ok = m.drainingHosts[hostname]; !ok {
			return yarpcerrors.InvalidArgumentErrorf("host not in expected state")
		}
		delete(m.drainingHosts, hostname)
	case host.HostState_HOST_STATE_DOWN:
		if hostInfo, ok = m.downHosts[hostname]; !ok {
			return yarpcerrors.InvalidArgumentErrorf("host not in expected state")
		}
		delete(m.downHosts, hostname)
	default:
		return yarpcerrors.InvalidArgumentErrorf("invalid current state")
	}

	hostInfo.State = to
	switch to {
	case host.HostState_HOST_STATE_DRAINING:
		m.drainingHosts[hostname] = hostInfo
	case host.HostState_HOST_STATE_DOWN:
		m.downHosts[hostname] = hostInfo
	}

	m.metrics.DrainingHosts.Update(float64(len(m.drainingHosts)))
	m.metrics.DownHosts.Update(float64(len(m.downHosts)))
	return nil
}

func (m *maintenanceHostInfoMap) ClearAndFillMap(hostInfos []*host.HostInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()

	for hostname := range m.drainingHosts {
		delete(m.drainingHosts, hostname)
	}

	for hostname := range m.downHosts {
		delete(m.downHosts, hostname)
	}

	for _, hostInfo := range hostInfos {
		switch hostInfo.State {
		case host.HostState_HOST_STATE_DRAINING:
			m.drainingHosts[hostInfo.GetHostname()] = hostInfo
		case host.HostState_HOST_STATE_DOWN:
			m.downHosts[hostInfo.GetHostname()] = hostInfo
		}
	}

	m.metrics.DrainingHosts.Update(float64(len(m.drainingHosts)))
	m.metrics.DownHosts.Update(float64(len(m.downHosts)))
}
