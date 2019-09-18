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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	pbhost "github.com/uber/peloton/.gen/peloton/api/v0/host"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	host_util "github.com/uber/peloton/pkg/hostmgr/util"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	log "github.com/sirupsen/logrus"
	uatomic "github.com/uber-go/atomic"
	"github.com/uber-go/tally"
)

const _defaultCassandraTimeout = 10 * time.Second

// ResourceCapacity contains total quantity of various kinds of host resources.
type ResourceCapacity struct {
	// Physical capacity.
	Physical scalar.Resources
	// Revocable capacity.
	Slack scalar.Resources
}

// AgentMap is a placeholder from agent id to agent related information.
// Note that AgentInfo is immutable as of Mesos 1.3.
type AgentMap struct {
	// Registered agent details by id.
	RegisteredAgents map[string]*mesos_master.Response_GetAgents_Agent

	// Resource capacity of all hosts.
	HostCapacities map[string]*ResourceCapacity

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
	HostInfoOps        ormobjects.HostInfoOps // DB ops for host_info table
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
		HostCapacities:   make(map[string]*ResourceCapacity),
		Capacity:         scalar.Resources{},
		SlackCapacity:    scalar.Resources{},
	}

	wg := &sync.WaitGroup{}

	ctx, cancel := context.WithTimeout(context.Background(), _defaultCassandraTimeout)
	defer cancel()

	hostInfosFromDB, err := loader.HostInfoOps.GetAll(ctx)
	if err != nil {
		log.WithError(err).Error("failed to get host infos from DB")
	}

	hostsInDB := make(map[string]bool)
	for _, hostInfo := range hostInfosFromDB {
		hostsInDB[hostInfo.GetHostname()] = true
	}

	response, err := loader.OperatorClient.GetMaintenanceStatus()
	if err != nil {
		log.WithError(err).Error("failed to get mesos maintenance status")
		return
	}

	hostsInDrainingState := make(map[string]bool)
	for _, drainingMachine := range response.GetStatus().GetDrainingMachines() {
		hostsInDrainingState[drainingMachine.GetId().GetHostname()] = true
	}

	for _, agent := range agents.GetAgents() {
		ctx, cancel := context.WithTimeout(context.Background(), _defaultCassandraTimeout)
		defer cancel()

		hostname := agent.GetAgentInfo().GetHostname()

		// if the host is not present in DB, create an entry for the host in DB
		if !hostsInDB[hostname] {
			ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(agent.GetPid())
			if err != nil {
				log.WithError(err).
					WithField("host", hostname).
					Error("failed to get ip for host")
				continue
			}

			if err = loader.HostInfoOps.Create(
				ctx,
				hostname,
				ip,
				pbhost.HostState_HOST_STATE_UP,
				pbhost.HostState_HOST_STATE_UP,
				map[string]string{},
				"",
				"",
			); err != nil {
				log.WithField("host", hostname).
					WithError(err).
					Error("failed to create entry in DB")
			}
		}

		// skip hosts in maintenance from cluster capacity calculation
		if _, ok := hostsInDrainingState[hostname]; ok {
			continue
		}

		m.RegisteredAgents[hostname] = agent
		capacity := &ResourceCapacity{}
		m.HostCapacities[hostname] = capacity
		wg.Add(1)
		go getResourcesByType(
			agent.GetTotalResources(),
			loader.SlackResourceTypes,
			capacity,
			wg)
	}
	wg.Wait()

	for _, c := range m.HostCapacities {
		m.SlackCapacity = m.SlackCapacity.Add(c.Slack)
		m.Capacity = m.Capacity.Add(c.Physical)
	}

	agentInfoMap.Store(m)
	m.ReportCapacityMetrics(loader.Scope)
}

// getResourcesByType returns supported revocable
// and non-revocable physical resources for an agent.
func getResourcesByType(
	agentResources []*mesos.Resource,
	slackResourceTypes []string,
	hostCapacity *ResourceCapacity,
	wg *sync.WaitGroup) {
	agentRes, _ := scalar.FilterMesosResources(
		agentResources,
		func(r *mesos.Resource) bool {
			if r.GetRevocable() == nil {
				return true
			}
			return host_util.IsSlackResourceType(r.GetName(), slackResourceTypes)
		})

	revRes, nonrevRes := scalar.FilterRevocableMesosResources(agentRes)
	hostCapacity.Slack = scalar.FromMesosResources(revRes)
	hostCapacity.Physical = scalar.FromMesosResources(nonrevRes)
	wg.Done()
}

// IsRegisteredAsPelotonAgent returns true if the host is registered as Peloton agent
func IsRegisteredAsPelotonAgent(hostname, pelotonAgentRole string) bool {
	agentInfo := GetAgentInfo(hostname)
	if agentInfo == nil {
		return false
	}

	// For Peloton agents, AgentInfo->Resources->Reservations->Role would be set
	// to Peloton role because of how agents are configured
	// (check mesos.framework in /config/hostmgr/base.yaml)
	for _, r := range agentInfo.GetResources() {
		reservations := r.GetReservations()
		// look at the latest(active) reservation info and check if the
		// agent is registered to peloton framework. This check is necessary
		// to ensure Peloton does not put into maintenance a host which
		// it does not manage.
		if len(reservations) > 0 &&
			reservations[len(reservations)-1].GetRole() == pelotonAgentRole {
			return true
		}
	}

	return false
}

// IsHostRegistered returns whether the host is registered
func IsHostRegistered(hostname string) bool {
	return GetAgentInfo(hostname) != nil
}

// BuildHostInfoForRegisteredAgents builds host infos for registered agents
func BuildHostInfoForRegisteredAgents() (map[string]*pbhost.HostInfo, error) {
	agentMap := GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return nil, nil
	}
	upHosts := make(map[string]*pbhost.HostInfo)
	for _, agent := range agentMap.RegisteredAgents {
		hostname := agent.GetAgentInfo().GetHostname()
		agentIP, _, err := util.ExtractIPAndPortFromMesosAgentPID(agent.GetPid())
		if err != nil {
			return nil, err
		}
		hostInfo := &pbhost.HostInfo{
			Hostname: hostname,
			Ip:       agentIP,
			State:    pbhost.HostState_HOST_STATE_UP,
		}
		upHosts[hostname] = hostInfo
	}
	return upHosts, nil
}

// GetUpHostIP gets the IP address of a host in UP state
func GetUpHostIP(hostname string) (string, error) {
	agentMap := GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return "", fmt.Errorf("no registered agents")
	}
	if _, ok := agentMap.RegisteredAgents[hostname]; !ok {
		return "", fmt.Errorf("unknown host %s", hostname)
	}
	pid := agentMap.RegisteredAgents[hostname].GetPid()
	ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(pid)
	if err != nil {
		return "", err
	}
	return ip, nil
}
