package hostsvc

import (
	"context"
	"strings"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_maintenance "code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"code.uber.internal/infra/peloton/common/stringset"
	hm_host "code.uber.internal/infra/peloton/hostmgr/host"
	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

const (
	ipPortSeparator  = ":"
	slaveIPSeparator = "@"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	agentMap             *hm_host.AgentMap
	maintenanceQueue     queue.MaintenanceQueue
	metrics              *Metrics
	operatorMasterClient mpb.MasterOperatorClient
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	operatorMasterClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue,
	agentMap *hm_host.AgentMap) {
	handler := &serviceHandler{
		agentMap:             agentMap,
		maintenanceQueue:     maintenanceQueue,
		metrics:              NewMetrics(parent.SubScope("hostsvc")),
		operatorMasterClient: operatorMasterClient,
	}
	d.Register(host_svc.BuildHostServiceYARPCProcedures(handler))
	log.Info("Hostsvc handler initialized")
}

// QueryHosts returns the hosts which are in one of the specified states.
func (m *serviceHandler) QueryHosts(
	ctx context.Context,
	request *host_svc.QueryHostsRequest) (*host_svc.QueryHostsResponse, error) {
	m.metrics.QueryHostsAPI.Inc(1)

	var err error
	hostStateSet := stringset.New()
	for _, state := range request.GetHostStates() {
		hostStateSet.Add(state.String())
	}

	// upHosts is map to ensure we don't have 2 entries for DRAINING hosts
	// as the response from Agents() includes info of DRAINING hosts
	var upHosts map[string]*host.HostInfo
	if hostStateSet.Contains(host.HostState_HOST_STATE_UP.String()) {
		upHosts, err = m.getRegisteredAgents()
		if err != nil {
			m.metrics.QueryHostsFail.Inc(1)
			return nil, err
		}
	}

	var clusterStatus *mesos_maintenance.ClusterStatus

	// Get current maintenance status
	response, err := m.operatorMasterClient.GetMaintenanceStatus()
	if err != nil {
		m.metrics.QueryHostsFail.Inc(1)
		return nil, err
	}
	clusterStatus = response.GetStatus()

	var drainingHosts []*host.HostInfo
	if hostStateSet.Contains(host.HostState_HOST_STATE_DRAINING.String()) ||
		hostStateSet.Contains(host.HostState_HOST_STATE_UP.String()) {
		drainingHosts = getDrainingHosts(clusterStatus.DrainingMachines)
		// We need to remove DRAINING hosts from upHosts
		removeFromUpHosts(upHosts, drainingHosts)
	}

	// Build result
	var hostInfos []*host.HostInfo
	for _, hostState := range request.GetHostStates() {
		switch hostState {
		case host.HostState_HOST_STATE_UP:
			for _, hostInfo := range upHosts {
				hostInfos = append(hostInfos, hostInfo)
			}
		case host.HostState_HOST_STATE_DRAINING:
			for _, hostInfo := range drainingHosts {
				hostInfos = append(hostInfos, hostInfo)
			}
		case host.HostState_HOST_STATE_DOWN:
			for _, downMachine := range clusterStatus.DownMachines {
				hostname := downMachine.GetHostname()
				hostInfo := &host.HostInfo{
					Hostname: hostname,
					Ip:       downMachine.GetIp(),
					State:    host.HostState_HOST_STATE_DOWN,
				}
				hostInfos = append(hostInfos, hostInfo)
			}
		}
	}

	m.metrics.QueryHostsSuccess.Inc(1)
	return &host_svc.QueryHostsResponse{
		HostInfos: hostInfos,
	}, nil
}

// StartMaintenance starts maintenance on the specified hosts.
func (m *serviceHandler) StartMaintenance(
	ctx context.Context,
	request *host_svc.StartMaintenanceRequest,
) (*host_svc.StartMaintenanceResponse, error) {
	m.metrics.StartMaintenanceAPI.Inc(1)

	// Get current maintenance schedule
	response, err := m.operatorMasterClient.GetMaintenanceSchedule()
	if err != nil {
		log.WithError(err).Error("Error getting maintenance schedule")
		m.metrics.StartMaintenanceFail.Inc(1)
		return nil, err
	}
	schedule := response.GetSchedule()
	// Set current time as the `start` of maintenance window
	nanos := time.Now().UnixNano()

	// The maintenance duration has no real significance. A machine can be put into
	// maintenance even after its maintenance window has passed. According to Mesos,
	// omitting the duration means that the unavailability will last forever. Since
	// we do not know the duration, we are omitting it.

	// Construct maintenance window
	maintenanceWindow := &mesos_maintenance.Window{
		MachineIds: request.GetMachineIds(),
		Unavailability: &mesos.Unavailability{
			Start: &mesos.TimeInfo{
				Nanoseconds: &nanos,
			},
		},
	}
	// Add maintenance window to schedule
	schedule.Windows = append(schedule.Windows, maintenanceWindow)

	// Post the new schedule
	err = m.operatorMasterClient.UpdateMaintenanceSchedule(schedule)
	if err != nil {
		log.WithError(err).Error("UpdateMaintenanceSchedule failed")
		m.metrics.StartMaintenanceFail.Inc(1)
		return nil, err
	}
	log.WithField("maintenance_schedule", schedule).
		Info("Maintenance Schedule posted to Mesos Master")

	var hosts []string
	for _, machineID := range request.GetMachineIds() {
		hosts = append(hosts, machineID.GetHostname())
	}

	// Enqueue hostnames into maintenance queue to initiate
	// the rescheduling of tasks running on these hosts
	err = m.maintenanceQueue.Enqueue(hosts)
	if err != nil {
		log.WithError(err).
			Error("Failed to enqueue some hosts into maintenance queue")
		return nil, err
	}

	m.metrics.StartMaintenanceSuccess.Inc(1)
	return &host_svc.StartMaintenanceResponse{}, nil
}

// CompleteMaintenance completes maintenance on the specified hosts.
func (m *serviceHandler) CompleteMaintenance(
	ctx context.Context,
	request *host_svc.CompleteMaintenanceRequest,
) (*host_svc.CompleteMaintenanceResponse, error) {
	m.metrics.CompleteMaintenanceAPI.Inc(1)

	// Post to Mesos Master's /machine/up endpoint
	err := m.operatorMasterClient.StopMaintenance(request.GetMachineIds())
	if err != nil {
		log.WithError(err).Error("CompleteMaintenanceForMachine failed")
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return nil, err
	}

	m.metrics.CompleteMaintenanceSuccess.Inc(1)
	return &host_svc.CompleteMaintenanceResponse{}, nil
}

func (m *serviceHandler) getRegisteredAgents() (map[string]*host.HostInfo, error) {
	upHosts := make(map[string]*host.HostInfo)
	for _, agent := range m.agentMap.RegisteredAgents {
		hostname := agent.GetAgentInfo().GetHostname()
		agentIPPort := strings.Split(agent.GetPid(), slaveIPSeparator)[1]
		agentIP := strings.Split(agentIPPort, ipPortSeparator)[0]
		hostInfo := &host.HostInfo{
			Hostname: hostname,
			Ip:       agentIP,
			State:    host.HostState_HOST_STATE_UP,
		}
		upHosts[hostname] = hostInfo
	}
	return upHosts, nil
}

// Build HostInfo from machineID for DRAINING machines
func getDrainingHosts(
	drainingMachines []*mesos_maintenance.ClusterStatus_DrainingMachine,
) []*host.HostInfo {
	var drainingHosts []*host.HostInfo
	for _, drainingMachine := range drainingMachines {
		machineID := drainingMachine.GetId()
		hostname := machineID.GetHostname()
		hostInfo := &host.HostInfo{
			Hostname: hostname,
			Ip:       machineID.GetIp(),
			State:    host.HostState_HOST_STATE_DRAINING,
		}
		drainingHosts = append(drainingHosts, hostInfo)
	}
	return drainingHosts
}

func removeFromUpHosts(
	upHosts map[string]*host.HostInfo,
	drainingHosts []*host.HostInfo,
) {
	for _, drainingHost := range drainingHosts {
		ip := drainingHost.GetIp()
		hostname := drainingHost.GetHostname()
		// Remove the entry from upHosts, if both hostname and IP match
		if hostInfo, ok := upHosts[hostname]; ok && hostInfo.GetIp() == ip {
			delete(upHosts, hostname)
		}
	}
}
