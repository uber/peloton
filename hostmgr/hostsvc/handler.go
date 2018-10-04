package hostsvc

import (
	"context"
	"fmt"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_maintenance "code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"

	"code.uber.internal/infra/peloton/common/stringset"
	hm_host "code.uber.internal/infra/peloton/hostmgr/host"
	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	maintenanceQueue     queue.MaintenanceQueue
	metrics              *Metrics
	operatorMasterClient mpb.MasterOperatorClient
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	operatorMasterClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue) {
	handler := &serviceHandler{
		maintenanceQueue:     maintenanceQueue,
		metrics:              NewMetrics(parent.SubScope("hostsvc")),
		operatorMasterClient: operatorMasterClient,
	}
	d.Register(host_svc.BuildHostServiceYARPCProcedures(handler))
	log.Info("Hostsvc handler initialized")
}

// QueryHosts returns the hosts which are in one of the specified states.
// A host, at any given time, will be in one of the following states
// 		1.HostState_HOST_STATE_UP - The host is up and running
// 		2.HostState_HOST_STATE_DRAINING - The tasks running on the host are being rescheduled and
// 										  there will be no further placement of tasks on the host
//		3.HostState_HOST_STATE_DRAINED - There are no tasks running on this host and it is ready to be 'DOWN'ed
// 		4.HostState_HOST_STATE_DOWN - The host is in maintenance.
func (m *serviceHandler) QueryHosts(
	ctx context.Context,
	request *host_svc.QueryHostsRequest) (*host_svc.QueryHostsResponse, error) {
	m.metrics.QueryHostsAPI.Inc(1)

	var err error
	hostStateSet := stringset.New()
	for _, state := range request.GetHostStates() {
		hostStateSet.Add(state.String())
	}

	if request.HostStates == nil || len(request.HostStates) == 0 {
		for _, state := range host.HostState_name {
			hostStateSet.Add(state)
		}
	}

	// upHosts is map to ensure we don't have 2 entries for DRAINING hosts
	// as the response from Agents() includes info of DRAINING hosts
	var upHosts map[string]*host.HostInfo
	if hostStateSet.Contains(host.HostState_HOST_STATE_UP.String()) {
		upHosts, err = buildHostInfoForRegisteredAgents()
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
		drainingHosts = buildHostInfosForMachineIDs(clusterStatus.GetDrainingMachines())
		// We need to remove DRAINING hosts from upHosts
		removeHostsFromMap(upHosts, drainingHosts)
	}

	// Build result
	var hostInfos []*host.HostInfo
	for _, hostState := range hostStateSet.ToSlice() {
		switch host.HostState(host.HostState_value[hostState]) {
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

// StartMaintenance puts the host(s) into DRAINING state by posting a maintenance
// schedule to Mesos Master. Inverse offers are sent out and all future offers
// from the(se) host(s) are tagged with unavailability (Please check Mesos
// Maintenance Primitives for more info). The hosts are first drained of tasks
// before they are put into maintenance by posting to /machine/down endpoint of
// Mesos Master. The hosts transition from UP to DRAINING and finally to DOWN.
func (m *serviceHandler) StartMaintenance(
	ctx context.Context,
	request *host_svc.StartMaintenanceRequest,
) (*host_svc.StartMaintenanceResponse, error) {
	m.metrics.StartMaintenanceAPI.Inc(1)

	machineIds, err := buildMachineIDsForHosts(request.GetHostnames())
	if err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return nil, err
	}

	// Get current maintenance schedule
	response, err := m.operatorMasterClient.GetMaintenanceSchedule()
	if err != nil {
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
		MachineIds: machineIds,
		Unavailability: &mesos.Unavailability{
			Start: &mesos.TimeInfo{
				Nanoseconds: &nanos,
			},
		},
	}
	schedule.Windows = append(schedule.Windows, maintenanceWindow)

	err = m.operatorMasterClient.UpdateMaintenanceSchedule(schedule)
	if err != nil {
		m.metrics.StartMaintenanceFail.Inc(1)
		return nil, err
	}
	log.WithField("maintenance_schedule", schedule).
		Info("Maintenance Schedule posted to Mesos Master")

	// Enqueue hostnames into maintenance queue to initiate
	// the rescheduling of tasks running on these hosts
	err = m.maintenanceQueue.Enqueue(request.GetHostnames())
	if err != nil {
		return nil, err
	}

	m.metrics.StartMaintenanceSuccess.Inc(1)
	return &host_svc.StartMaintenanceResponse{}, nil
}

// CompleteMaintenance completes maintenance on the specified hosts. It brings
// UP a host which is in maintenance by posting to /machine/up endpoint of
// Mesos Master i.e. the machine transitions from DOWN to UP state
// (Please check Mesos Maintenance Primitives for more info)
func (m *serviceHandler) CompleteMaintenance(
	ctx context.Context,
	request *host_svc.CompleteMaintenanceRequest,
) (*host_svc.CompleteMaintenanceResponse, error) {
	m.metrics.CompleteMaintenanceAPI.Inc(1)

	response, err := m.operatorMasterClient.GetMaintenanceStatus()
	if err != nil {
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return nil, err
	}
	clusterStatus := response.GetStatus()

	downMachinesMap := make(map[string]string)
	for _, downMachine := range clusterStatus.GetDownMachines() {
		downMachinesMap[downMachine.GetHostname()] = downMachine.GetIp()
	}

	var machineIds []*mesos.MachineID
	hostnames := request.GetHostnames()
	for i := 0; i < len(hostnames); i++ {
		hostname := hostnames[i]
		ip, ok := downMachinesMap[hostname]
		if !ok {
			return nil, fmt.Errorf("invalid request. Host %s is not DOWN", hostname)
		}
		machineID := &mesos.MachineID{
			Hostname: &hostname,
			Ip:       &ip,
		}
		machineIds = append(machineIds, machineID)
	}

	err = m.operatorMasterClient.StopMaintenance(machineIds)
	if err != nil {
		m.metrics.CompleteMaintenanceFail.Inc(1)
		return nil, err
	}

	m.metrics.CompleteMaintenanceSuccess.Inc(1)
	return &host_svc.CompleteMaintenanceResponse{}, nil
}

// Build host info for registered agents
func buildHostInfoForRegisteredAgents() (map[string]*host.HostInfo, error) {
	agentMap := hm_host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return nil, nil
	}
	upHosts := make(map[string]*host.HostInfo)
	for _, agent := range agentMap.RegisteredAgents {
		hostname := agent.GetAgentInfo().GetHostname()
		agentIP, _, err := util.ExtractIPAndPortFromMesosAgentPID(
			agent.GetPid())
		if err != nil {
			return nil, err
		}
		hostInfo := &host.HostInfo{
			Hostname: hostname,
			Ip:       agentIP,
			State:    host.HostState_HOST_STATE_UP,
		}
		upHosts[hostname] = hostInfo
	}
	return upHosts, nil
}

// Build machine ID for specified hosts
func buildMachineIDsForHosts(
	hostnames []string,
) ([]*mesos.MachineID, error) {
	var machineIds []*mesos.MachineID
	agentMap := hm_host.GetAgentMap()
	if agentMap == nil || len(agentMap.RegisteredAgents) == 0 {
		return nil, fmt.Errorf("no registered agents")
	}
	for i := 0; i < len(hostnames); i++ {
		hostname := hostnames[i]
		if _, ok := agentMap.RegisteredAgents[hostname]; !ok {
			return nil, fmt.Errorf("unknown host %s", hostname)
		}
		pid := agentMap.RegisteredAgents[hostname].GetPid()
		ip, _, err := util.ExtractIPAndPortFromMesosAgentPID(pid)
		if err != nil {
			return nil, err
		}
		machineID := &mesos.MachineID{
			Hostname: &hostname,
			Ip:       &ip,
		}
		machineIds = append(machineIds, machineID)
	}
	return machineIds, nil
}

// Build HostInfo from machineID
func buildHostInfosForMachineIDs(
	machines []*mesos_maintenance.ClusterStatus_DrainingMachine,
) []*host.HostInfo {
	var hosts []*host.HostInfo
	for _, machine := range machines {
		machineID := machine.GetId()
		hostname := machineID.GetHostname()
		hostInfo := &host.HostInfo{
			Hostname: hostname,
			Ip:       machineID.GetIp(),
			State:    host.HostState_HOST_STATE_DRAINING,
		}
		hosts = append(hosts, hostInfo)
	}
	return hosts
}

// Remove the 'hosts' from host map 'm'
func removeHostsFromMap(
	m map[string]*host.HostInfo,
	hosts []*host.HostInfo,
) {
	for _, host := range hosts {
		ip := host.GetIp()
		hostname := host.GetHostname()
		// Remove the entry from m, if both hostname and IP match
		if hostInfo, ok := m[hostname]; ok && hostInfo.GetIp() == ip {
			delete(m, hostname)
		}
	}
}
