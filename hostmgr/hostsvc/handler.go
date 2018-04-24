package hostsvc

import (
	"context"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	host "code.uber.internal/infra/peloton/.gen/peloton/api/host"
	host_svc "code.uber.internal/infra/peloton/.gen/peloton/api/host/svc"

	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

// serviceHandler implements peloton.api.host.svc.HostService
type serviceHandler struct {
	operatorMasterClient mpb.MasterOperatorClient
	maintenanceQueue     queue.MaintenanceQueue
	metrics              *Metrics
}

// InitServiceHandler initializes the HostService
func InitServiceHandler(
	d *yarpc.Dispatcher,
	parent tally.Scope,
	operatorMasterClient mpb.MasterOperatorClient,
	maintenanceQueue queue.MaintenanceQueue) {
	handler := &serviceHandler{
		operatorMasterClient: operatorMasterClient,
		maintenanceQueue:     maintenanceQueue,
		metrics:              NewMetrics(parent.SubScope("hostsvc")),
	}
	d.Register(host_svc.BuildHostServiceYARPCProcedures(handler))
	log.Info("Hostsvc handler initialized")
}

// QueryHosts returns the hosts which are in one of the specified states.
// TODO: Handle other HostStates once state machine for host states has been added
func (m *serviceHandler) QueryHosts(
	ctx context.Context,
	request *host_svc.QueryHostsRequest) (*host_svc.QueryHostsResponse, error) {
	m.metrics.QueryHostsAPI.Inc(1)

	// Get current maintenance status
	response, err := m.operatorMasterClient.GetMaintenanceStatus()
	if err != nil {
		log.WithError(err).Error("QueryHosts failed")
		m.metrics.QueryHostsFail.Inc(1)
		return nil, err
	}
	clusterStatus := response.GetStatus()

	var hostInfos []*host.HostInfo

	// Build result `hostInfos`
	for _, state := range request.GetHostStates() {
		switch state {
		case host.HostState_HOST_STATE_DRAINING:
			for _, drainingMachine := range clusterStatus.DrainingMachines {
				machineID := drainingMachine.GetId()
				hostInfo := &host.HostInfo{
					Hostname: machineID.GetHostname(),
					Ip:       machineID.GetIp(),
					State:    host.HostState_HOST_STATE_DRAINING,
				}
				hostInfos = append(hostInfos, hostInfo)
			}
		case host.HostState_HOST_STATE_DOWN:
			for _, downMachine := range clusterStatus.DownMachines {
				hostInfo := &host.HostInfo{
					Hostname: downMachine.GetHostname(),
					Ip:       downMachine.GetIp(),
					State:    host.HostState_HOST_STATE_DOWN,
				}
				hostInfos = append(hostInfos, hostInfo)
			}
		default:
			log.WithField("host_state", state).Warn("Query by this host state is not supported yet")
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
	request *host_svc.StartMaintenanceRequest) (*host_svc.StartMaintenanceResponse, error) {
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
	maintenanceWindow := &mesos_v1_maintenance.Window{
		MachineIds: request.GetMachineIds(),
		Unavailability: &mesos_v1.Unavailability{
			Start: &mesos_v1.TimeInfo{
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
	log.WithField("maintenance_schedule", schedule).Info("Maintenance Schedule posted to Mesos Master")

	var hosts []string
	for _, machineID := range request.GetMachineIds() {
		hosts = append(hosts, machineID.GetHostname())
	}

	// Enqueue hostnames into maintenance queue to initiate the rescheduling of tasks running on these hosts
	err = m.maintenanceQueue.Enqueue(hosts)
	if err != nil {
		log.WithError(err).Error("Failed to enqueue some hosts into maintenance queue")
		return nil, err
	}

	m.metrics.StartMaintenanceSuccess.Inc(1)
	return &host_svc.StartMaintenanceResponse{}, nil
}

// CompleteMaintenance completes maintenance on the specified hosts.
func (m *serviceHandler) CompleteMaintenance(
	ctx context.Context,
	request *host_svc.CompleteMaintenanceRequest) (*host_svc.CompleteMaintenanceResponse, error) {
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
