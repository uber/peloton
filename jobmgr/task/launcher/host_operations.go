package launcher

import (
	"errors"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/common/reservation"
	"code.uber.internal/infra/peloton/util"
)

var (
	_disk  = "disk"
	_mem   = "mem"
	_cpus  = "cpus"
	_ports = "ports"
)

// HostOperationsFactory returns operations for hostmgr offeroperation rpc.
type HostOperationsFactory struct {
	tasks         []*hostsvc.LaunchableTask
	hostname      string
	selectedPorts []uint32
}

// NewHostOperationsFactory returns a new HostOperationsFactory instance.
func NewHostOperationsFactory(
	tasks []*hostsvc.LaunchableTask,
	hostname string,
	selectedPorts []uint32) *HostOperationsFactory {

	return &HostOperationsFactory{
		tasks:         tasks,
		hostname:      hostname,
		selectedPorts: selectedPorts,
	}
}

// GetHostOperations returns list of host offer operations.
func (h *HostOperationsFactory) GetHostOperations(
	opTypes []hostsvc.OfferOperation_Type) ([]*hostsvc.OfferOperation, error) {

	hostOperations := []*hostsvc.OfferOperation{}
	for _, op := range opTypes {
		hostOperation, err := h.getHostOperation(op)
		if err != nil {
			return nil, err
		}
		hostOperations = append(hostOperations, hostOperation)
	}
	return hostOperations, nil
}

func (h *HostOperationsFactory) getHostOperation(
	opType hostsvc.OfferOperation_Type) (*hostsvc.OfferOperation, error) {

	switch opType {
	case hostsvc.OfferOperation_LAUNCH:
		return h.getHostLaunchOperation()
	case hostsvc.OfferOperation_RESERVE:
		return h.getHostReserveOperation()
	case hostsvc.OfferOperation_CREATE:
		return h.getHostCreateOperation()
	default:
		return nil, errors.New("host operation type not supported")
	}
}

func (h *HostOperationsFactory) getHostReserveOperation() (*hostsvc.OfferOperation, error) {
	jobID, instanceID, err := util.ParseJobAndInstanceID(h.tasks[0].GetTaskId().GetValue())
	if err != nil {
		return nil, err
	}

	reserveOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_RESERVE,
		Reserve: &hostsvc.OfferOperation_Reserve{
			Resources: h.getMesosResources(),
		},
		ReservationLabels: reservation.CreateReservationLabels(
			jobID, instanceID, h.hostname),
	}
	return reserveOperation, nil
}

func (h *HostOperationsFactory) getHostCreateOperation() (*hostsvc.OfferOperation, error) {
	jobID, instanceID, err := util.ParseJobAndInstanceID(h.tasks[0].GetTaskId().GetValue())
	if err != nil {
		return nil, err
	}

	createOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_CREATE,
		Create: &hostsvc.OfferOperation_Create{
			Volume: h.tasks[0].GetVolume(),
		},
		ReservationLabels: reservation.CreateReservationLabels(
			jobID, instanceID, h.hostname),
	}
	return createOperation, nil
}

func (h *HostOperationsFactory) getHostLaunchOperation() (*hostsvc.OfferOperation, error) {
	jobID, instanceID, err := util.ParseJobAndInstanceID(h.tasks[0].GetTaskId().GetValue())
	if err != nil {
		return nil, err
	}

	launchOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_LAUNCH,
		Launch: &hostsvc.OfferOperation_Launch{
			Tasks: h.tasks,
		},
		ReservationLabels: reservation.CreateReservationLabels(
			jobID, instanceID, h.hostname),
	}
	return launchOperation, nil
}

func (h *HostOperationsFactory) getMesosResources() []*mesos.Resource {
	resources := []*mesos.Resource{}

	launchableTask := h.tasks[0]
	taskConfig := launchableTask.GetConfig()

	cpuLimit := taskConfig.GetResource().GetCpuLimit()
	if cpuLimit > 0 {
		resources = append(
			resources,
			util.NewMesosResourceBuilder().
				WithName(_cpus).
				WithValue(cpuLimit).
				Build())
	}
	memLimit := taskConfig.GetResource().GetMemLimitMb()
	if memLimit > 0 {
		resources = append(
			resources,
			util.NewMesosResourceBuilder().
				WithName(_mem).
				WithValue(memLimit).
				Build())
	}
	diskSize := taskConfig.GetResource().GetDiskLimitMb() + float64(taskConfig.GetVolume().GetSizeMB())
	if diskSize > 0 {
		resources = append(
			resources,
			util.NewMesosResourceBuilder().
				WithName(_disk).
				WithValue(diskSize).
				Build())
	}
	ports := h.selectedPorts
	if len(ports) > 0 {
		portSet := make(map[uint32]bool, len(ports))
		for _, port := range ports {
			portSet[port] = true
		}
		resources = append(
			resources,
			util.NewMesosResourceBuilder().
				WithName(_ports).
				WithType(mesos.Value_RANGES).
				WithRanges(util.CreatePortRanges(portSet)).
				Build())
	}
	return resources
}
