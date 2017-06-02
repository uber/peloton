package operation

import (
	"errors"

	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/hostmgr/factory/task"
	"code.uber.internal/infra/peloton/hostmgr/scalar"
)

var (
	pelotonRole      = "peloton"
	pelotonPrinciple = "peloton"
)

// OfferOperationsFactory returns operations for mesos offer ACCEPT call.
type OfferOperationsFactory struct {
	operations []*hostsvc.OfferOperation
	resources  []*mesos.Resource
	hostname   string
	agentID    *mesos.AgentID
}

// NewOfferOperationsFactory returns a new OfferOperationsFactory instance.
func NewOfferOperationsFactory(
	operations []*hostsvc.OfferOperation,
	resources []*mesos.Resource,
	hostname string,
	agentID *mesos.AgentID) *OfferOperationsFactory {

	return &OfferOperationsFactory{
		operations: operations,
		resources:  resources,
		hostname:   hostname,
		agentID:    agentID,
	}
}

// GetOfferOperations returns list of mesos offer operations.
func (o *OfferOperationsFactory) GetOfferOperations() ([]*mesos.Offer_Operation, error) {
	mesosOperations := []*mesos.Offer_Operation{}
	for _, op := range o.operations {
		mesosOperation, err := o.getOfferOperation(op)
		if err != nil {
			return nil, err
		}
		mesosOperations = append(mesosOperations, mesosOperation)
	}
	return mesosOperations, nil
}

func (o *OfferOperationsFactory) getOfferOperation(
	operation *hostsvc.OfferOperation) (*mesos.Offer_Operation, error) {

	switch operation.GetType() {
	case hostsvc.OfferOperation_LAUNCH:
		return o.getLaunchOperation(operation)
	case hostsvc.OfferOperation_RESERVE:
		return o.getReserveOperation(operation)
	case hostsvc.OfferOperation_CREATE:
		return o.getCreateOperation(operation)
	default:
		return nil, errors.New("offer operation type not supported")
	}
}

func checkOfferResources(offerRes []*mesos.Resource, opRes []*mesos.Resource) error {
	mesosResources := scalar.FromMesosResources(offerRes)
	operationResources := scalar.FromMesosResources(opRes)
	if !mesosResources.Contains(&operationResources) {
		log.WithFields(log.Fields{
			"offer_resources":     offerRes,
			"operation_resources": opRes,
		}).Warn("offer resource is less than operation resource")
		return errors.New("offer resource is less than operation resource")
	}
	return nil
}

func (o *OfferOperationsFactory) getReserveOperation(
	operation *hostsvc.OfferOperation) (*mesos.Offer_Operation, error) {

	if operation.GetReservationLabels() == nil ||
		operation.GetReserve() == nil ||
		len(operation.GetReserve().GetResources()) == 0 {
		log.WithField("operation", operation).Error("invalid reserve operation")
		return nil, errors.New("invalid reserve operation")
	}

	err := checkOfferResources(o.resources, operation.GetReserve().GetResources())
	if err != nil {
		return nil, err
	}

	var reservedResources []*mesos.Resource
	for _, res := range operation.GetReserve().GetResources() {
		reservedRes := proto.Clone(res).(*mesos.Resource)
		reservedRes.Role = &pelotonRole
		reservedRes.Reservation = &mesos.Resource_ReservationInfo{
			Principal: &pelotonPrinciple,
			Labels:    operation.GetReservationLabels(),
		}
		reservedResources = append(reservedResources, reservedRes)
	}

	reserveType := mesos.Offer_Operation_RESERVE
	return &mesos.Offer_Operation{
		Type: &reserveType,
		Reserve: &mesos.Offer_Operation_Reserve{
			Resources: reservedResources,
		},
	}, nil
}

func (o *OfferOperationsFactory) getCreateOperation(
	operation *hostsvc.OfferOperation) (*mesos.Offer_Operation, error) {

	if operation.GetReservationLabels() == nil ||
		operation.GetCreate() == nil ||
		operation.GetCreate().GetVolume() == nil ||
		operation.GetCreate().GetVolume().GetResource().GetName() != "disk" ||
		len(operation.GetCreate().GetVolume().GetId().GetValue()) == 0 ||
		len(operation.GetCreate().GetVolume().GetContainerPath()) == 0 {
		log.WithField("operation", operation).Error("invalid create operation")
		return nil, errors.New("invalid create operation")
	}

	err := checkOfferResources(o.resources, []*mesos.Resource{operation.GetCreate().GetVolume().GetResource()})
	if err != nil {
		return nil, err
	}

	var persistedResources []*mesos.Resource
	vol := operation.GetCreate().GetVolume()
	createRes := proto.Clone(vol.GetResource()).(*mesos.Resource)
	createRes.Role = &pelotonRole
	createRes.Reservation = &mesos.Resource_ReservationInfo{
		Principal: &pelotonPrinciple,
		Labels:    operation.GetReservationLabels(),
	}
	volumeID := vol.GetId().GetValue()
	containerPath := vol.GetContainerPath()
	volumeRWMode := mesos.Volume_RW
	createRes.Disk = &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id:        &volumeID,
			Principal: &pelotonPrinciple,
		},
		Volume: &mesos.Volume{
			ContainerPath: &containerPath,
			Mode:          &volumeRWMode,
		},
	}
	persistedResources = append(persistedResources, createRes)

	createType := mesos.Offer_Operation_CREATE
	return &mesos.Offer_Operation{
		Type: &createType,
		Create: &mesos.Offer_Operation_Create{
			Volumes: persistedResources,
		},
	}, nil
}

func (o *OfferOperationsFactory) getLaunchOperation(
	operation *hostsvc.OfferOperation) (*mesos.Offer_Operation, error) {

	if operation.GetLaunch() == nil ||
		len(operation.GetLaunch().GetTasks()) == 0 {
		return nil, errors.New("invalid launch operation")
	}

	var mesosTasks []*mesos.TaskInfo

	builder := task.NewBuilder(o.resources)

	for _, t := range operation.GetLaunch().GetTasks() {
		mesosTask, err := builder.Build(
			t.GetTaskId(),
			t.GetConfig(),
			t.GetPorts(),
			operation.GetReservationLabels(),
			t.GetVolume(),
		)
		if err != nil {
			log.WithFields(log.Fields{
				"error":     err,
				"task_id":   t.TaskId,
				"resources": o.resources,
			}).Warn("Fail to get correct Mesos TaskInfo for launch operation.")
			return nil, err
		}

		mesosTask.AgentId = o.agentID
		mesosTasks = append(mesosTasks, mesosTask)
	}

	launchType := mesos.Offer_Operation_LAUNCH
	return &mesos.Offer_Operation{
		Type: &launchType,
		Launch: &mesos.Offer_Operation_Launch{
			TaskInfos: mesosTasks,
		},
	}, nil
}
