package mesos

import (
	"reflect"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"github.com/golang/protobuf/proto"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
)

const (
	ServiceName     = "Scheduler"
	ServiceEndpoint = "/api/v1/scheduler"
)

// SchedulerDriver extends the Mesos HTTP Driver API
type SchedulerDriver interface {
	mhttp.MesosDriver

	GetMesosStreamId() string
}

// schedulerDriver implements the Mesos Driver API
type schedulerDriver struct {
	store         *mysql.MysqlJobStore
	frameworkId   *mesos.FrameworkID
	mesosStreamId string
	cfg           *FrameworkConfig
}

var instance *schedulerDriver = nil

// Initialize Mesos scheduler driver for Mesos scheduler HTTP API
func InitSchedulerDriver(
	cfg *FrameworkConfig, store *mysql.MysqlJobStore) *schedulerDriver {
	// TODO: load framework ID from ZK or DB
	instance = &schedulerDriver{
		store:         store,
		frameworkId:   nil,
		mesosStreamId: "",
		cfg:           cfg,
	}
	return instance
}

// Return the interface to SchedulerDriver
func GetSchedulerDriver() *schedulerDriver {
	return instance
}

func (d *schedulerDriver) GetMesosStreamId() string {
	if d.mesosStreamId != "" {
		return d.mesosStreamId
	}

	// Follower would try to read DB for the Mesos stream ID

	// TODO: followers should watch the stream ID from ZK so it can be
	// updated in case that the leader reconnects to Mesos
	id, err := d.store.GetMesosStreamId(d.cfg.Name)
	if err != nil {
		log.Errorf("failed to GetMesosStreamId from db for framework %v, err=%v",
			d.cfg.Name, err)
		return ""
	}
	log.Debugf("Load Mesos stream id %v for framework %v", id, d.cfg.Name)
	d.mesosStreamId = id
	return id
}

func (d *schedulerDriver) Name() string {
	return ServiceName
}

func (d *schedulerDriver) Endpoint() string {
	return ServiceEndpoint
}

func (d *schedulerDriver) EventDataType() reflect.Type {
	return reflect.TypeOf(sched.Event{})
}

func (d *schedulerDriver) PrepareSubscribe() proto.Message {
	gpuSupported := mesos.FrameworkInfo_Capability_GPU_RESOURCES
	capabilities := []*mesos.FrameworkInfo_Capability{
		&mesos.FrameworkInfo_Capability{
			Type: &gpuSupported,
		},
	}
	info := &mesos.FrameworkInfo{
		User:            &d.cfg.User,
		Name:            &d.cfg.Name,
		FailoverTimeout: &d.cfg.FailoverTimeout,
		Checkpoint:      &d.cfg.Checkpoint,
		Capabilities:    capabilities,
	}

	callType := sched.Call_SUBSCRIBE
	msg := &sched.Call{
		FrameworkId: d.frameworkId,
		Type:        &callType,
		Subscribe:   &sched.Call_Subscribe{FrameworkInfo: info},
	}

	// Add optional framework ID field for framework info and
	// subscribe call message
	if d.frameworkId != nil {
		info.Id = d.frameworkId
		msg.FrameworkId = d.frameworkId
		log.Infof("Reregister to Mesos with framework ID: %s", d.frameworkId)
	} else {
		log.Infof("Register to Mesos without framework ID")
	}

	if d.cfg.Role != "" {
		info.Role = &d.cfg.Role
	}

	return msg
}

func (d *schedulerDriver) PostSubscribe(mesosStreamId string) {
	err := d.store.SetMesosStreamId(d.cfg.Name, mesosStreamId)
	if err != nil {
		log.Errorf("Failed to save Mesos stream ID %v %v, err=%v",
			d.cfg.Name, mesosStreamId, err)
	}
}
