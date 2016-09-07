package mesos

import (
	"reflect"

	"github.com/golang/protobuf/proto"
	"code.uber.internal/go-common.git/x/log"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"
)

const (
	ServiceName = "Scheduler"
	ServiceEndpoint = "/api/v1/scheduler"
)

// schedulerDriver implements the Mesos Driver API
type schedulerDriver struct {
	id  *mesos.FrameworkID
	cfg *FrameworkConfig
}

func NewSchedulerDriver(cfg *FrameworkConfig, id *mesos.FrameworkID) *schedulerDriver{
	return &schedulerDriver{id: id, cfg: cfg}
}

func (d *schedulerDriver) Name() string {
	return ServiceName
}

func (d *schedulerDriver) Endpoint() string {
	return ServiceEndpoint
}

func (d *schedulerDriver) Subscribe() proto.Message {
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
		FrameworkId: d.id,
		Type:        &callType,
		Subscribe:   &sched.Call_Subscribe{FrameworkInfo: info},
	}

	// Add optional framework ID field for framework info and
	// subscribe call message
	if d.id != nil {
		info.Id = d.id
		msg.FrameworkId = d.id
		log.Infof("Reregister to Mesos with framework ID: %s", d.id)
	} else {
		log.Infof("Register to Mesos without framework ID")
	}

	if d.cfg.Role != "" {
		info.Role = &d.cfg.Role
	}

	return msg
}

func (d *schedulerDriver) EventDataType() reflect.Type {
	return reflect.TypeOf(sched.Event{})
}
