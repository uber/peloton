package mesos

import (
	"os"
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
	MesosFrameworkInfoProvider
}

// MesosFrameworkInfoProvider can be used to retrieve MesosStreamId and FrameworkID
type MesosFrameworkInfoProvider interface {
	GetMesosStreamId() string
	GetFrameworkId() *mesos.FrameworkID
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

// GetFrameworkId returns the frameworkid
func (d *schedulerDriver) GetFrameworkId() *mesos.FrameworkID {
	if d.frameworkId != nil {
		return d.frameworkId
	}
	frameworkIdVal, err := d.store.GetFrameworkId(d.cfg.Name)
	if err != nil {
		log.Errorf("failed to GetFrameworkId from db for framework %v, err=%v",
			d.cfg.Name, err)
		return nil
	}
	if frameworkIdVal == "" {
		log.Errorf("GetFrameworkId from db for framework %v is empty", d.cfg.Name)
		return nil
	}
	log.Debugf("Load FrameworkId %v for framework %v", frameworkIdVal, d.cfg.Name)
	d.frameworkId = &mesos.FrameworkID{
		Value: &frameworkIdVal,
	}
	return d.frameworkId
}

// GetMesosStreamId reads DB for the Mesos stream ID
func (d *schedulerDriver) GetMesosStreamId() string {

	// TODO: followers should watch the stream ID from ZK so it can be
	// updated in case that the leader reconnects to Mesos, or the leader changes
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
		{
			Type: &gpuSupported,
		},
	}
	host, err := os.Hostname()
	if err != nil {
		log.Errorf("Failed to get host name, err=%v", err)
	}

	info := &mesos.FrameworkInfo{
		User:            &d.cfg.User,
		Name:            &d.cfg.Name,
		FailoverTimeout: &d.cfg.FailoverTimeout,
		Checkpoint:      &d.cfg.Checkpoint,
		Capabilities:    capabilities,
		Hostname:        &host,
		Principal:       &d.cfg.Principal,
	}
	// TODO: it could happen that when we register, the framework id has already failed over timeout.
	// Although we have set the timeout to a very long time in the config. In this case we need to
	// use an empty framework id and subscribe again
	d.frameworkId = d.GetFrameworkId()
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
		log.Infof("Reregister to Mesos with framework ID: %s, with FailoverTimeout %v", d.frameworkId, d.cfg.FailoverTimeout)
	} else {
		log.Infof("Register to Mesos without framework ID, with FailoverTimeout %v", d.cfg.FailoverTimeout)
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
