package mesos

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	mesos "mesos/v1"
	sched "mesos/v1/scheduler"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
)

const (
	// ServiceName for mesos scheduler
	ServiceName = "Scheduler"
	// ServiceEndpoint of scheduler
	ServiceEndpoint = "/api/v1/scheduler"
)

// SchedulerDriver extends the Mesos HTTP Driver API
type SchedulerDriver interface {
	mhttp.MesosDriver
	FrameworkInfoProvider
}

// FrameworkInfoProvider can be used to retrieve mesosStreamID and frameworkID
type FrameworkInfoProvider interface {
	GetMesosStreamID() string
	GetFrameworkID() *mesos.FrameworkID
}

// schedulerDriver implements the Mesos Driver API
type schedulerDriver struct {
	store         storage.FrameworkInfoStore
	frameworkID   *mesos.FrameworkID
	mesosStreamID string
	cfg           *FrameworkConfig
	encoding      string
}

var instance *schedulerDriver

// InitSchedulerDriver initialize Mesos scheduler driver for Mesos scheduler
// HTTP API.
func InitSchedulerDriver(
	cfg *Config,
	store storage.FrameworkInfoStore) SchedulerDriver {
	// TODO: load framework ID from ZK or DB
	instance = &schedulerDriver{
		store:         store,
		frameworkID:   nil,
		mesosStreamID: "",
		cfg:           cfg.Framework,
		encoding:      cfg.Encoding,
	}
	return instance
}

// GetSchedulerDriver return the interface to SchedulerDriver.
func GetSchedulerDriver() SchedulerDriver {
	return instance
}

// GetFrameworkID returns the frameworkID.
func (d *schedulerDriver) GetFrameworkID() *mesos.FrameworkID {
	if d.frameworkID != nil {
		return d.frameworkID
	}
	frameworkIDVal, err := d.store.GetFrameworkID(d.cfg.Name)
	if err != nil {
		log.Errorf("failed to GetframeworkID from db for framework %v, err=%v",
			d.cfg.Name, err)
		return nil
	}
	if frameworkIDVal == "" {
		log.Errorf("GetframeworkID from db for framework %v is empty", d.cfg.Name)
		return nil
	}
	log.Debugf("Load frameworkID %v for framework %v", frameworkIDVal, d.cfg.Name)
	d.frameworkID = &mesos.FrameworkID{
		Value: &frameworkIDVal,
	}
	return d.frameworkID
}

// GetMesosStreamID reads DB for the Mesos stream ID
func (d *schedulerDriver) GetMesosStreamID() string {

	// TODO: followers should watch the stream ID from ZK so it can be
	// updated in case that the leader reconnects to Mesos, or the leader changes
	id, err := d.store.GetMesosStreamID(d.cfg.Name)
	if err != nil {
		log.Errorf(
			"failed to GetmesosStreamID from db for framework %v, err=%v",
			d.cfg.Name,
			err)
		return ""
	}
	log.WithFields(log.Fields{
		"stream_id": id,
		"framework": d.cfg.Name,
	}).Debug("Loaded Mesos stream id")
	d.mesosStreamID = id
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

func (d *schedulerDriver) prepareSubscribe() (*sched.Call, error) {
	// TODO: Inject capabilities based on config.
	gpuSupported := mesos.FrameworkInfo_Capability_GPU_RESOURCES
	capabilities := []*mesos.FrameworkInfo_Capability{
		{
			Type: &gpuSupported,
		},
	}
	host, err := os.Hostname()
	if err != nil {
		msg := "Failed to get host name"
		log.WithError(err).Error(msg)
		return nil, errors.Wrap(err, msg)
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
	if d.cfg.GPUSupported {
		log.Info("GPU capability is supported")
		var gpuCapability = mesos.FrameworkInfo_Capability_GPU_RESOURCES
		info.Capabilities = []*mesos.FrameworkInfo_Capability{
			{
				Type: &gpuCapability,
			},
		}
	}

	// TODO: it could happen that when we register,
	// the framework id has already failed over timeout.
	// Although we have set the timeout to a very long time in the config.
	// In this case we need to use an empty framework id and subscribe again
	d.frameworkID = d.GetFrameworkID()
	callType := sched.Call_SUBSCRIBE
	msg := &sched.Call{
		FrameworkId: d.frameworkID,
		Type:        &callType,
		Subscribe:   &sched.Call_Subscribe{FrameworkInfo: info},
	}

	// Add optional framework ID field for framework info and
	// subscribe call message
	if d.frameworkID != nil {
		info.Id = d.frameworkID
		msg.FrameworkId = d.frameworkID
		log.WithFields(log.Fields{
			"framework_id": d.frameworkID,
			"timeout":      d.cfg.FailoverTimeout,
		}).Info("Reregister to Mesos master with previous framework ID")
	} else {
		log.WithFields(log.Fields{
			"timeout": d.cfg.FailoverTimeout,
		}).Info("Register to Mesos without framework ID")
	}

	if d.cfg.Role != "" {
		info.Role = &d.cfg.Role
	}

	return msg, nil
}

// PrepareSubscribeRequest returns a HTTP post request that can be used to initiate subscription to mesos master
func (d *schedulerDriver) PrepareSubscribeRequest(mesosMasterHostPort string) (
	*http.Request, error) {

	subscribe, err := d.prepareSubscribe()
	if err != nil {
		return nil, errors.Wrap(err, "Failed prepareSubscribe")
	}

	body, err := mpb.MarshalPbMessage(subscribe, d.encoding)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshal subscribe call")
	}

	url := fmt.Sprintf("http://%s%s", mesosMasterHostPort, d.Endpoint())
	var req *http.Request
	req, err = http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "Failed HTTP request")
	}
	req.Header.Set("Content-Type", fmt.Sprintf("application/%s", d.encoding))
	req.Header.Set("Accept", fmt.Sprintf("application/%s", d.encoding))
	return req, nil
}

func (d *schedulerDriver) PostSubscribe(mesosStreamID string) {
	err := d.store.SetMesosStreamID(d.cfg.Name, mesosStreamID)
	if err != nil {
		log.Errorf("Failed to save Mesos stream ID %v %v, err=%v",
			d.cfg.Name, mesosStreamID, err)
	}
}

func (d *schedulerDriver) GetContentEncoding() string {
	return d.encoding
}
