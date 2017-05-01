package mesos

import (
	"fmt"
	"net/http"
	"net/url"
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

	// Schema and path for Mesos service URL.
	serviceSchema = "http"
	servicePath   = "/api/v1/scheduler"
)

// SchedulerDriver extends the Mesos HTTP Driver API.
type SchedulerDriver interface {
	mhttp.MesosDriver
	FrameworkInfoProvider
}

// FrameworkInfoProvider can be used to retrieve mesosStreamID and frameworkID.
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
// Implements FrameworkInfoProvider.GetFrameworkID().
func (d *schedulerDriver) GetFrameworkID() *mesos.FrameworkID {
	if d.frameworkID != nil {
		return d.frameworkID
	}
	frameworkIDVal, err := d.store.GetFrameworkID(d.cfg.Name)
	if err != nil {
		log.WithError(err).
			WithField("framework_name", d.cfg.Name).
			Error("failed to GetframeworkID from db for framework")
		return nil
	}
	if frameworkIDVal == "" {
		log.WithField("framework_name", d.cfg.Name).
			Error("GetframeworkID from db is empty")
		return nil
	}
	log.WithFields(log.Fields{
		"framework_id":   frameworkIDVal,
		"framework_name": d.cfg.Name,
	}).Debug("Loaded frameworkID")
	d.frameworkID = &mesos.FrameworkID{
		Value: &frameworkIDVal,
	}
	return d.frameworkID
}

// GetMesosStreamID reads DB for the Mesos stream ID.
// Implements FrameworkInfoProvider.GetMesosStreamID().
func (d *schedulerDriver) GetMesosStreamID() string {
	id, err := d.store.GetMesosStreamID(d.cfg.Name)
	if err != nil {
		log.WithError(err).
			WithField("framework_name", d.cfg.Name).
			Error("Failed to GetmesosStreamID from db")
		return ""
	}
	log.WithFields(log.Fields{
		"stream_id": id,
		"framework": d.cfg.Name,
	}).Debug("Loaded Mesos stream id")

	// TODO: This cache variable was never used?
	d.mesosStreamID = id
	return id
}

// Returns the name of Scheduler driver.
// Implements mhttp.MesosDriver.Name().
func (d *schedulerDriver) Name() string {
	return ServiceName
}

// Returns the Mesos endpoint to be connected to.
// Implements mhttp.MesosDriver.Endpoint().
func (d *schedulerDriver) Endpoint() url.URL {
	return url.URL{
		Scheme: serviceSchema,
		Path:   servicePath,
	}
}

// Returns the Type of Mesos event message such as
// mesos.v1.scheduler.Event or mesos.v1.executor.Event
// Implements mhttp.MesosDriver.EventDataType().
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

// PrepareSubscribeRequest returns a HTTP post request that can be used to
// initiate subscription to mesos master.
// Implements mhttp.MesosDriver.PrepareSubscribeRequest().
func (d *schedulerDriver) PrepareSubscribeRequest(mesosMasterHostPort string) (
	*http.Request, error) {

	if len(mesosMasterHostPort) == 0 {
		return nil, errors.New("No active leader detected")
	}

	subscribe, err := d.prepareSubscribe()
	if err != nil {
		return nil, errors.Wrap(err, "Failed prepareSubscribe")
	}

	body, err := mpb.MarshalPbMessage(subscribe, d.encoding)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshal subscribe call")
	}

	url := d.Endpoint()
	url.Host = mesosMasterHostPort
	var req *http.Request
	req, err = http.NewRequest("POST", url.String(), strings.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "Failed HTTP request")
	}
	req.Header.Set("Content-Type", fmt.Sprintf("application/%s", d.encoding))
	req.Header.Set("Accept", fmt.Sprintf("application/%s", d.encoding))
	return req, nil
}

// Invoked after the subscription to Mesos is done
// Implements mhttp.MesosDriver.PostSubscribe().
func (d *schedulerDriver) PostSubscribe(mesosStreamID string) {
	err := d.store.SetMesosStreamID(d.cfg.Name, mesosStreamID)
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"framework_name": d.cfg.Name,
				"stream_id":      mesosStreamID,
			}).Error("Failed to save Mesos stream ID")
	}
}

// GetContentEncoding returns the http content encoding of the Mesos
// HTTP traffic.
// Implements mhttp.MesosDriver.GetContentEncoding().
func (d *schedulerDriver) GetContentEncoding() string {
	return d.encoding
}
