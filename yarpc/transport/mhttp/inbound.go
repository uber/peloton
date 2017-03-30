package mhttp

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/transport"
)

const (
	// MesosHTTPConnTimeout is the mesos connection timeout
	MesosHTTPConnTimeout = 30 * time.Second

	// MesosHTTPConnKeepAlive is the mesos connection keep alive
	MesosHTTPConnKeepAlive = 30 * time.Second

	_stopRetryInterval = 100 * time.Millisecond

	_tickerInterval = 1 * time.Second
)

// Inbound represents a Mesos HTTP Inbound. It is the same as the
// transport.Inbound except it exposes the address on which the system is
// listening for connections.
type Inbound interface {
	transport.Inbound

	StartMesosLoop(newHostPort string) error
	IsRunning() bool
}

// InboundOption is an option for an Mesos HTTP inbound.
type InboundOption func(*inbound)

// NewInbound builds a new Mesos HTTP inbound after registering with
// Mesos master via Subscribe message
func NewInbound(parent tally.Scope, d MesosDriver, opts ...InboundOption) Inbound {
	i := &inbound{
		driver:  d,
		metrics: newMetrics(parent),
	}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type inbound struct {
	sync.Mutex

	metrics *Metrics

	hostPort      string
	driver        MesosDriver
	stopFlag      atomic.Bool
	serviceDetail transport.ServiceDetail
	deps          transport.Deps
	client        *http.Client
	runningState  atomic.Bool
	ticker        *time.Ticker
}

// Start would initialize some variables, actual mesos communication would be
// started by StartMesosLoop(...)
func (i *inbound) Start(
	service transport.ServiceDetail,
	d transport.Deps) error {

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   MesosHTTPConnTimeout,
			KeepAlive: MesosHTTPConnKeepAlive,
		}).Dial,
	}
	i.client = &http.Client{Transport: transport}
	i.serviceDetail = service
	i.deps = d
	return nil
}

// StartMesosLoop subscribes to mesos master as a framework, and starts a
// go-routine to dispatch the mesos callbacks.
// It also creates a ticker to periodically check and retry the Mesos
// subscription routine.
// The call can be called multiple times to start/stop talking to Mesos master,
// or can be used to switch new Mesos master leader when failed over.
// TODO(zhitao): Fix this logic:
// Instead of relying on the following ticker to reinitialize the
// inbound, a callback function/channel should be passed-in to notify
// upper-level logic to prepare for a broken channel.
func (i *inbound) StartMesosLoop(hostPort string) error {
	log.Infof("StartMesosLoop called")
	err := i.startLoopInternal(hostPort)
	if err != nil {
		log.WithError(err).Error("Initial start mesos loop failed")
	}

	i.Lock()
	defer i.Unlock()

	if i.ticker != nil {
		log.Warn("Ticker already created")
		return err
	}
	i.ticker = time.NewTicker(_tickerInterval)
	go func() {
		// The go routine that does Mesos master subscription and event
		// processing can exit due to error (e.g. lose network etc) thus
		// there is a ticker callback to re-kickoff the Mesos
		// subscription if needed.
		for range i.ticker.C {
			stopped := i.stopFlag.Load()
			running := i.runningState.Load()
			if stopped || running || len(i.hostPort) == 0 {
				continue
			}

			log.WithFields(log.Fields{
				"running":  running,
				"stopped":  stopped,
				"hostport": i.hostPort,
			}).Info("Restarting mesos loop under states")

			if err := i.startLoopInternal(i.hostPort); err != nil {
				log.WithError(err).
					WithField("hostport", i.hostPort).
					Error("Failed to start mesos loop")
			}
		}
	}()
	return err
}

// startLoopInternal subscribes to Mesos master as a framework,
// and starts a go-routine to dispatch the Mesos callbacks.
func (i *inbound) startLoopInternal(hostPort string) error {
	i.Lock()
	defer i.Unlock()

	i.metrics.StartCount.Inc(1)

	if len(hostPort) == 0 {
		return errors.New("Empty hostport when starting Mesos loop")
	}

	if i.runningState.Load() {
		if i.hostPort == hostPort {
			log.WithField("hostport", i.hostPort).
				Info("Running inbound hostport matches")
			return nil
		}
		i.metrics.LeaderChanges.Inc(1)
		log.WithFields(log.Fields{
			"old": i.hostPort,
			"new": hostPort,
		}).Info("Hostport changed, resetting inbound")
		i.stopInternal()
	}

	i.stopFlag.Store(false)
	i.metrics.Stopped.Update(0)
	log.WithField("hostport", hostPort).
		Info("Starting the inbound for mesos master")
	i.hostPort = hostPort

	req, err := i.driver.PrepareSubscribeRequest(hostPort)
	if err != nil {
		return fmt.Errorf("Failed to PrepareSubscribeRequest: %v", err)
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf(
			"Failed to POST subscribe request to master: %v", err)
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(
			"Failed to subscribe to master (Status=%d): %s",
			resp.StatusCode,
			respBody)
	}

	// Invoke the post subscribe callback on Mesos driver
	values := resp.Header["Mesos-Stream-Id"]
	if len(values) != 1 {
		return fmt.Errorf(
			"Failed to obtain stream id from values: %v",
			values)
	}
	i.driver.PostSubscribe(values[0])

	hdl := handler{
		ServiceDetail: i.serviceDetail,
		Service:       i.driver.Name(),
		Caller:        i.hostPort,
		EventDataType: i.driver.EventDataType(),
		ContentType:   i.driver.GetContentEncoding(),
	}

	started := make(chan int, 1)
	go func() error {
		defer i.runningState.Store(false)
		defer i.metrics.Running.Update(0)
		defer resp.Body.Close()

		i.runningState.Store(true)
		i.metrics.Running.Update(1)
		started <- 0
		reader := bufio.NewReader(resp.Body)
		for {
			if stopped := i.stopFlag.Load(); stopped {
				log.Info("mInbound go routine stopped")
				return nil
			}

			// TODO: if the master decide to disconnect the framework,
			// we can read a EOF here. We need to handle this case by
			// detect and re-register again with different framework

			// Read the length of the next RecordIO frame
			line, _, err := reader.ReadLine()
			if err != nil {
				log.WithError(err).Error("Failed to read line")
				i.metrics.ReadLineError.Inc(1)
				return err
			}

			framelen, err := strconv.ParseUint(string(line), 10, 64)
			if framelen < 1 || err != nil {
				log.WithField("frame_len", framelen).
					WithError(err).
					Error("Failed to read framelen")
				i.metrics.FrameLengthError.Inc(1)
				return err
			}

			// Read next RecordIO frame with framelen bytes
			buf := make([]byte, framelen)
			readlen, err := io.ReadFull(reader, buf)
			if err != nil {
				return err
			}
			if uint64(readlen) != framelen {
				msg := "Failed to read full frame"
				log.WithFields(log.Fields{
					"read_len":  readlen,
					"frame_len": framelen,
				}).Error(msg)
				i.metrics.LineLengthError.Inc(1)
				return fmt.Errorf(msg)
			}

			err = hdl.HandleRecordIO(buf)
			if err != nil {
				msg := "Failed to handle record IO event"
				log.WithError(err).Error(msg)
				i.metrics.RecordIOError.Inc(1)
				return errors.Wrap(err, msg)
			}

			i.metrics.Frames.Inc(1)
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// stopInternal must be called with mutex locked
func (i *inbound) stopInternal() error {
	i.stopFlag.Store(true)
	i.metrics.Stopped.Update(1)
	for {
		if running := i.runningState.Load(); running {
			time.Sleep(_stopRetryInterval)
		} else {
			break
		}
	}
	log.Info("mInbound stopped")
	return nil
}

// Stop would stop the internal go-routine that receives mesos callback
// and disconnect with current mesos master
func (i *inbound) Stop() error {
	i.Lock()
	defer i.Unlock()

	log.WithField("hostport", i.hostPort).
		Info("mInbound stopping")
	if i.ticker != nil {
		i.ticker.Stop()
		i.ticker = nil
	} else {
		log.Warn("ticker is already nil")
	}
	return i.stopInternal()
}

// IsRunning returns the running state.
func (i *inbound) IsRunning() bool {
	return i.runningState.Load()
}
