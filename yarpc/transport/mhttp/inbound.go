// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mhttp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/pkg/errors"
	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/transport"
)

const (
	// MesosHTTPConnTimeout is the mesos connection timeout
	MesosHTTPConnTimeout = 30 * time.Second

	// MesosHTTPConnKeepAlive is the mesos connection keep alive
	MesosHTTPConnKeepAlive = 30 * time.Second

	_stopRetryInterval = 100 * time.Millisecond
)

// Inbound represents a Mesos HTTP Inbound. It is the same as the
// transport.Inbound except it exposes the address on which the system is
// listening for connections.
type Inbound interface {
	transport.Inbound

	StartMesosLoop(ctx context.Context, newHostPort string) (chan error, error)
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

	hostPort     string
	driver       MesosDriver
	stopFlag     atomic.Bool
	router       transport.Router
	client       *http.Client
	runningState atomic.Bool
	ticker       *time.Ticker
}

// Start would initialize some variables, actual mesos communication would be
// started by StartMesosLoop(...)
func (i *inbound) Start() error {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   MesosHTTPConnTimeout,
			KeepAlive: MesosHTTPConnKeepAlive,
		}).Dial,
	}
	i.client = &http.Client{Transport: transport}
	return nil
}

// StartMesosLoop subscribes to mesos master as a framework, and starts a
// go-routine to dispatch the mesos callbacks.
// The call can be called multiple times to start/stop talking to Mesos master,
// or can be used to switch new Mesos master leader after a fail over.
func (i *inbound) StartMesosLoop(ctx context.Context, hostPort string) (chan error, error) {
	log.WithField("hostport", hostPort).Info("StartMesosLoop called")

	if len(hostPort) == 0 {
		return nil, errors.New("Empty hostport when starting Mesos loop")
	}

	i.Lock()
	defer i.Unlock()

	i.metrics.StartCount.Inc(1)

	if i.runningState.Load() {
		if i.hostPort != hostPort {
			i.metrics.LeaderChanges.Inc(1)
			log.WithFields(log.Fields{
				"old": i.hostPort,
				"new": hostPort,
			}).Info("Mesos leader address changed.")
		} // TODO: Determine whether we need check else case.

		i.stopInternal()
	}

	i.stopFlag.Store(false)
	i.metrics.Stopped.Update(0)
	log.WithField("hostport", hostPort).
		Info("Starting the inbound for mesos master")
	i.hostPort = hostPort

	req, err := i.driver.PrepareSubscribeRequest(ctx, hostPort)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to PrepareSubscribeRequest: %v", err)
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to POST subscribe request to master: %v", err)
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		respBody, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"Failed to subscribe to master (Status=%d): %s",
			resp.StatusCode,
			respBody)
	}

	// Invoke the post subscribe callback on Mesos driver
	values := resp.Header["Mesos-Stream-Id"]
	if len(values) != 1 {
		return nil, fmt.Errorf(
			"Failed to obtain stream id from values: %v",
			values)
	}
	i.driver.PostSubscribe(ctx, values[0])

	started := make(chan interface{}, 1)
	end := make(chan error, 1)

	go func() {
		end <- i.processUntilEnd(started, resp)
	}()

	<-started
	return end, nil
}

func (i *inbound) processUntilEnd(
	started chan interface{},
	resp *http.Response) error {

	defer i.runningState.Store(false)
	defer i.metrics.Running.Update(0)
	defer resp.Body.Close()

	hdl := handler{
		Router:        i.router,
		Service:       i.driver.Name(),
		Caller:        i.hostPort,
		EventDataType: i.driver.EventDataType(),
		ContentType:   i.driver.GetContentEncoding(),
	}

	i.runningState.Store(true)
	i.metrics.Running.Update(1)
	started <- nil
	reader := bufio.NewReader(resp.Body)
	for {
		if stopped := i.stopFlag.Load(); stopped {
			log.Info("mInbound go routine stopped")
			return nil
		}

		// NOTE: if the master decide to disconnect the framework,
		// we can read a EOF here. Caller should ensure this inbound is
		// started again with up to date leader address.

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
			return errors.New(msg)
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

// SetRouter sets the router associated with the inbound.
func (i *inbound) SetRouter(r transport.Router) {
	i.router = r
}

// Transports returns the transports used by the Inbound.
func (i *inbound) Transports() []transport.Transport {
	return nil
}
