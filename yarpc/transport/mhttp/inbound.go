package mhttp

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/yarpc/transport"
)

const (
	RunningState_NotStarted = 0
	RunningState_Running    = 1
)

// Inbound represents an Mesos HTTP Inbound. It is the same as the transport Inbound
// except it exposes the address on which the system is listening for
// connections.
type Inbound interface {
	transport.Inbound
	StartMesosLoop(newHostPort string) error
	GetRunningState() int32
}

// InboundOption is an option for an Mesos HTTP inbound.
type InboundOption func(*inbound)

// NewInbound builds a new Mesos HTTP inbound after registering with
// Mesos master via Subscribe message
func NewInbound(d MesosDriver, opts ...InboundOption) Inbound {
	i := &inbound{driver: d, mutex: &sync.Mutex{}, runningState: int32(RunningState_NotStarted)}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type inbound struct {
	mutex        *sync.Mutex
	hostPort     string
	driver       MesosDriver
	stopFlag     int32
	handler      transport.Handler
	deps         transport.Deps
	client       *http.Client
	runningState int32
}

// Start would initialize some variables, actual mesos communication would be
// started by StartMesosLoop(...)
func (i *inbound) Start(h transport.Handler, d transport.Deps) error {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}
	i.client = &http.Client{Transport: transport}
	i.handler = h
	i.deps = d
	return nil
}

// StartMesosLoop subscribes to mesos master as a framework, and starts a go-routine to
// dispatch the mesos callbacks.
// The call can be called multiple times to start / stop talking to mesos master, or can be
// used to switch between mesos masters when mesos master itself fails over
func (i *inbound) StartMesosLoop(hostPort string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	var err error

	i.stopFlag = 0
	runningState := atomic.LoadInt32(&i.runningState)
	if i.hostPort == hostPort && runningState == RunningState_Running {
		log.Infof("mesos hostPort is already %v, and inbound is running, thus return", hostPort)
		return nil
	}

	if runningState == RunningState_Running {
		log.Infof("Stopping the inbound for mesos master %v", i.hostPort)
		i.stopInternal()
	}
	log.Infof("Starting the inbound for mesos master %v", hostPort)
	i.hostPort = hostPort

	msg := i.driver.PrepareSubscribe()
	i.hostPort = hostPort
	encoder := jsonpb.Marshaler{
		EnumsAsInts: false,
		OrigName:    true,
	}
	body, err := encoder.MarshalToString(msg)
	if err != nil {
		return fmt.Errorf("Failed to marshal subscribe call: %s", err)
	}
	url := fmt.Sprintf("http://%s%s", i.hostPort, i.driver.Endpoint())

	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to POST request to master: %s", err)
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Failed to subscribe to master (Status=%d): %s",
			resp.StatusCode, respBody)
	}

	// Invoke the post subscribe callback on Mesos driver
	mesosStreamId := resp.Header["Mesos-Stream-Id"]
	i.driver.PostSubscribe(mesosStreamId[0])

	hdl := handler{
		Handler:       i.handler,
		Service:       i.driver.Name(),
		Caller:        i.hostPort,
		EventDataType: i.driver.EventDataType(),
	}

	started := make(chan int, 1)
	go func() error {
		defer atomic.StoreInt32(&i.runningState, RunningState_NotStarted)
		defer resp.Body.Close()

		atomic.StoreInt32(&i.runningState, RunningState_Running)
		started <- 0
		reader := bufio.NewReader(resp.Body)
		for {
			stopFlag := atomic.LoadInt32(&i.stopFlag)
			if stopFlag != 0 {
				log.Infof("mInbound go routine stopped")
				return nil
			}
			// TODO: if the master decide to disconnect the framework,
			// we can read a EOF here. We need to handle this case by
			// detect and re-register again with different framework

			// Read the length of the next RecordIO frame
			line, _, err := reader.ReadLine()
			if err != nil {
				log.Errorf("Failed to read line: %s", err)
				return err
			}

			framelen, err := strconv.ParseUint(string(line), 10, 64)
			if framelen < 1 || err != nil {
				log.Errorf("Failed to read framelen, framelen :%v, err= %v", framelen, err)
				return err
			}

			// Read next RecordIO frame with framelen bytes
			buf := make([]byte, framelen)
			readlen, err := io.ReadFull(reader, buf)
			if err != nil {
				return err
			}
			if uint64(readlen) != framelen {
				errMsg := fmt.Sprintf("Failed to read full frame: read %d bytes, "+
					"expect %d bytes", readlen, framelen)
				log.Errorf(errMsg)
				return fmt.Errorf(errMsg)
			}

			err = hdl.HandleRecordIO(buf)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to handle record IO event: %s", err)
				log.Errorf(errMsg)
				return fmt.Errorf(errMsg)
			}
		}
	}()
	// Wait until go routine is started
	<-started
	return nil
}

// stopInternal must be called with mutex locked
func (i *inbound) stopInternal() error {
	i.stopFlag = 1
	for {
		runningState := atomic.LoadInt32(&i.runningState)
		if runningState == RunningState_Running {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	log.Infof("mInbound stopped")
	return nil
}

// Stop would stop the internal go-routine that receives mesos callback
// and disconnect with current mesos master
func (i *inbound) Stop() error {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	return i.stopInternal()
}

//GetRunningState returns the runningstate
func (i *inbound) GetRunningState() int32 {
	return atomic.LoadInt32(&i.runningState)
}
