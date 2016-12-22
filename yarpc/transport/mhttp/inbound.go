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
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"go.uber.org/yarpc/transport"
)

const (
	runningStateNotStarted = 0
	runningStateRunning    = 1

	// MesosHTTPConnTimeout is the mesos connection timeout
	MesosHTTPConnTimeout = 30 * time.Second
	// MesosHTTPConnKeepAlive is the mesos connection keep alive
	MesosHTTPConnKeepAlive = 30 * time.Second
)

var tickerInterval = time.Second * 1

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
	i := &inbound{driver: d, mutex: &sync.Mutex{}, runningState: int32(runningStateNotStarted)}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type inbound struct {
	mutex         *sync.Mutex
	hostPort      string
	driver        MesosDriver
	stopFlag      int32
	serviceDetail transport.ServiceDetail
	deps          transport.Deps
	client        *http.Client
	runningState  int32
	ticker        *time.Ticker
}

// Start would initialize some variables, actual mesos communication would be
// started by StartMesosLoop(...)
func (i *inbound) Start(service transport.ServiceDetail, d transport.Deps) error {
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

// StartMesosLoop subscribes to mesos master as a framework, and starts a go-routine to
// dispatch the mesos callbacks. It also creates a ticker to periodically check and retry the
// mesos subscription routine.
// The call can be called multiple times to start / stop talking to mesos master, or can be
// used to switch between mesos masters when mesos master itself fails over
func (i *inbound) StartMesosLoop(hostPort string) error {
	log.Infof("StartMesosLoop called")
	err := i.startMesosLoopInternal(hostPort)
	if err != nil {
		log.Errorf("Initial start mesos loop failed, err=%v", err)
	}
	i.mutex.Lock()
	defer i.mutex.Unlock()
	if i.ticker != nil {
		log.Warnf("ticker already created")
		return err
	}
	i.ticker = time.NewTicker(tickerInterval)
	go func() {
		for t := range i.ticker.C {
			// The go routine that does mesos master subscription and event
			// processing can exit due to error (e.g. lose network etc) thus
			// there is a ticker callback to re-kickoff the mesos subscription
			// if needed.
			stopFlag := atomic.LoadInt32(&i.stopFlag)
			runningState := atomic.LoadInt32(&i.runningState)
			if stopFlag == 0 && runningState != runningStateRunning && len(i.hostPort) > 0 {
				log.Infof("Restarting mesos loop at %v, running state %v", t, runningState)
				err := i.startMesosLoopInternal(i.hostPort)
				if err != nil {
					log.Errorf("Failed to start mesos loop, hostport = %v, err = %v", i.hostPort, err)
				}
			}
		}
	}()
	return err
}

// startMesosLoopInternal subscribes to mesos master as a framework, and starts a go-routine to
// dispatch the mesos callbacks.

func (i *inbound) startMesosLoopInternal(hostPort string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	var err error

	if hostPort == "" {
		log.Infof("hostPort empty, thus return")
		return nil
	}
	runningState := atomic.LoadInt32(&i.runningState)
	if i.hostPort == hostPort && runningState == runningStateRunning {
		log.Infof("mesos hostPort is already %v, and inbound is running, thus return", hostPort)
		return nil
	}

	if runningState == runningStateRunning {
		log.Infof("Stopping the inbound for mesos master %v", i.hostPort)
		i.stopInternal()
	}
	atomic.StoreInt32(&i.stopFlag, 0)
	log.Infof("Starting the inbound for mesos master %v", hostPort)
	i.hostPort = hostPort

	req, err := i.driver.PrepareSubscribeRequest(hostPort)
	if err != nil {
		return fmt.Errorf("Failed to PrepareSubscribeRequest: %v", err)
	}
	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("Failed to POST subscribe request to master: %v", err)
	}

	if resp.StatusCode != 200 {
		defer resp.Body.Close()
		respBody, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("Failed to subscribe to master (Status=%d): %s",
			resp.StatusCode, respBody)
	}

	// Invoke the post subscribe callback on Mesos driver
	mesosStreamID := resp.Header["Mesos-Stream-Id"]
	i.driver.PostSubscribe(mesosStreamID[0])

	hdl := handler{
		ServiceDetail: i.serviceDetail,
		Service:       i.driver.Name(),
		Caller:        i.hostPort,
		EventDataType: i.driver.EventDataType(),
		ContentType:   i.driver.GetContentEncoding(),
	}

	started := make(chan int, 1)
	go func() error {
		defer atomic.StoreInt32(&i.runningState, runningStateNotStarted)
		defer resp.Body.Close()

		atomic.StoreInt32(&i.runningState, runningStateRunning)
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
	atomic.StoreInt32(&i.stopFlag, 1)
	for {
		runningState := atomic.LoadInt32(&i.runningState)
		if runningState == runningStateRunning {
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
	log.Infof("mInbound stopping with mesos master url %v", i.hostPort)
	if i.ticker != nil {
		i.ticker.Stop()
		i.ticker = nil
	} else {
		log.Warnf("ticker is already nil")
	}
	return i.stopInternal()
}

//GetRunningState returns the runningstate
func (i *inbound) GetRunningState() int32 {
	return atomic.LoadInt32(&i.runningState)
}
