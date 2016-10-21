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
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/yarpc/yarpc-go/transport"
)

// Inbound represents an Mesos HTTP Inbound. It is the same as the transport Inbound
// except it exposes the address on which the system is listening for
// connections.
type Inbound interface {
	transport.Inbound
}

// InboundOption is an option for an Mesos HTTP inbound.
type InboundOption func(*inbound)

// NewInbound builds a new Mesos HTTP inbound after registering with
// Mesos master via Subscribe message
func NewInbound(hostPort string, d MesosDriver, opts ...InboundOption) Inbound {
	i := &inbound{hostPort: hostPort, driver: d, done: make(chan error, 1)}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

type inbound struct {
	hostPort string
	driver   MesosDriver
	stopped  uint32
	done     chan error
}

func (i *inbound) Start(h transport.Handler, d transport.Deps) error {
	var err error

	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}
	client := &http.Client{Transport: transport}

	msg := i.driver.PrepareSubscribe()
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

	resp, err := client.Do(req)
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
		Handler:       h,
		Service:       i.driver.Name(),
		Caller:        i.hostPort,
		EventDataType: i.driver.EventDataType(),
	}

	go func() error {
		defer resp.Body.Close()
		reader := bufio.NewReader(resp.Body)
		for {
			if i.stopped != 0 {
				log.Infof("mInound stopped")
				break
			}

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

			// TODO: spawn a goroutine to process frame
			err = hdl.HandleRecordIO(buf)
			if err != nil {
				errMsg := fmt.Sprintf("Failed to handle record IO event: %s", err)
				log.Errorf(errMsg)
				return fmt.Errorf(errMsg)
			}
		}
		// TODO: handle error conditions more gracefully
		i.done <- nil
		return nil
	}()
	return nil
}

func (i *inbound) Stop() error {
	if !atomic.CompareAndSwapUint32(&i.stopped, 0, 1) {
		return nil
	}

	serveErr := <-i.done
	return serveErr
}
