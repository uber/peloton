package mhttp

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"code.uber.internal/go-common.git/x/log"
	"github.com/yarpc/yarpc-go/transport"
	"golang.org/x/net/context"
)

// OutboundOption is an option for an Mesos HTTP outbound.
type OutboundOption func(*outbound)

// NewOutbound builds a new Mesos HTTP outbound, with the inbound, which is to register with
// Mesos master via Subscribe message
func NewOutbound(hostPort string, d MesosDriver, mesosStreamId string, opts ...OutboundOption) transport.Outbound {
	o := &outbound{hostPort: hostPort, driver: d}
	for _, opt := range opts {
		opt(o)
	}
	log.Infof("mOutbound using mesos stream id %v", mesosStreamId)
	o.mesosStreamId = mesosStreamId
	return o
}

type outbound struct {
	hostPort      string
	driver        MesosDriver
	stopped       uint32
	mesosStreamId string
	client        *http.Client
}

// Start thee outbound
func (o *outbound) Start(d transport.Deps) error {
	transport := &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}
	o.client = &http.Client{Transport: transport}
	return nil
}

func (o *outbound) Stop() error {
	atomic.CompareAndSwapUint32(&o.stopped, 0, 1)
	return nil
}

// Call simply sent the body to mesos master, with appropriate headers added.
// Note: yarpc http outbound cannot be used for this as it does not handle arbitrary headers.
// It always translate the headers to application headers or baggage headers
func (o outbound) Call(ctx context.Context, treq *transport.Request) (*transport.Response, error) {
	url := fmt.Sprintf("http://%s%s", o.hostPort, o.driver.Endpoint())
	req, err := http.NewRequest("POST", url, treq.Body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// Obtain the MesosStreamId from the inbound
	mesosStreamId := o.mesosStreamId
	if mesosStreamId == "" {
		return nil, fmt.Errorf("mesosStreamId is not set")
	}
	req.Header.Set("Mesos-Stream-Id", mesosStreamId)
	log.Infof("Mesos stream id %v", mesosStreamId)
	response, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var contentStr = string(contents)
	log.Infof("Response code %v, mesos-stream-id %v , content=%v", response.StatusCode, mesosStreamId, contentStr)

	// TODO: 3xx is not processed and is returned as is
	if response.StatusCode >= 400 && response.StatusCode < 500 {
		return nil, fmt.Errorf("Response code 4xx %v, message : %v ", response.StatusCode, contentStr)
	}
	return &transport.Response{
		Body: ioutil.NopCloser(strings.NewReader(contentStr)),
	}, nil
}

// Options for the HTTP transport.
func (o outbound) Options() (options transport.Options) {
	return options
}
