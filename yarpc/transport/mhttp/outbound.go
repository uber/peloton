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
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/yarpc/yarpc-go/transport"
	"golang.org/x/net/context"
)

// Outbound represents an Mesos HTTP Outbound. It is the same as the transport Outbound
// except it exposes the address on which the system is listening for
// connections.
type Outbound interface {
	transport.Outbound
	SendPbRequest(msg proto.Message) error
}

// OutboundOption is an option for an Mesos HTTP outbound.
type OutboundOption func(*outbound)

// NewOutbound builds a new Mesos HTTP outbound, with the inbound, which is to register with
// Mesos master via Subscribe message
func NewOutbound(hostPort string, d MesosDriver, inbound Inbound, opts ...OutboundOption) Outbound {
	o := &outbound{hostPort: hostPort, driver: d}
	for _, opt := range opts {
		opt(o)
	}
	o.inbound = inbound
	return o
}

type outbound struct {
	hostPort string
	driver   MesosDriver
	stopped  uint32
	inbound  Inbound
	client   *http.Client
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

// SendPbRequest sends a protocol buffer message to the server (mesos master)
func (o *outbound) SendPbRequest(msg proto.Message) error {
	encoder := jsonpb.Marshaler{
		EnumsAsInts: false,
		OrigName:    true,
	}
	body, err := encoder.MarshalToString(msg)
	if err != nil {
		return fmt.Errorf("Failed to marshal subscribe call: %s", err)
	}
	log.WithField("sent", body).Infof("Sending accept request")
	ctx := context.Background()
	var tReq = transport.Request{
		Body: strings.NewReader(body),
	}
	_, err = o.Call(ctx, &tReq)
	log.WithField("request", tReq).Infof("response from accept request %v", err)
	return err
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
	mesosStreamId := o.inbound.GetMesosStreamId()
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
