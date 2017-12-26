package client

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	agent "code.uber.internal/infra/peloton/.gen/mesos/v1/agent"
	executor "code.uber.internal/infra/peloton/.gen/mesos/v1/executor"
)

// Client is the struct that will interface directly with the network to make API
// calls with the mesos agent on the host.
type Client struct {
	agentAddress string
	handler      EventHandler

	// The function that returns the value of an environment variable
	Environment func(string) string

	// The client that will be used to keep the connection alive after a SUBSCRIBE call
	SubscribeClient HTTPDoer

	// The client that will be used to make regular calls to the mesos agent
	OutboundClient HTTPDoer

	Marshal   MarshalFunc
	Unmarshal UnmarshalFunc
}

// HTTPDoer is the interface that is needed to interact with the mesos API. The *http.Client
// struct implements this.
type HTTPDoer interface {
	Do(req *http.Request) (resp *http.Response, err error)
}

// MarshalFunc is the function signature to marshal protobuf messages.
type MarshalFunc func(writer io.Writer, msg proto.Message) error

// UnmarshalFunc is the function signature to unmarshal protobuf messages.
type UnmarshalFunc func([]byte, proto.Message) error

// New returns a new mesos client with the streamer and regular http client provided.
func New(agentAddress string, handler EventHandler) *Client {
	marshaller := jsonpb.Marshaler{OrigName: true}
	return &Client{
		agentAddress: agentAddress,
		handler:      handler,

		Environment:     os.Getenv,
		SubscribeClient: &http.Client{},
		OutboundClient:  &http.Client{},

		Marshal:   marshaller.Marshal,
		Unmarshal: proto.Unmarshal,
	}
}

// ExecutorCall implements executor::Call
func (c *Client) ExecutorCall(call *executor.Call) (int, error) {
	if call.Type == nil {
		return 0, fmt.Errorf("Call with no type is not accepted")
	}

	req, err := c.request(call, true)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to create executor request")
	}

	if *call.Type == executor.Call_SUBSCRIBE {
		return c.subscribe(req)
	}

	resp, err := c.OutboundClient.Do(req)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to get executor response")
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, errors.Wrapf(err, "Failed to read response body")
	} else if resp.StatusCode != http.StatusAccepted {
		return resp.StatusCode, errors.Errorf("Bad status code from mesos agent: (%v), %v", resp.StatusCode, string(content))
	}
	return resp.StatusCode, nil
}

// AgentCall makes a call to the mesos agent.
func (c *Client) AgentCall(call *agent.Call) (int, error) {
	if call.Type == nil {
		return 0, fmt.Errorf("Call with no type is not accepted")
	}

	req, err := c.request(call, false)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to create launch request")
	}

	resp, err := c.OutboundClient.Do(req)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to get launch response")
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, errors.Wrapf(err, "Failed to read response body")
	} else if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, errors.Errorf("Bad status code from mesos agent: (%v), %v", resp.StatusCode, string(content))
	}

	if *call.Type == agent.Call_WAIT_NESTED_CONTAINER {
		waitresp := &agent.Response{}
		if err = c.Unmarshal(content, waitresp); err != nil {
			return resp.StatusCode, errors.Wrapf(err, "Failed to parse protobuf")
		}
		return int(*waitresp.WaitNestedContainer.ExitStatus), nil
	}
	return resp.StatusCode, nil
}

// This call will block until the connection encounters an error or the agent willfully
// disconnects from the client.
func (c *Client) subscribe(req *http.Request) (int, error) {
	resp, err := c.SubscribeClient.Do(req)
	if err != nil {
		return 0, errors.Wrapf(err, "Failed to get subscribe response")
	}

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		content, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, errors.Wrapf(err, "Failed to read response body")
		}
		return resp.StatusCode, errors.Errorf("Bad response to subscribe: (%v) %v", resp.StatusCode, string(content))
	}

	log.Debugf("Got a good response from subscribe")
	c.handler.Connected()
	return resp.StatusCode, DispatchEvents(resp.Body, c.Unmarshal, c.handler)
}

func (c Client) request(call proto.Message, executor bool) (*http.Request, error) {
	body := &bytes.Buffer{}
	err := c.Marshal(body, call)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to marshal call to protobuf")
	}

	url := "http://" + path.Join(c.agentAddress, "/api/v1")
	if executor {
		url += "/executor"
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create new request")
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	return req, nil
}
