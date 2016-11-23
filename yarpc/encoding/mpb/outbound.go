package mpb

import (
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"golang.org/x/net/context"
)

// Client makes Mesos JSON requests to Mesos endpoint
type Client interface {
	// Call performs an outbound Mesos JSON request.
	//
	// Returns an error if the request failed.
	Call(mesosStreamId string, msg proto.Message) error
}

// New builds a new Mesos JSON client.
func New(c transport.Channel, contentType string) Client {
	return mpbClient{ch: c, contentType: contentType}
}

type mpbClient struct {
	ch          transport.Channel
	contentType string
}

func (c mpbClient) Call(mesosStreamId string, msg proto.Message) error {
	headers := yarpc.NewHeaders().
		With("Mesos-Stream-Id", mesosStreamId).
		With("Content-Type", fmt.Sprintf("application/%s", c.contentType)).
		With("Accept", fmt.Sprintf("application/%s", c.contentType))

	body, err := MarshalPbMessage(msg, c.contentType)
	if err != nil {
		return fmt.Errorf("Failed to marshal subscribe call to contentType %s %v", c.contentType, err)
	}

	treq := transport.Request{
		Caller:    c.ch.Caller(),
		Service:   c.ch.Service(),
		Encoding:  Encoding,
		Procedure: "Scheduler_Call",
		Headers:   transport.Headers(headers),
		Body:      strings.NewReader(body),
	}

	ctx, _ := context.WithTimeout(context.Background(), 100*1000*time.Millisecond)
	_, err = c.ch.GetOutbound().Call(ctx, &treq)

	// All Mesos calls are one-way so no need to decode response body
	return err
}
