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
	Call(mesosStreamID string, msg proto.Message) error
}

// New builds a new Mesos JSON client.
func New(c transport.ClientConfig, contentType string) Client {
	return mpbClient{cfg: c, contentType: contentType}
}

type mpbClient struct {
	cfg         transport.ClientConfig
	contentType string
}

func (c mpbClient) Call(mesosStreamID string, msg proto.Message) error {
	headers := yarpc.NewHeaders().
		With("Mesos-Stream-Id", mesosStreamID).
		With("Content-Type", fmt.Sprintf("application/%s", c.contentType)).
		With("Accept", fmt.Sprintf("application/%s", c.contentType))

	body, err := MarshalPbMessage(msg, c.contentType)
	if err != nil {
		return fmt.Errorf("Failed to marshal subscribe call to contentType %s %v", c.contentType, err)
	}

	treq := transport.Request{
		Caller:    c.cfg.Caller(),
		Service:   c.cfg.Service(),
		Encoding:  Encoding,
		Procedure: "Scheduler_Call",
		Headers:   transport.Headers(headers),
		Body:      strings.NewReader(body),
	}

	ctx, _ := context.WithTimeout(context.Background(), 100*1000*time.Millisecond)
	_, err = c.cfg.GetUnaryOutbound().Call(ctx, &treq)

	// All Mesos calls are one-way so no need to decode response body
	return err
}
