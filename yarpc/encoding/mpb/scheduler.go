package mpb

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/yarpc/api/transport"
	"golang.org/x/net/context"

	"code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"
)

// SchedulerClient makes Mesos JSON requests to Mesos endpoint
type SchedulerClient interface {
	// Call performs an outbound Mesos JSON request.
	// Returns an error if the request failed.
	Call(mesosStreamID string, msg *mesos_v1_scheduler.Call) error
}

// NewSchedulerClient builds a new Mesos Scheduler JSON client.
func NewSchedulerClient(c transport.ClientConfig, contentType string) SchedulerClient {
	return &schedulerClient{
		cfg:         c,
		contentType: contentType,
	}
}

type schedulerClient struct {
	cfg         transport.ClientConfig
	contentType string
}

func (c *schedulerClient) Call(mesosStreamID string, msg *mesos_v1_scheduler.Call) error {
	headers := transport.NewHeaders().
		With("Mesos-Stream-Id", mesosStreamID).
		With("Content-Type", fmt.Sprintf("application/%s", c.contentType)).
		With("Accept", fmt.Sprintf("application/%s", c.contentType))

	body, err := MarshalPbMessage(msg, c.contentType)
	if err != nil {
		return fmt.Errorf(
			"failed to marshal subscribe call to contentType %s %v",
			c.contentType,
			err,
		)
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
