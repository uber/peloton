package mjson

import (
	"fmt"
	"strings"

	"golang.org/x/net/context"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/transport"
)

// Client makes Mesos JSON requests to Mesos endpoint
type Client interface {
	// Call performs an outbound Mesos JSON request.
	//
	// Returns an error if the request failed.
	Call(reqMeta yarpc.CallReqMeta, msg proto.Message) error
}

// New builds a new Mesos JSON client.
func New(c transport.Channel) Client {
	return mjsonClient{ch: c}
}

type mjsonClient struct {
	ch transport.Channel
}

func (c mjsonClient) Call(reqMeta yarpc.CallReqMeta, msg proto.Message) error {
	encoder := jsonpb.Marshaler{
		EnumsAsInts: false,
		OrigName:    true,
	}
	body, err := encoder.MarshalToString(msg)
	if err != nil {
		return fmt.Errorf("Failed to marshal call message: %s", err)
	}

	treq := transport.Request{
		Caller:   c.ch.Caller(),
		Service:  c.ch.Service(),
		Encoding: Encoding,
		Headers: transport.Headers(reqMeta.GetHeaders()),
		Body: strings.NewReader(body),
	}

	ctx := context.Background()
	_, err = c.ch.GetOutbound().Call(ctx, &treq)

	// All Mesos calls are one-way so no need to decode response body
	return err
}
