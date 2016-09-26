package yarpc

import (
	"code.uber.internal/go-common.git/x/log"
	"fmt"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/yarpc/yarpc-go/transport"
	"golang.org/x/net/context"
	"strings"
)

// Caller provides helper functions to send requests to invoke mesos functions on mesos master
type Caller struct {
	outbound transport.Outbound
}

func NewMesoCaller(outbound transport.Outbound) Caller {
	return Caller{
		outbound: outbound,
	}
}

// SendPbRequest sends a protocol buffer message to the server (mesos master)
func (c *Caller) SendPbRequest(msg proto.Message) error {
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
	_, err = c.outbound.Call(ctx, &tReq)
	log.WithField("request", tReq).Infof("response from accept request %v", err)
	return err
}

// TODO: add more helper function to call into mesos
