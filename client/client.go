package client

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/leader"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jobClient  json.Client
	resClient  json.Client
	dispatcher yarpc.Dispatcher
	ctx        context.Context
	cancelFunc context.CancelFunc
	// Debug is whether debug output is enabled
	Debug bool
}

// New returns a new RPC client given a framework URL and timeout and error
func New(
	discovery leader.Discovery,
	timeout time.Duration,
	debug bool) (*Client, error) {

	jobmgrURL, err := discovery.GetJobMgrURL()
	if err != nil {
		return nil, err
	}

	resmgrURL, err := discovery.GetResMgrURL()
	if err != nil {
		return nil, err
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-client",
		Outbounds: yarpc.Outbounds{
			"peloton-master": transport.Outbounds{
				Unary: http.NewOutbound(jobmgrURL.String()),
			},
			"peloton-resmgr": transport.Outbounds{
				Unary: http.NewOutbound(resmgrURL.String()),
			},
		},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	client := Client{
		Debug:      debug,
		jobClient:  json.New(dispatcher.ClientConfig("peloton-master")),
		resClient:  json.New(dispatcher.ClientConfig("peloton-resmgr")),
		dispatcher: dispatcher,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	return &client, nil
}

// Cleanup ensures the client's YARPC dispatcher is stopped
func (c *Client) Cleanup() {
	defer c.cancelFunc()
	c.dispatcher.Stop()
}
