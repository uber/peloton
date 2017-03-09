package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"code.uber.internal/infra/peloton/common"
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
	jobmgrURL url.URL,
	timeout time.Duration,
	debug bool,
	resmgrURL url.URL) (*Client, error) {

	// use whereever the master roots its RPC path
	jobmgrURL.Path = common.PelotonEndpointPath
	resmgrURL.Path = common.PelotonEndpointPath

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
