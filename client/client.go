package client

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"code.uber.internal/infra/peloton/master/config"
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
func New(frameworkURL url.URL, timeout time.Duration, debug bool, resframeworkURL url.URL) (*Client, error) {

	// use whereever the master roots its RPC path
	frameworkURL.Path = config.FrameworkURLPath
	resframeworkURL.Path = config.FrameworkURLPath

	outbound := http.NewOutbound(frameworkURL.String())
	pOutbounds := transport.Outbounds{
		Unary: outbound,
	}

	resoutbound := http.NewOutbound(resframeworkURL.String())
	resOutbounds := transport.Outbounds{
		Unary: resoutbound,
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-client",
		Outbounds: yarpc.Outbounds{
			"peloton-master": pOutbounds,
			"peloton-resmgr": resOutbounds,
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
