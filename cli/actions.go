package main

import (
	ej "encoding/json"
	"fmt"
	"net/url"
	"time"

	"context"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jsonClient json.Client
	dispatcher yarpc.Dispatcher
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// Returns a new RPC client given a framework URL and timeout and error
func newClient(frameworkURL url.URL, timeout time.Duration) (*Client, error) {
	outbound := http.NewOutbound(frameworkURL.String())
	pOutbounds := transport.Outbounds{
		Unary: outbound,
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-client",
		Outbounds: yarpc.Outbounds{"peloton-master": pOutbounds},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	client := Client{
		jsonClient: json.New(dispatcher.ClientConfig("peloton-master")),
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

func printResponseJSON(response interface{}) {
	buffer, err := ej.MarshalIndent(response, "", "  ")
	if err == nil {
		fmt.Printf("%v\n", string(buffer))
	} else {
		fmt.Printf("MarshalIndent err=%v\n", err)
	}
}
