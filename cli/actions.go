package main

import (
	ej "encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"golang.org/x/net/context"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jsonClient json.Client
	dispatcher yarpc.Dispatcher
	ctx        context.Context
}

// Returns a new RPC client given a framework URL and timeout and error
func newClient(frameworkURL url.URL, timeout time.Duration) (*Client, error) {
	outbound := http.NewOutbound(frameworkURL.String())
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-client",
		Outbounds: transport.Outbounds{"peloton-master": outbound},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	client := Client{
		jsonClient: json.New(dispatcher.Channel("peloton-master")),
		dispatcher: dispatcher,
		ctx:        ctx,
	}
	return &client, nil
}

// Cleanup ensures the client's YARPC dispatcher is stopped
func (c *Client) Cleanup() {
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
