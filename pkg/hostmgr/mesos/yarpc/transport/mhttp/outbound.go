// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This is a fork of yarpc http outbound. The only reason to fork it is because it
// appends application prefix in all HTTP headers.

// TODO: add an option to http outbound so that we can disable the
// application prefix.

package mhttp

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/uber-go/atomic"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/net/context/ctxhttp"
)

// ErrNoLeader represents no leader is available for outbound.
type ErrNoLeader string

func (e ErrNoLeader) Error() string {
	return fmt.Sprintf("%s has no active leader", string(e))
}

const (
	// outboundError is a metric to represent the number of outbound
	// errors occurred on request to Mesos Master.
	outboundError = "host_manager_mesos_outbound_errors"
)

var (
	errOutboundAlreadyStarted = yarpcerrors.InternalErrorf("Outbound already started")
	errOutboundNotStarted     = yarpcerrors.InternalErrorf("Outbound not started")
	errNoLeader               = yarpcerrors.InternalErrorf("No leader available for outbound")
)

type outboundConfig struct {
	keepAlive       time.Duration
	MaxConnsPerHost int
}

var defaultConfig = outboundConfig{
	keepAlive:       30 * time.Second,
	MaxConnsPerHost: 1024,
}

// OutboundOption customizes the behavior of a Mesos HTTP outbound.
type OutboundOption func(*outboundConfig)

// KeepAlive specifies the keep-alive period for the network connection. If
// zero, keep-alives are disabled.
//
// Defaults to 30 seconds.
func KeepAlive(t time.Duration) OutboundOption {
	return func(c *outboundConfig) {
		c.keepAlive = t
	}
}

// MaxConnectionsPerHost defines the max connections per host
// Default value is 1024
func MaxConnectionsPerHost(conns int) OutboundOption {
	return func(c *outboundConfig) {
		c.MaxConnsPerHost = conns
	}
}

// LeaderDetector provides current leader's hostport.
type LeaderDetector interface {
	// Current leader's hostport, or empty string if no leader.
	HostPort() string
}

// NewOutbound builds a new HTTP outbound that sends requests to the given
// URL.
func NewOutbound(
	parent tally.Scope,
	detector LeaderDetector,
	urlTemplate url.URL,
	defaultHeaders http.Header,
	opts ...OutboundOption) transport.Outbounds {

	cfg := defaultConfig
	for _, o := range opts {
		o(&cfg)
	}

	// Instead of using a global client for all outbounds, we use an HTTP
	// client per outbound if unspecified.
	client := buildClient(&cfg)

	// TODO: Use option pattern with varargs instead
	return transport.Outbounds{
		Unary: &outbound{
			Client:         client,
			detector:       detector,
			started:        atomic.NewBool(false),
			urlTemplate:    urlTemplate,
			scope:          parent,
			defaultHeaders: defaultHeaders,
		},
	}
}

type outbound struct {
	Client      *http.Client
	detector    LeaderDetector
	started     *atomic.Bool
	urlTemplate url.URL
	scope       tally.Scope

	defaultHeaders http.Header
}

func (o *outbound) Start() error {
	if o.started.Swap(true) {
		return errOutboundAlreadyStarted
	}
	return nil
}

func (o *outbound) Stop() error {
	if !o.started.Swap(false) {
		return errOutboundNotStarted
	}
	return nil
}

func (o *outbound) Call(
	ctx context.Context,
	req *transport.Request) (*transport.Response, error) {

	if !o.started.Load() {
		// panic because there's no recovery from this
		panic(errOutboundNotStarted)
	}

	start := time.Now()
	deadline, _ := ctx.Deadline()
	ttl := deadline.Sub(start)

	hostPort := o.detector.HostPort()
	if len(hostPort) == 0 {
		return nil, errNoLeader
	}

	actualURL := o.urlTemplate
	actualURL.Host = hostPort

	request, err := http.NewRequest("POST", actualURL.String(), req.Body)
	if err != nil {
		return nil, err
	}

	request.Header = http.Header{}
	for k, v := range o.defaultHeaders {
		for _, vv := range v {
			request.Header.Set(k, vv)
		}
	}
	for k, v := range req.Headers.Items() {
		request.Header.Set(k, v)
	}
	request.Header.Set(CallerHeader, req.Caller)
	request.Header.Set(ServiceHeader, req.Service)
	request.Header.Set(ProcedureHeader, req.Procedure)
	request.Header.Set(TTLMSHeader, fmt.Sprintf("%d", ttl/time.Millisecond))

	encoding := string(req.Encoding)
	if encoding != "" {
		request.Header.Set(EncodingHeader, encoding)
	}

	response, err := ctxhttp.Do(ctx, o.Client, request)
	if err != nil {
		o.scope.Counter(outboundError).Inc(1)
		if err == context.DeadlineExceeded {
			return nil, yarpcerrors.DeadlineExceededErrorf(
				"Outbound service timeout: service: %s, procedure: %s, timeout: %v",
				req.Service,
				req.Procedure,
				deadline.Sub(start))
		}

		return nil, err
	}

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return &transport.Response{
			Headers: transport.NewHeaders(),
			Body:    response.Body,
		}, nil
	}

	// TODO Behavior for 300-range status codes is undefined
	contents, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if err := response.Body.Close(); err != nil {
		return nil, err
	}

	// Trim the trailing newline from HTTP error messages
	message := fmt.Sprintf(
		"{\"status_code\": %d, \"contents\": \"%s\"}",
		response.StatusCode,
		strings.TrimSuffix(string(contents), "\n"))

	if response.StatusCode == 401 {
		return nil, yarpcerrors.UnauthenticatedErrorf(message)
	}

	if response.StatusCode >= 400 && response.StatusCode < 500 {
		return nil, yarpcerrors.InternalErrorf(message)
	}

	if response.StatusCode == http.StatusGatewayTimeout {
		return nil, yarpcerrors.DeadlineExceededErrorf(message)
	}

	return nil, yarpcerrors.UnknownErrorf(message)
}

// IsRunning returns the running state.
func (o *outbound) IsRunning() bool {
	return o.started.Load()
}

// Transports returns the transports used by the Outbound.
func (o *outbound) Transports() []transport.Transport {
	return nil
}
