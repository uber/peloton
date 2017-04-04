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
	"go.uber.org/yarpc/transport"
	"golang.org/x/net/context/ctxhttp"

	"code.uber.internal/infra/peloton/yarpc/internal/errors"
)

// ErrNoLeader represents no leader is available for outbound.
type ErrNoLeader string

func (e ErrNoLeader) Error() string {
	return fmt.Sprintf("%s has no active leader", string(e))
}

var (
	errOutboundAlreadyStarted = errors.ErrOutboundAlreadyStarted("http.Outbound")
	errOutboundNotStarted     = errors.ErrOutboundNotStarted("http.Outbound")
	errNoLeader               = errors.ErrOutboundNotStarted("http.Outbound")
)

type outboundConfig struct {
	keepAlive time.Duration
}

var defaultConfig = outboundConfig{keepAlive: 30 * time.Second}

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

// LeaderDetector provides current leader's hostport.
type LeaderDetector interface {
	// Current leader's hostport, or empty string if no leader.
	HostPort() string
}

// NewOutbound builds a new HTTP outbound that sends requests to the given
// URL.
func NewOutbound(
	detector LeaderDetector,
	urlTemplate url.URL,
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
		Unary: outbound{
			Client:      client,
			detector:    detector,
			started:     atomic.NewBool(false),
			urlTemplate: urlTemplate,
		},
	}
}

type outbound struct {
	Client      *http.Client
	detector    LeaderDetector
	started     *atomic.Bool
	urlTemplate url.URL
}

func (o outbound) Start(d transport.Deps) error {
	if o.started.Swap(true) {
		return errOutboundAlreadyStarted
	}
	return nil
}

func (o outbound) Stop() error {
	if !o.started.Swap(false) {
		return errOutboundNotStarted
	}
	return nil
}

func (o outbound) Call(
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
		if err == context.DeadlineExceeded {
			return nil, errors.ClientTimeoutError(
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

	if response.StatusCode >= 400 && response.StatusCode < 500 {
		return nil, errors.RemoteBadRequestError(message)
	}

	if response.StatusCode == http.StatusGatewayTimeout {
		return nil, errors.RemoteTimeoutError(message)
	}

	return nil, errors.RemoteUnexpectedError(message)
}
