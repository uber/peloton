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

package middleware

import (
	"context"

	"go.uber.org/yarpc/api/middleware"
	"go.uber.org/yarpc/api/transport"
)

const (
	_usernameHeader = "username"
	_passwordHeader = "password"
)

type outboundMiddleware interface {
	middleware.UnaryOutbound
	middleware.OnewayOutbound
	middleware.StreamOutbound
}

var _ outboundMiddleware = &BasicAuthOutboundMiddleware{}

// BasicAuthConfig is the config for basic auth
type BasicAuthConfig struct {
	Username string
	Password string
}

// BasicAuthOutboundMiddleware provides basic auth
// support for all outbound requests
type BasicAuthOutboundMiddleware struct {
	config *BasicAuthConfig
}

// NewBasicAuthOutboundMiddleware creates BasicAuthOutboundMiddleware
func NewBasicAuthOutboundMiddleware(config *BasicAuthConfig) *BasicAuthOutboundMiddleware {
	if config != nil && len(config.Password) == 0 && len(config.Username) == 0 {
		config = nil
	}

	return &BasicAuthOutboundMiddleware{
		config: config,
	}
}

// Call adds auth info to yarpc request header and relay the request
func (m *BasicAuthOutboundMiddleware) Call(ctx context.Context, request *transport.Request, out transport.UnaryOutbound) (*transport.Response, error) {
	request.Headers = m.addAuthToHeader(request.Headers)
	return out.Call(ctx, request)
}

// CallOneway adds auth info to yarpc request header and relay the request
func (m *BasicAuthOutboundMiddleware) CallOneway(ctx context.Context, request *transport.Request, out transport.OnewayOutbound) (transport.Ack, error) {
	request.Headers = m.addAuthToHeader(request.Headers)
	return out.CallOneway(ctx, request)
}

// CallStream adds auth info to yarpc request header and relay the request
func (m *BasicAuthOutboundMiddleware) CallStream(ctx context.Context, request *transport.StreamRequest, out transport.StreamOutbound) (*transport.ClientStream, error) {
	request.Meta.Headers = m.addAuthToHeader(request.Meta.Headers)
	return out.CallStream(ctx, request)
}

func (m *BasicAuthOutboundMiddleware) addAuthToHeader(headers transport.Headers) transport.Headers {
	if m.config == nil {
		return headers
	}

	headers = headers.With(_usernameHeader, m.config.Username)
	headers = headers.With(_passwordHeader, m.config.Password)

	return headers
}
