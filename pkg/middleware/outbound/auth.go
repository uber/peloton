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

package outbound

import (
	"context"

	"github.com/uber/peloton/pkg/auth"

	"go.uber.org/yarpc/api/transport"
)

// AuthOutboundMiddleware is the outbound middleware for auth
type AuthOutboundMiddleware struct {
	auth.SecurityClient
}

// Call adds token info into request and invoke the underlying outbound call
func (a *AuthOutboundMiddleware) Call(
	ctx context.Context,
	request *transport.Request,
	out transport.UnaryOutbound,
) (*transport.Response, error) {
	request.Headers = getHeadersWithToken(request.Headers, a.GetToken())
	return out.Call(ctx, request)
}

// CallOneway adds token info into request and invoke the underlying outbound call
func (a *AuthOutboundMiddleware) CallOneway(
	ctx context.Context,
	request *transport.Request,
	out transport.OnewayOutbound,
) (transport.Ack, error) {
	request.Headers = getHeadersWithToken(request.Headers, a.GetToken())
	return out.CallOneway(ctx, request)
}

// CallStream adds token info into request and invoke the underlying outbound call
func (a *AuthOutboundMiddleware) CallStream(
	ctx context.Context,
	request *transport.StreamRequest,
	out transport.StreamOutbound,
) (*transport.ClientStream, error) {
	request.Meta.Headers = getHeadersWithToken(request.Meta.Headers, a.GetToken())
	return out.CallStream(ctx, request)
}

func getHeadersWithToken(headers transport.Headers, token auth.Token) transport.Headers {
	for k, v := range token.Items() {
		headers = headers.With(k, v)
	}

	return headers
}

// NewAuthOutboundMiddleware returns AuthOutboundMiddleware
func NewAuthOutboundMiddleware(security auth.SecurityClient) *AuthOutboundMiddleware {
	return &AuthOutboundMiddleware{
		SecurityClient: security,
	}
}
