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

package forward

import (
	"context"

	"go.uber.org/yarpc/api/transport"
)

var _ transport.UnaryHandler = (*unaryForward)(nil)

// unaryForward implements a UnaryHandler to forward request to given outbound.
type unaryForward struct {
	// outbound is the outbound to forward requests to.
	outbound transport.UnaryOutbound

	// overrideService is the service name to override the original service name.
	overrideService string
}

func NewUnaryForward(
	outbound transport.UnaryOutbound,
	overrideService string,
) *unaryForward {
	return &unaryForward{
		outbound:        outbound,
		overrideService: overrideService,
	}
}

// Handle implements Handle function of UnaryHandler to forward request to given
// outbound and copy the response.
func (uf *unaryForward) Handle(
	ctx context.Context,
	req *transport.Request,
	w transport.ResponseWriter,
) error {
	resp, err := uf.outbound.Call(ctx, uf.preprocess(req))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// copy the response
	if resp.ApplicationError {
		w.SetApplicationError()
	}
	w.AddHeaders(resp.Headers)
	_, err = copy(w, resp.Body)
	return err
}

// preprocess overrides request service if overrideService is provided.
func (uf *unaryForward) preprocess(req *transport.Request) *transport.Request {
	if uf.overrideService != "" {
		req.Service = uf.overrideService
	}
	return req
}
