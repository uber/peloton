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

package inbound

import (
	"context"
	"strings"

	"github.com/uber-go/tally"
	"go.uber.org/net/metrics"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_unknownError          = "unknown"
	_procedureSeparator    = "::"
	_newProcedureSeparator = "__"
)

// YAPRCMetricsInboundMiddleware collects yarpc related
// metrics, such as error code
type YAPRCMetricsInboundMiddleware struct {
	Scope tally.Scope
}

// Handle checks error returned by underlying handler and collect metrics
func (m *YAPRCMetricsInboundMiddleware) Handle(
	ctx context.Context,
	req *transport.Request,
	resw transport.ResponseWriter,
	h transport.UnaryHandler,
) error {
	err := h.Handle(ctx, req, resw)
	if err != nil {
		errorCounter(m.Scope, req.Procedure, err).Inc(1)
	}

	return err
}

// HandleOneway checks error returned by underlying handler and collect metrics
func (m *YAPRCMetricsInboundMiddleware) HandleOneway(
	ctx context.Context,
	req *transport.Request,
	h transport.OnewayHandler,
) error {
	err := h.HandleOneway(ctx, req)
	if err != nil {
		errorCounter(m.Scope, req.Procedure, err).Inc(1)
	}

	return err
}

// HandleStream checks error returned by underlying handler and collect metrics
func (m *YAPRCMetricsInboundMiddleware) HandleStream(
	s *transport.ServerStream,
	h transport.StreamHandler,
) error {
	err := h.HandleStream(s)
	if err != nil {
		errorCounter(m.Scope, s.Request().Meta.Procedure, err).Inc(1)
	}

	return err
}

func errorCode(err error) string {
	if yarpcerrors.IsStatus(err) {
		return yarpcerrors.FromError(err).Code().String()
	}

	return _unknownError
}

func errorCounter(
	scope tally.Scope,
	procedure string,
	err error,
) tally.Counter {
	// need to replace the _procedureSeparator, because it is reserved in m3
	// and tag can get dropped
	procedure = strings.Replace(procedure, _procedureSeparator, _newProcedureSeparator, 1)
	return scope.Tagged(metrics.Tags{
		"procedure": procedure,
		"error":     errorCode(err),
	}).Counter("error")
}
