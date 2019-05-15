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

	leader "github.com/uber/peloton/pkg/common/leader"

	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

// LeaderCheckInboundMiddleware validates inbound request are only served via
// leader
type LeaderCheckInboundMiddleware struct {
	Nomination leader.Nomination
}

// SetNomination sets the nominator for checking the leadership of the node
func (m *LeaderCheckInboundMiddleware) SetNomination(nomination leader.Nomination) {
	m.Nomination = nomination
}

// Handle checks error returned by underlying handler and collect metrics
func (m *LeaderCheckInboundMiddleware) Handle(
	ctx context.Context,
	req *transport.Request,
	resw transport.ResponseWriter,
	h transport.UnaryHandler,
) error {

	if !m.Nomination.HasGainedLeadership() {
		return yarpcerrors.UnavailableErrorf("call to non-leader node")
	}

	return h.Handle(ctx, req, resw)
}

// HandleOneway checks error returned by underlying handler and collect metrics
func (m *LeaderCheckInboundMiddleware) HandleOneway(
	ctx context.Context,
	req *transport.Request,
	h transport.OnewayHandler,
) error {

	if !m.Nomination.HasGainedLeadership() {
		return yarpcerrors.UnavailableErrorf("call to non-leader node")
	}

	return h.HandleOneway(ctx, req)
}

// HandleStream checks error returned by underlying handler and collect metrics
func (m *LeaderCheckInboundMiddleware) HandleStream(
	s *transport.ServerStream,
	h transport.StreamHandler,
) error {

	if !m.Nomination.HasGainedLeadership() {
		return yarpcerrors.UnavailableErrorf("call to non-leader node")
	}

	return h.HandleStream(s)
}
