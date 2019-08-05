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

	"github.com/uber/peloton/pkg/auth"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"

	log "github.com/sirupsen/logrus"
)

var permissionDeniedErrorStr = "not permitted to call %s in %s"

const (
	// _pelotonServicePrefix is the prefix which all peloton
	// services have
	_pelotonServicePrefix = "peloton"
)

// AuthInboundMiddleware is the inbound middleware for auth
type AuthInboundMiddleware struct {
	auth.SecurityManager
}

// Handle authenticates user and invokes the underlying handler
func (m *AuthInboundMiddleware) Handle(ctx context.Context, req *transport.Request, resw transport.ResponseWriter, h transport.UnaryHandler) error {
	permitted, err := m.isPermitted(req.Headers, req.Service, req.Procedure, req.Caller)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, req.Procedure, req.Service)
	}

	return h.Handle(ctx, req, resw)
}

// HandleOneway authenticates user and invokes the underlying handler
func (m *AuthInboundMiddleware) HandleOneway(ctx context.Context, req *transport.Request, h transport.OnewayHandler) error {
	permitted, err := m.isPermitted(req.Headers, req.Service, req.Procedure, req.Caller)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, req.Procedure, req.Service)
	}

	return h.HandleOneway(ctx, req)
}

// HandleStream authenticates user and invokes the underlying handler
func (m *AuthInboundMiddleware) HandleStream(s *transport.ServerStream, h transport.StreamHandler) error {
	service := s.Request().Meta.Service
	procedure := s.Request().Meta.Procedure

	permitted, err := m.isPermitted(s.Request().Meta.Headers, service, procedure, s.Request().Meta.Caller)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, service, procedure)
	}

	return h.HandleStream(s)
}

func (m *AuthInboundMiddleware) isPermitted(
	headers transport.Headers,
	service string,
	procedure string,
	caller string) (permitted bool, err error) {
	// check the service name and authenticate only peloton services.
	// Other services such as Mesos callback (service name: Scheduler)
	// cannot be authenticated by peloton auth mechanism for now.
	if !strings.HasPrefix(service, _pelotonServicePrefix) {
		return true, nil
	}

	user, err := m.Authenticate(headers)
	if err != nil {
		return false, err
	}

	m.RedactToken(headers)

	permitted, err = user.IsPermitted(procedure), nil
	if !permitted {
		log.WithFields(log.Fields{
			"procedure": procedure,
			"headers":   headers,
			"caller":    caller,
		}).Info("procedure called not permitted for user")
	}

	return permitted, err
}

// NewAuthInboundMiddleware returns AuthInboundMiddleware with auth check
func NewAuthInboundMiddleware(security auth.SecurityManager) *AuthInboundMiddleware {
	return &AuthInboundMiddleware{
		SecurityManager: security,
	}
}
