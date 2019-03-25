package inbound

import (
	"context"

	"github.com/uber/peloton/pkg/auth"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

var permissionDeniedErrorStr = "not permitted to call %s in %s"

type authInboundMiddleware struct {
	auth.SecurityManager
}

func (m *authInboundMiddleware) Handle(ctx context.Context, req *transport.Request, resw transport.ResponseWriter, h transport.UnaryHandler) error {
	permitted, err := m.isPermitted(req.Headers, req.Procedure)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, req.Procedure, req.Service)
	}

	return h.Handle(ctx, req, resw)
}

func (m *authInboundMiddleware) HandleOneway(ctx context.Context, req *transport.Request, h transport.OnewayHandler) error {
	permitted, err := m.isPermitted(req.Headers, req.Procedure)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, req.Procedure, req.Service)
	}

	return h.HandleOneway(ctx, req)
}

func (m *authInboundMiddleware) HandleStream(s *transport.ServerStream, h transport.StreamHandler) error {
	service := s.Request().Meta.Service
	procedure := s.Request().Meta.Procedure

	permitted, err := m.isPermitted(s.Request().Meta.Headers, procedure)
	if err != nil {
		return err
	}

	if !permitted {
		return yarpcerrors.PermissionDeniedErrorf(permissionDeniedErrorStr, service, procedure)
	}

	return h.HandleStream(s)
}

func (m *authInboundMiddleware) isPermitted(headers transport.Headers, procedure string) (bool, error) {
	user, err := m.Authenticate(headers)
	if err != nil {
		return false, err
	}

	return user.IsPermitted(procedure), nil
}

// NewAuthInboundMiddleware returns DispatcherInboundMiddleWare with auth check
func NewAuthInboundMiddleware(security auth.SecurityManager) DispatcherInboundMiddleWare {
	return &authInboundMiddleware{
		SecurityManager: security,
	}
}
