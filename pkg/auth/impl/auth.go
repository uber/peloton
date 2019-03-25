package impl

import (
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/auth/impl/basic"
	"github.com/uber/peloton/pkg/auth/impl/noop"

	"go.uber.org/yarpc/yarpcerrors"
)

// CreateNewSecurityManager creates SecurityManager based on type
func CreateNewSecurityManager(t auth.Type, configPath string) (auth.SecurityManager, error) {
	switch t {
	case auth.NOOP, auth.UNDEFINED:
		return noop.NewNoopSecurityManager(), nil
	case auth.BASIC:
		return basic.NewBasicSecurityManager(configPath)
	default:
		return nil,
			yarpcerrors.InvalidArgumentErrorf("unknown security type provided: %s", t)
	}
}
