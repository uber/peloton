package noop

import "github.com/uber/peloton/pkg/auth"

// SecurityManager accepts all auth requests
type SecurityManager struct{}

var _ auth.SecurityManager = &SecurityManager{}

// Authenticate always return success
func (m *SecurityManager) Authenticate(token auth.Token) (auth.User, error) {
	return &noopUser{}, nil
}

type noopUser struct{}

// IsPermitted always return true
func (u *noopUser) IsPermitted(procedure string) bool {
	return true
}

// NewNoopSecurityManager returns SecurityManager
func NewNoopSecurityManager() *SecurityManager {
	return &SecurityManager{}
}
