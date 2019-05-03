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

package noop

import "github.com/uber/peloton/pkg/auth"

// SecurityManager accepts all auth requests
type SecurityManager struct{}

var _ auth.SecurityManager = &SecurityManager{}

// Authenticate always return success
func (m *SecurityManager) Authenticate(token auth.Token) (auth.User, error) {
	return &noopUser{}, nil
}

// RedactToken is noop
func (m *SecurityManager) RedactToken(token auth.Token) {
	return
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

// SecurityClient returns token which returns nil for
// every call
type SecurityClient struct{}

// GetToken returns a token for noop auth
func (c *SecurityClient) GetToken() auth.Token {
	return &noopToken{}
}

type noopToken struct{}

func (t *noopToken) Get(k string) (string, bool) {
	return "", false
}

func (t *noopToken) Items() map[string]string {
	return nil
}

func (t *noopToken) Del(k string) {
	return
}

// NewNoopSecurityClient returns SecurityClient
func NewNoopSecurityClient() *SecurityClient {
	return &SecurityClient{}
}
