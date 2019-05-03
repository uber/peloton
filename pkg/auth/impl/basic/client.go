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

package basic

import (
	"github.com/uber/peloton/pkg/auth"
	"go.uber.org/yarpc/yarpcerrors"
)

// SecurityClient returns token which authenticates internal
// communication when basic auth is enabled
type SecurityClient struct {
	token *basicToken
}

// GetToken returns a token for basic auth
func (c *SecurityClient) GetToken() auth.Token {
	return c.token
}

type basicToken struct {
	items map[string]string
}

func (t *basicToken) Get(k string) (string, bool) {
	result, ok := t.items[k]
	return result, ok
}

func (t *basicToken) Items() map[string]string {
	return t.items
}

func (t *basicToken) Del(k string) {
	delete(t.items, k)
}

// NewBasicSecurityClient returns SecurityClient
func NewBasicSecurityClient(configPath string) (*SecurityClient, error) {
	cConfig, err := parseConfig(configPath)
	if err != nil {
		return nil, err
	}
	return newBasicSecurityClient(cConfig)
}

// helper method to create SecurityClient which makes test easier
func newBasicSecurityClient(cConfig *authConfig) (*SecurityClient, error) {
	if err := validateConfig(cConfig); err != nil {
		return nil, err
	}

	password, err := getPassword(cConfig.InternalUser, cConfig)
	if err != nil {
		return nil, err
	}

	return &SecurityClient{
		token: &basicToken{
			items: map[string]string{
				_usernameHeaderKey: cConfig.InternalUser,
				_passwordHeaderKey: password,
			},
		},
	}, nil
}

func getPassword(username string, config *authConfig) (string, error) {
	for _, uConfig := range config.Users {
		if uConfig.Username == username {
			return uConfig.Password, nil
		}
	}

	return "", yarpcerrors.InvalidArgumentErrorf("no password found for internal user")
}
