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

package impl

import (
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/auth/impl/basic"
	"github.com/uber/peloton/pkg/auth/impl/noop"

	"go.uber.org/yarpc/yarpcerrors"
)

// CreateNewSecurityManager creates SecurityManager based on type
func CreateNewSecurityManager(config *auth.Config) (auth.SecurityManager, error) {
	switch config.AuthType {
	case auth.NOOP, auth.UNDEFINED:
		return noop.NewNoopSecurityManager(), nil
	case auth.BASIC:
		return basic.NewBasicSecurityManager(config.Path)
	default:
		return nil,
			yarpcerrors.InvalidArgumentErrorf("unknown security type provided: %s", config.AuthType)
	}
}

// CreateNewSecurityClient creates SecurityClient based on type
func CreateNewSecurityClient(config *auth.Config) (auth.SecurityClient, error) {
	switch config.AuthType {
	case auth.NOOP, auth.UNDEFINED:
		return noop.NewNoopSecurityClient(), nil
	case auth.BASIC:
		return basic.NewBasicSecurityClient(config.Path)
	default:
		return nil,
			yarpcerrors.InvalidArgumentErrorf("unknown security type provided: %s", config.AuthType)

	}
}
