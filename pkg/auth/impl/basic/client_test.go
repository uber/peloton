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
	"testing"

	"github.com/stretchr/testify/suite"
)

type SecurityClientTestSuite struct {
	suite.Suite

	c *SecurityClient
}

func (suite *SecurityClientTestSuite) SetupTest() {
	c, err := NewBasicSecurityClient(_testConfigPath)
	suite.NoError(err)
	suite.c = c
}

func (suite *SecurityClientTestSuite) TestBasicSecurityClientGetToken() {
	t := suite.c.GetToken()

	password, ok := t.Get(_passwordHeaderKey)
	suite.True(ok)
	suite.Equal(password, "password2")

	username, ok := t.Get(_usernameHeaderKey)
	suite.True(ok)
	suite.Equal(username, "user2")

	suite.Len(t.Items(), 2)
}

func (suite *SecurityClientTestSuite) TestCreateBasicSecurityClientInvalidUserFailure() {
	config, err := parseConfig(_testConfigPath)
	suite.NoError(err)

	// change user to one which has no root privilege
	config.InternalUser = "user1"

	c, err := newBasicSecurityClient(config)
	suite.Nil(c)
	suite.Error(err)
}

func TestSecurityClientTestSuite(t *testing.T) {
	suite.Run(t, new(SecurityClientTestSuite))
}
