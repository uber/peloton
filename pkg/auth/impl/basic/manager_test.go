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

const _testConfigPath = "testdata/test_basic_auth_config.yaml"

type SecurityManagerTestSuite struct {
	suite.Suite

	m *SecurityManager
}

func (suite *SecurityManagerTestSuite) SetupTest() {
	m, err := NewBasicSecurityManager(_testConfigPath)
	suite.NoError(err)
	suite.m = m
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerSuccess() {
	role1 := &roleConfig{
		Role:   "admin",
		Accept: []string{_matchAllRule},
	}
	role2 := &roleConfig{
		Role: "default",
	}

	user1 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password1",
	}
	user2 := &userConfig{
		Role: role2.Role,
	}
	user3 := &userConfig{
		Role:     role1.Role,
		Username: "user3",
		Password: "password3",
	}

	config := &authConfig{
		Users:        []*userConfig{user1, user2, user3},
		Roles:        []*roleConfig{role1, role2},
		InternalUser: user1.Username,
	}

	m, err := newBasicSecurityManager(config)
	suite.NotNil(m)
	suite.NoError(err)
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerMultiDefaultUserErr() {
	role1 := &roleConfig{
		Role: "default",
	}

	user1 := &userConfig{
		Role: role1.Role,
	}
	user2 := &userConfig{
		Role: role1.Role,
	}

	config := &authConfig{
		Users: []*userConfig{user1, user2},
		Roles: []*roleConfig{role1},
	}

	m, err := newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerDuplicatedRolesErr() {
	role1 := &roleConfig{
		Role: "admin",
	}
	role2 := &roleConfig{
		Role: "admin",
	}

	user1 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password1",
	}
	user2 := &userConfig{
		Role: role2.Role,
	}

	config := &authConfig{
		Users: []*userConfig{user1, user2},
		Roles: []*roleConfig{role1, role2},
	}

	m, err := newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerDuplicatedUsersErr() {
	role1 := &roleConfig{
		Role: "admin",
	}

	user1 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password1",
	}
	user2 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password2",
	}

	config := &authConfig{
		Users: []*userConfig{user1, user2},
		Roles: []*roleConfig{role1},
	}

	m, err := newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerUndefinedRoleErr() {
	role1 := &roleConfig{
		Role: "admin",
	}

	user1 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password1",
	}
	user2 := &userConfig{
		Role:     "undefined-role",
		Username: "user1",
		Password: "password2",
	}

	config := &authConfig{
		Users: []*userConfig{user1, user2},
		Roles: []*roleConfig{role1},
	}

	m, err := newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestCreateBasicSecurityManagerMissingUserInfoErr() {
	role := &roleConfig{
		Role: "admin",
	}

	// no user name
	user := &userConfig{
		Role:     role.Role,
		Password: "password1",
	}
	config := &authConfig{
		Users: []*userConfig{user},
		Roles: []*roleConfig{role},
	}

	m, err := newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)

	// no password
	user = &userConfig{
		Role:     role.Role,
		Username: "user1",
	}
	config = &authConfig{
		Users: []*userConfig{user},
		Roles: []*roleConfig{role},
	}

	m, err = newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)

	// no role
	user = &userConfig{
		Username: "user1",
		Password: "password1",
	}
	config = &authConfig{
		Users: []*userConfig{user},
		Roles: []*roleConfig{role},
	}

	m, err = newBasicSecurityManager(config)
	suite.Nil(m)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestAuthenticateUser() {
	tests := []struct {
		username  string
		password  string
		expectErr bool
	}{
		{
			username: "user1", password: "password1", expectErr: false,
		},
		{
			username: "user2", password: "password2", expectErr: false,
		},
		{
			expectErr: false,
		},
		{
			username: "user1", password: "wrong password", expectErr: true,
		},
		{
			username: "user2", password: "wrong password", expectErr: true,
		},
		{
			username: "user1", expectErr: true,
		},
		{
			password: "wrong password", expectErr: true,
		},
		{
			username: "non-exist-user", password: "password2", expectErr: true,
		},
	}

	for _, test := range tests {
		u, err := suite.m.Authenticate(
			&testToken{username: test.username, password: test.password},
		)
		if test.expectErr {
			suite.Nil(u)
			suite.Error(err)
		} else {
			suite.NotNil(u)
			suite.NoError(err)
		}
	}
}

func (suite *SecurityManagerTestSuite) TestAuthenticateDefaultUserWhenNonDefinedErr() {
	role1 := &roleConfig{
		Role:   "admin",
		Accept: []string{_matchAllRule},
	}

	user1 := &userConfig{
		Role:     role1.Role,
		Username: "user1",
		Password: "password1",
	}
	user2 := &userConfig{
		Role:     role1.Role,
		Username: "user2",
		Password: "password2",
	}

	config := &authConfig{
		Users:        []*userConfig{user1, user2},
		Roles:        []*roleConfig{role1},
		InternalUser: user1.Username,
	}

	// no default user defined
	m, err := newBasicSecurityManager(config)
	suite.NotNil(m)
	suite.NoError(err)

	u, err := m.Authenticate(&testToken{username: "", password: ""})
	suite.Nil(u)
	suite.Error(err)
}

func (suite *SecurityManagerTestSuite) TestRootUserPermission() {
	tests := []struct {
		procedureName string
	}{
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::GetJob"},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::ListJob"},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::GetPodCache"},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::CreateJob"},
	}

	u, err := suite.m.Authenticate(
		&testToken{username: "user2", password: "password2"},
	)
	suite.NoError(err)

	for _, test := range tests {
		suite.True(u.IsPermitted(test.procedureName))
	}
}

func (suite *SecurityManagerTestSuite) TestNormalUserPermission() {
	tests := []struct {
		procedureName string
		isPermitted   bool
	}{
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::GetJobCache", isPermitted: false},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::GetJob", isPermitted: true},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::ListJob", isPermitted: true},
		{procedureName: "peloton.api.v1alpha.pod.svc.PodService::GetPod", isPermitted: true},
		{procedureName: "peloton.api.v1alpha.respool.svc.ResourcePoolService::CreateResourcePool", isPermitted: true},
		{procedureName: "peloton.api.v1alpha.respool.svc.ResourcePoolService::GetResourcePool", isPermitted: true},
		{procedureName: "peloton.api.v1alpha.job.stateless.svc.JobService::CreateJob", isPermitted: false},
		{procedureName: "peloton.api.v1alpha.job.volume.svc.VolumeService::GetVolume", isPermitted: false},
	}

	u, err := suite.m.Authenticate(
		&testToken{username: "user1", password: "password1"},
	)
	suite.NoError(err)

	for _, test := range tests {
		if test.isPermitted {
			suite.True(u.IsPermitted(test.procedureName), test.procedureName)
		} else {
			suite.False(u.IsPermitted(test.procedureName), test.procedureName)
		}

	}
}

func (suite *SecurityManagerTestSuite) TestValidateRule() {
	tests := []struct {
		rule      string
		expectErr bool
	}{
		{
			rule:      "peloton.api.v1alpha.job.stateless.svc.JobService:Get*",
			expectErr: false,
		},
		{
			rule:      "peloton.api.v1alpha.job.stateless.svc.JobService:*",
			expectErr: false,
		},
		{
			rule:      "peloton.api.v1alpha.job.stateless.svc.JobService:GetJob",
			expectErr: false,
		},
		{
			rule:      "*",
			expectErr: false,
		},
		{
			rule:      "peloton.api.v1alpha.job.stateless.svc.JobService:Get*Job",
			expectErr: true,
		},
		{
			rule:      "peloton*Service:Get*Job",
			expectErr: true,
		},
		{
			rule:      "pelotonService",
			expectErr: true,
		},
		{
			rule:      "pelotonService:GetJob:CreateJob",
			expectErr: true,
		},
	}

	for _, test := range tests {
		if test.expectErr {
			suite.Error(validateRule(test.rule))
		} else {
			suite.NoError(validateRule(test.rule))
		}
	}
}

type testToken struct {
	username string
	password string
}

func (t *testToken) Get(k string) (string, bool) {
	if k == _usernameHeaderKey {
		return t.username, len(t.username) != 0
	}

	if k == _passwordHeaderKey {
		return t.password, len(t.password) != 0
	}

	return "", false
}

func (t *testToken) Items() map[string]string {
	return map[string]string{
		_usernameHeaderKey: t.username,
		_passwordHeaderKey: t.password,
	}
}

func (t *testToken) Del(k string) {
	if k == _usernameHeaderKey {
		t.username = ""
	}

	if k == _passwordHeaderKey {
		t.password = ""
	}
}

func TestSecurityManagerTestSuite(t *testing.T) {
	suite.Run(t, new(SecurityManagerTestSuite))
}
