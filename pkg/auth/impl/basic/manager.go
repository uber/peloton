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
	"bytes"
	"crypto/sha256"
	"io"
	"strings"

	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common/config"

	"go.uber.org/yarpc/yarpcerrors"
)

const (
	_ruleSeparator = ":"
	// rule that matches all methods under all services
	_matchAllRule       = "*"
	_procedureSeparator = "::"

	// expected fields passed by token
	_usernameHeaderKey = "username"
	_passwordHeaderKey = "password"
)

// SecurityManager uses Username and Password for auth
type SecurityManager struct {
	defaultUser *user
	users       map[string]*user
}

// all fields are immutable after init,
// need lock protection if the assumption breaks
type user struct {
	username string
	role     *role
	// store the Password in hashed way,
	// so it is not exposed by mem dump.
	hashedPassword []byte
}

// all fields are immutable after init,
// need lock protection if the assumption breaks
type role struct {
	role string
	// service -> methods
	accepts map[string][]string
	// service -> methods
	rejects map[string][]string
}

var _ auth.SecurityManager = &SecurityManager{}

// Authenticate authenticates a user,
// it expects to Accept UsernamePasswordToken
func (m *SecurityManager) Authenticate(token auth.Token) (auth.User, error) {
	authErr := yarpcerrors.UnauthenticatedErrorf("invalid Username/Password combination")

	username, _ := token.Get(_usernameHeaderKey)
	password, _ := token.Get(_passwordHeaderKey)

	// no Username & Password provided, return default user
	if len(username) == 0 && len(password) == 0 {
		if m.defaultUser == nil {
			return nil, authErr
		}
		return m.defaultUser, nil
	}

	// invalid token format, expect both Username and Password to be provided
	if len(username) == 0 || len(password) == 0 {
		return nil, authErr
	}

	user, ok := m.users[username]
	if !ok {
		return nil, authErr
	}

	if !compareHashedPassword(user.hashedPassword, password) {
		return nil, authErr
	}

	return user, nil
}

// RedactToken removes password info from the token
func (m *SecurityManager) RedactToken(token auth.Token) {
	token.Del(_passwordHeaderKey)
}

// IsPermitted returns if a procedure is permitted for user
func (u *user) IsPermitted(procedure string) bool {
	// procedure is permitted if it is accepted by
	// the role and is not rejected
	results := strings.Split(procedure, _procedureSeparator)
	service := results[0]
	method := results[1]

	if matchRules(service, method, u.role.accepts) &&
		!matchRules(service, method, u.role.rejects) {
		return true
	}

	return false
}

func matchRules(service, method string, rules map[string][]string) bool {
	// _matchAllRule is set, all services and methods are matched
	if _, ok := rules[_matchAllRule]; ok {
		return true
	}

	ruleSlice, ok := rules[service]
	if !ok {
		return false
	}

	for _, r := range ruleSlice {
		if matchRule(method, r) {
			return true
		}
	}
	return false
}

func matchRule(method string, rule string) bool {
	if len(rule) == 0 {
		return false
	}

	if rule == _matchAllRule {
		return true
	}

	if rule[len(rule)-1:] == _matchAllRule {
		return strings.HasPrefix(method, rule[0:len(rule)-1])
	}

	return rule == method
}

// NewBasicSecurityManager returns SecurityManager
func NewBasicSecurityManager(configPath string) (*SecurityManager, error) {
	mConfig, err := parseConfig(configPath)
	if err != nil {
		return nil, err
	}

	return newBasicSecurityManager(mConfig)
}

// helper method to create SecurityManager which makes test easier
func newBasicSecurityManager(mConfig *authConfig) (*SecurityManager, error) {
	if err := validateConfig(mConfig); err != nil {
		return nil, err
	}

	defaultUser, users, err := constructUsers(mConfig, constructRoles(mConfig))
	if err != nil {
		return nil, err
	}

	return &SecurityManager{
		defaultUser: defaultUser,
		users:       users,
	}, nil
}

func parseConfig(configPath string) (*authConfig, error) {
	mConfig := &authConfig{}
	if err := config.Parse(mConfig, configPath); err != nil {
		return nil, err
	}
	return mConfig, nil
}

func validateConfig(config *authConfig) error {
	roleConfigs := make(map[string]*roleConfig)
	// check if rules are valid
	for _, roleConfig := range config.Roles {
		for _, acceptRule := range roleConfig.Accept {
			if err := validateRule(acceptRule); err != nil {
				return err
			}
		}
		for _, rejectRule := range roleConfig.Reject {
			if err := validateRule(rejectRule); err != nil {
				return err
			}
		}

		if _, ok := roleConfigs[roleConfig.Role]; ok {
			return yarpcerrors.InvalidArgumentErrorf(
				"same Role defined more than once. Role:%s",
				roleConfig.Role,
			)
		}
		roleConfigs[roleConfig.Role] = roleConfig
	}

	var defaultUserCount int
	userConfigs := make(map[string]*userConfig)
	for _, userConfig := range config.Users {
		if len(userConfig.Role) == 0 {
			return yarpcerrors.InvalidArgumentErrorf("no Role specified for user")
		}

		// check if user has a Role that is defined in Roles
		if _, ok := roleConfigs[userConfig.Role]; !ok {
			return yarpcerrors.InvalidArgumentErrorf(
				"user: %s has undefined Role: %s",
				userConfig.Username,
				userConfig.Role,
			)
		}

		if len(userConfig.Password) != 0 && len(userConfig.Username) == 0 {
			return yarpcerrors.InvalidArgumentErrorf("no Username specified for user")
		}

		if len(userConfig.Password) == 0 && len(userConfig.Username) != 0 {
			return yarpcerrors.InvalidArgumentErrorf("no Password specified for user")
		}

		if len(userConfig.Password) == 0 && len(userConfig.Username) == 0 {
			defaultUserCount++
		}

		// only one user can be default user
		if defaultUserCount > 1 {
			return yarpcerrors.InvalidArgumentErrorf("more than one default user specified")
		}

		if _, ok := userConfigs[userConfig.Username]; ok {
			return yarpcerrors.InvalidArgumentErrorf(
				"same user defined more than once. user:%s",
				userConfig.Username,
			)
		}
		userConfigs[userConfig.Username] = userConfig
	}

	// validate internal user configs
	internalUserConfig, ok := userConfigs[config.InternalUser]
	if !ok {
		return yarpcerrors.InvalidArgumentErrorf("undefined internal user")
	}

	internalUserRoleConfig, ok := roleConfigs[internalUserConfig.Role]
	if !ok {
		return yarpcerrors.InvalidArgumentErrorf("undefined role for internal user")
	}

	if !isRootRole(internalUserRoleConfig) {
		return yarpcerrors.InvalidArgumentErrorf(
			"role for internal user must accept * and reject no method")
	}

	return nil
}

func isRootRole(config *roleConfig) bool {
	if !(len(config.Accept) == 1 && config.Accept[0] == _matchAllRule) {
		return false
	}

	if len(config.Reject) != 0 {
		return false
	}

	return true
}

// check if the rule is valid,
func validateRule(rule string) error {
	error := yarpcerrors.InvalidArgumentErrorf(
		"rule: %s has unexpected format",
		rule,
	)

	if rule == _matchAllRule {
		return nil
	}

	results := strings.Split(rule, _ruleSeparator)
	if len(results) != 2 {
		return error
	}

	if len(results[0]) == 0 || len(results[1]) == 0 {
		return error
	}

	if !isValidServiceName(results[0]) || !isValidMethod(results[1]) {
		return error
	}

	return nil
}

func isValidServiceName(service string) bool {
	// service name in a rule can have only [a-zA-Z0-9] and '.'
	for _, r := range service {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '.' {
			continue
		}
		return false
	}

	return true
}

func isValidMethod(method string) bool {
	// method part in a rule can be '*', method_name_prefix + '*' or method name
	if method == _matchAllRule {
		return true
	}

	c := strings.Count(method, _matchAllRule)
	if c > 1 {
		return false
	} else if c == 1 {
		return method[len(method)-1:] == _matchAllRule
	}

	for _, r := range method {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			continue
		}
		return false
	}
	return true
}

func constructRoles(mConfig *authConfig) map[string]*role {
	result := make(map[string]*role)
	for _, roleConfig := range mConfig.Roles {
		accepts := make(map[string][]string)
		rejects := make(map[string][]string)

		for _, accept := range roleConfig.Accept {
			if accept == _matchAllRule {
				accepts[_matchAllRule] = []string{_matchAllRule}
				continue
			}
			results := strings.Split(accept, _ruleSeparator)
			accepts[results[0]] = append(accepts[results[0]], results[1])
		}

		for _, reject := range roleConfig.Reject {
			if reject == _matchAllRule {
				accepts[_matchAllRule] = []string{_matchAllRule}
				continue
			}
			results := strings.Split(reject, _ruleSeparator)
			rejects[results[0]] = append(rejects[results[0]], results[1])

		}

		result[roleConfig.Role] = &role{
			role:    roleConfig.Role,
			accepts: accepts,
			rejects: rejects,
		}
	}

	return result
}

func constructUsers(mConfig *authConfig, roles map[string]*role) (
	defaultUser *user,
	users map[string]*user,
	err error,
) {
	users = make(map[string]*user)
	for _, userConfig := range mConfig.Users {
		role, ok := roles[userConfig.Role]
		if !ok {
			// safety check,
			// should not reach this branch after validation
			return nil, nil, yarpcerrors.InvalidArgumentErrorf(
				"cannot find Role:%s for user:%s",
				userConfig.Role,
				userConfig.Username,
			)
		}

		if len(userConfig.Username) == 0 && len(userConfig.Password) == 0 {
			if defaultUser != nil {
				// safety check,
				// should not reach this branch after validation
				return nil, nil,
					yarpcerrors.InvalidArgumentErrorf("more than one default user specified")
			}
			defaultUser = &user{
				role: role,
			}
		}

		users[userConfig.Username] = &user{
			username:       userConfig.Username,
			role:           role,
			hashedPassword: generateHashByte(userConfig.Password),
		}
	}

	return defaultUser, users, nil
}

func generateHashByte(password string) []byte {
	h := sha256.New()
	io.WriteString(h, password)
	return h.Sum(nil)
}

func compareHashedPassword(hashedPassword []byte, password string) bool {
	return bytes.Equal(hashedPassword, generateHashByte(password))
}
