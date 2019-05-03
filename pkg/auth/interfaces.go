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

package auth

// Type is the auth type used
type Type string

const (
	// UNDEFINED is the undefined type which would behave the same as NOOP
	UNDEFINED = Type("")
	// NOOP would effectively disable security feature
	NOOP = Type("NOOP")
	// BASIC would use username and password for auth
	BASIC = Type("BASIC")
)

// Token is used by SecurityManager to authenticate a user
type Token interface {
	Get(k string) (string, bool)
	Del(k string)
	// Items returns all of the items in the token.
	// The returned map should not be modified, it is
	// the caller's responsibility to copy before write.
	Items() map[string]string
}

// SecurityManager includes authentication related methods
type SecurityManager interface {
	// Authenticate with the token provided,
	// return User if authentication succeeds
	Authenticate(token Token) (User, error)
	// RedactToken redact token passed in by
	// removing sensitive info, so the info would
	// not be leaked into other components and get
	// leaked/logged accidentally.
	// It would be called after auth is done, before
	// passing the token to other components
	RedactToken(token Token)
}

// User includes authorization related methods
type User interface {
	// IsPermitted returns whether user can
	// access the specified procedure
	IsPermitted(procedure string) bool
}

// SecurityClient is the internal client used by each of
// the peloton components to talk to each other.
// For each SecurityManager there should be a corresponding
// SecurityClient, which helps components communication.
type SecurityClient interface {
	// GetToken returns the token needed by internal component
	// for communication
	GetToken() Token
}
