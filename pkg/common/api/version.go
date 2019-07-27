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

package api

import "go.uber.org/yarpc/yarpcerrors"

// ErrInvalidVersion is returned when user configures an invalid api version.
var ErrInvalidVersion = yarpcerrors.InvalidArgumentErrorf("invalid version")

// Version is the type of API versions.
type Version string

// These constants are the string representations of the different
// API versions.
const (
	V0      = Version("v0")
	V1Alpha = Version("v1alpha")
	V1      = Version("v1")
)

// IsV1 returns whether the API version is for v1.
func (v Version) IsV1() bool {
	return v == V1 || v == V1Alpha
}

// String returns the string representation of the version.
func (v Version) String() string {
	return string(v)
}

// ParseVersion parses version from string.
func ParseVersion(str string) (Version, error) {
	switch Version(str) {
	case V0:
		return V0, nil
	case V1:
		return V1, nil
	case V1Alpha:
		return V1Alpha, nil
	default:
		return "", ErrInvalidVersion
	}
}
