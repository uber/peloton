// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law orupd agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package base

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

type OptionalTypeTestSuite struct {
	suite.Suite
}

func TestOptionalTypeTestSuite(t *testing.T) {
	suite.Run(t, new(OptionalTypeTestSuite))
}

func (s *OptionalTypeTestSuite) TestNewOptionalString() {
	s.Nil(
		NewOptionalString(1),
	)
	s.Nil(
		NewOptionalString(""),
	)
	s.Equal(
		&OptionalString{Value: "test"},
		NewOptionalString("test"),
	)
}

func (s *OptionalTypeTestSuite) TestIsOfTypeOptional() {
	s.True(
		IsOfTypeOptional(reflect.ValueOf(&OptionalString{Value: "test"})),
	)
	s.False(
		IsOfTypeOptional(reflect.ValueOf("test")),
	)
}

func (s *OptionalTypeTestSuite) TestConvertFromOptionalToRawType() {
	s.Equal(
		"test",
		ConvertFromOptionalToRawType(reflect.ValueOf(&OptionalString{Value: "test"})),
	)
}

func (s *OptionalTypeTestSuite) TestConvertFromRawToOptionalType() {
	stringReadFromDB := "test"
	s.Equal(
		reflect.ValueOf(&OptionalString{Value: "test"}).Interface(),
		ConvertFromRawToOptionalType(reflect.ValueOf(&stringReadFromDB)).Interface(),
	)
}
