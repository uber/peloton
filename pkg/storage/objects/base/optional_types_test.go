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

func (s *OptionalTypeTestSuite) TestNewOptionalUInt64() {
	s.Nil(
		NewOptionalUInt64(""),
	)
	s.Equal(
		&OptionalUInt64{Value: 1},
		NewOptionalUInt64(uint64(1)),
	)
}

func (s *OptionalTypeTestSuite) TestIsOfTypeOptional() {
	s.True(
		IsOfTypeOptional(reflect.ValueOf(&OptionalString{Value: "test"})),
		IsOfTypeOptional(reflect.ValueOf(&OptionalUInt64{Value: uint64(1)})),
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
	s.Equal(
		uint64(1),
		ConvertFromOptionalToRawType(reflect.ValueOf(
			&OptionalUInt64{Value: uint64(1)})),
	)
}

func (s *OptionalTypeTestSuite) TestConvertFromRawToOptionalType() {
	stringReadFromDB := "test"
	convertedFromDB := ConvertFromRawToOptionalType(
		reflect.ValueOf(&stringReadFromDB),
		reflect.TypeOf(&OptionalString{}))
	s.Equal(
		reflect.ValueOf(&OptionalString{Value: "test"}).Interface(),
		convertedFromDB.Interface(),
	)
	//C* internally uses int and int64
	int64ReadFromDB := int64(1)
	convertedFromDB = ConvertFromRawToOptionalType(
		reflect.ValueOf(&int64ReadFromDB),
		reflect.TypeOf(&OptionalUInt64{}))
	s.Equal(
		reflect.ValueOf(&OptionalUInt64{Value: uint64(1)}).Interface(),
		convertedFromDB.Interface())
}
