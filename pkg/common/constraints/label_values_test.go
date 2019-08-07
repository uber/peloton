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

package constraints

import (
	"testing"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/common"

	"github.com/stretchr/testify/suite"
)

type LabelValuesTestSuite struct {
	suite.Suite
}

func (suite *LabelValuesTestSuite) TestMerge() {
	originLV := LabelValues{
		"label1": {},
		"label2": map[string]uint32{
			"value1": 1,
		},
		"label3": map[string]uint32{
			"value1": 1,
			"value2": 1,
		},
	}

	additionalLV := LabelValues{
		"label1": map[string]uint32{
			"value1": 1,
		},
		"label2": {},
		"label3": map[string]uint32{
			"value1": 1,
		},
		"label4": map[string]uint32{
			"value1": 1,
			"value2": 1,
		},
	}

	expectedResult := LabelValues{
		"label1": map[string]uint32{
			"value1": 1,
		},
		"label2": map[string]uint32{
			"value1": 1,
		},
		"label3": map[string]uint32{
			"value1": 2,
			"value2": 1,
		},
		"label4": map[string]uint32{
			"value1": 1,
			"value2": 1,
		},
	}

	originLV.Merge(additionalLV)
	suite.EqualValues(expectedResult, originLV)
}

func (suite *LabelValuesTestSuite) TestGetHostLabelValues() {
	// An attribute of range type.
	rangeName := "range"
	rangeType := mesos.Value_RANGES
	rangeBegin := uint64(100)
	rangeEnd := uint64(200)
	ranges := mesos.Value_Ranges{
		Range: []*mesos.Value_Range{
			{
				Begin: &rangeBegin,
				End:   &rangeEnd,
			},
		},
	}
	rangeAttr := mesos.Attribute{
		Name:   &rangeName,
		Type:   &rangeType,
		Ranges: &ranges,
	}

	// An attribute of set type.
	setName := "set"
	setType := mesos.Value_SET
	setValues := mesos.Value_Set{
		Item: []string{"set_value1", "set_value2"},
	}
	setAttr := mesos.Attribute{
		Name: &setName,
		Type: &setType,
		Set:  &setValues,
	}

	// An attribute of scalar type.
	scalarName := "scalar"
	scalarType := mesos.Value_SCALAR
	sv := float64(1.0)
	scalarValue := mesos.Value_Scalar{
		Value: &sv,
	}
	scalarAttr := mesos.Attribute{
		Name:   &scalarName,
		Type:   &scalarType,
		Scalar: &scalarValue,
	}

	// An attribute of text type.
	textName := "text"
	textType := mesos.Value_TEXT
	tv := "text-value"
	textValue := mesos.Value_Text{
		Value: &tv,
	}
	textAttr := mesos.Attribute{
		Name: &textName,
		Type: &textType,
		Text: &textValue,
	}

	attributes := []*mesos.Attribute{
		&rangeAttr,
		&setAttr,
		&scalarAttr,
		&textAttr,
	}

	hostname := "test-host"

	res := GetHostLabelValues(hostname, attributes)
	// hostname, text, set and scalar.
	suite.Equal(4, len(res), "result: ", res)
	suite.Equal(map[string]uint32{hostname: 1}, res[common.HostNameKey])
	suite.Equal(
		map[string]uint32{
			"set_value1": 1,
			"set_value2": 1,
		},
		res[setName])
	suite.Equal(map[string]uint32{tv: 1}, res[textName])
	suite.Equal(map[string]uint32{"1.000000": 1}, res[scalarName])
}

func TestLabelValuesTestSuite(t *testing.T) {
	suite.Run(t, new(LabelValuesTestSuite))
}
