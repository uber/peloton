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
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"

	"github.com/uber/peloton/pkg/common"
)

// LabelValues tracks how many times a value presents for a given label key.
// First level key is label key, second level key is label value.
// This is the subject of constraint evaluation process.
type LabelValues map[string]map[string]uint32

// GetHostLabelValues returns label counts for a host and its labels,
// which can be used to evaluate a constraint.
// NOTE: `hostname` is added unconditionally, to make sure hostname based
// constraints can be done regardless of label configuration.
func GetHostLabelValues(
	hostname string,
	labels []*peloton.Label,
) LabelValues {
	result := LabelValues{
		common.HostNameKey: map[string]uint32{hostname: 1},
	}

	for _, label := range labels {
		lk := label.GetKey()
		lv := label.GetValue()
		if v, ok := result[lk]; ok {
			v[lv] = v[lv] + 1
		} else {
			result[lk] = map[string]uint32{lv: 1}
		}
	}
	return result
}

func HasExclusiveLabel(labels []*peloton.Label) bool {
	for _, label := range labels {
		if label.GetKey() == common.PelotonExclusiveNodeLabel {
			return true
		}
	}
	return false
}
