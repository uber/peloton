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
	"strconv"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/pkg/common"

	log "github.com/sirupsen/logrus"
)

const (
	_precision = 6
	_bitsize   = 64
)

// LabelValues tracks how many times a value presents for a given label key.
// First level key is label key, second level key is label value.
// This is the subject of constraint evaluation process.
type LabelValues map[string]map[string]uint32

// Merge merges additional label values into current label values.
// Values of the same label will be merged as well.
func (lv LabelValues) Merge(additionalLV LabelValues) {
	for label, origin := range additionalLV {
		if origin == nil {
			continue
		}
		copy := make(map[string]uint32)
		for value, count := range origin {
			copy[value] = count
		}
		if _, ok := lv[label]; !ok {
			lv[label] = copy
			continue
		}
		for v, c := range copy {
			if current, ok := lv[label][v]; !ok {
				lv[label][v] = c
			} else {
				lv[label][v] = current + c
			}
		}
	}
}

// GetHostLabelValues returns label counts for a host and its attributes,
// which can be used to evaluate a constraint.
// NOTE: `hostname` is added unconditionally, to make sure hostname based
// constraints can be done regardless of attribute configuration.
func GetHostLabelValues(
	hostname string,
	attributes []*mesos.Attribute) LabelValues {

	result := make(map[string]map[string]uint32)
	result[common.HostNameKey] = map[string]uint32{hostname: 1}
OUTER:
	for _, attr := range attributes {
		key := attr.GetName()
		values := []string{}
		switch attr.GetType() {
		case mesos.Value_TEXT:
			values = append(values, attr.GetText().GetValue())
		case mesos.Value_SCALAR:
			value := strconv.FormatFloat(
				attr.GetScalar().GetValue(),
				'f',
				_precision,
				_bitsize)
			values = append(values, value)
		case mesos.Value_SET:
			for _, value := range attr.GetSet().GetItem() {
				values = append(values, value)
			}
		default:
			// TODO: Add support for range attributes.
			log.WithFields(log.Fields{
				"key":  key,
				"type": attr.GetType(),
			}).Warn("Attribute type is not supported yet")
			continue OUTER
		}
		if _, ok := result[key]; !ok {
			result[key] = make(map[string]uint32)
		}
		for _, value := range values {
			result[key][value] = result[key][value] + 1
		}
	}
	return result
}
