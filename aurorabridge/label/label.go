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

package label

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
)

// Label defines a label.
type Label interface {
	Key() string
	Value() string
}

// rawLabel implements Label interface, but allows setting key and value
// directly>
type rawLabel struct {
	key, value string
}

func (l *rawLabel) Key() string {
	return l.key
}

func (l *rawLabel) Value() string {
	return l.value
}

// Build builds l into a Peloton Label message.
func Build(l Label) *peloton.Label {
	return &peloton.Label{
		Key:   l.Key(),
		Value: l.Value(),
	}
}

// BuildMany builds a list of Labels into a list of Peloton Labels.
func BuildMany(labels []Label) []*peloton.Label {
	var pelotonLabels []*peloton.Label
	for _, label := range labels {
		pelotonLabels = append(pelotonLabels, Build(label))
	}
	return pelotonLabels
}
