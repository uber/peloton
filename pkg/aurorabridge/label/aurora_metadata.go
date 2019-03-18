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
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"go.uber.org/thriftrw/ptr"
)

const (
	// Used for simulating the Aurora's behavior when creating labels in Mesos
	_auroraLabelPrefix = "org.apache.aurora.metadata."
)

// NewAuroraMetadataLabels generates a list of labels using Aurora's
// format for compatibility purposes.
func NewAuroraMetadataLabels(md []*api.Metadata) []*peloton.Label {
	var l []*peloton.Label
	for _, m := range md {
		l = append(l, &peloton.Label{
			Key:   _auroraLabelPrefix + m.GetKey(),
			Value: m.GetValue(),
		})
	}
	return l
}

// ParseAuroraMetadata converts Peloton label to a list of
// Aurora Metadata. The label for aurora metadata must be present, otherwise
// an error will be returned.
func ParseAuroraMetadata(ls []*peloton.Label) []*api.Metadata {
	var m []*api.Metadata
	for _, l := range ls {
		if strings.HasPrefix(l.GetKey(), _auroraLabelPrefix) {
			m = append(m, &api.Metadata{
				Key:   ptr.String(strings.TrimPrefix(l.GetKey(), _auroraLabelPrefix)),
				Value: ptr.String(l.GetValue()),
			})
		}
	}
	return m
}
