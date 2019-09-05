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
	"strconv"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/common"

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

// IsGpuConfig returns true if the uDeploy specific label used to
// request GPU resources is present in the metadata
func IsGpuConfig(md []*api.Metadata, rs []*api.Resource) bool {
	for _, m := range md {
		if m.GetKey() == common.AuroraGpuResourceKey {
			return true
		}
	}

	for _, r := range rs {
		if r.IsSetNumGpus() {
			return true
		}
	}
	return false
}

// GetUdeployGpuLimit extracts uDeploy specific label used for requesting GPU
// resources, and returns it as float pointer. Nil pointer is returned if the
// label does not exist.
func GetUdeployGpuLimit(md []*api.Metadata) (*float64, error) {
	for _, m := range md {
		if m.GetKey() == common.AuroraGpuResourceKey {
			numGpus, err := strconv.ParseFloat(m.GetValue(), 64)
			if err != nil {
				return nil, err
			}
			return ptr.Float64(numGpus), nil
		}
	}
	return nil, nil
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
