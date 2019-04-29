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

package common

import (
	"fmt"
	"hash/fnv"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	"go.uber.org/thriftrw/ptr"
)

// ConvertTimestampToUnixMS converts Peloton event timestamp string
// (in seconds) to unix time in milliseconds used by Bridge.
func ConvertTimestampToUnixMS(timestamp string) (*int64, error) {
	u, err := util.ConvertTimestampToUnixSeconds(timestamp)
	if err != nil {
		return nil, fmt.Errorf("unable to parse timestamp %s", err)
	}
	return ptr.Int64(u * 1000), nil
}

// Random defines an interface for generating random string.
type Random interface {
	RandomUUID() string
}

// RandomImpl implements Random interface
type RandomImpl struct{}

// RandomUUID proxies call to uuid.New() function and returns a random UUID
// string
func (r RandomImpl) RandomUUID() string {
	return uuid.New()
}

// RemoveBridgeUpdateLabel removes "bridge update label" from pod spec and
// returns the label value.
func RemoveBridgeUpdateLabel(podSpec *pod.PodSpec) string {
	labels := podSpec.GetLabels()
	newLabels := make([]*peloton.Label, 0, len(labels))
	var updateLabelValue string

	for _, label := range labels {
		if label.GetKey() == BridgeUpdateLabelKey {
			updateLabelValue = label.GetValue()
			continue
		}
		newLabels = append(newLabels, label)
	}

	podSpec.Labels = newLabels
	return updateLabelValue
}

// IsPodSpecWithoutBridgeUpdateLabelChanged check if two PodSpec without
// "bridge update label" are changed.
func IsPodSpecWithoutBridgeUpdateLabelChanged(
	prevSpec *pod.PodSpec,
	newSpec *pod.PodSpec,
) bool {
	// Make copy of pod specs so that they can be call concurrently
	// from multiple go routines.
	prevPodSpec := proto.Clone(prevSpec).(*pod.PodSpec)
	newPodSpec := proto.Clone(newSpec).(*pod.PodSpec)

	prevLabels := prevPodSpec.GetLabels()
	newLabels := newPodSpec.GetLabels()

	defer func() {
		prevPodSpec.Labels = prevLabels
		newPodSpec.Labels = newLabels
	}()

	RemoveBridgeUpdateLabel(prevPodSpec)
	RemoveBridgeUpdateLabel(newPodSpec)

	return taskconfig.HasPodSpecChanged(prevPodSpec, newPodSpec)
}

// Hash creates 32-bit FNV-1a hash for string
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
