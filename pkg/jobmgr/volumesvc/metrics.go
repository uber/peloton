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

package volumesvc

import (
	"github.com/uber-go/tally"
)

// Metrics is a placeholder for all metrics in volume manager.
type Metrics struct {
	GetVolumeAPI  tally.Counter
	GetVolume     tally.Counter
	GetVolumeFail tally.Counter

	ListVolumeAPI  tally.Counter
	ListVolume     tally.Counter
	ListVolumeFail tally.Counter

	DeleteVolumeAPI  tally.Counter
	DeleteVolume     tally.Counter
	DeleteVolumeFail tally.Counter
}

// NewMetrics returns a new instance of volumesvc.Metrics.
func NewMetrics(scope tally.Scope) *Metrics {
	subScope := scope.SubScope("volume")
	return &Metrics{
		GetVolume:     subScope.Counter("get"),
		GetVolumeAPI:  subScope.Counter("get_api"),
		GetVolumeFail: subScope.Counter("get_fail"),

		ListVolume:     subScope.Counter("list"),
		ListVolumeAPI:  subScope.Counter("list_api"),
		ListVolumeFail: subScope.Counter("list_fail"),

		DeleteVolume:     subScope.Counter("delete"),
		DeleteVolumeAPI:  subScope.Counter("delete_api"),
		DeleteVolumeFail: subScope.Counter("delete_fail"),
	}
}
