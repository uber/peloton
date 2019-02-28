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

package updatesvc

import (
	"github.com/uber-go/tally"
)

// Metrics is the struct containing all the counters that track
// internal state of the update service
type Metrics struct {
	UpdateAPICreate  tally.Counter
	UpdateCreate     tally.Counter
	UpdateCreateFail tally.Counter

	UpdateAPIGet  tally.Counter
	UpdateGet     tally.Counter
	UpdateGetFail tally.Counter

	UpdateAPIList  tally.Counter
	UpdateList     tally.Counter
	UpdateListFail tally.Counter

	UpdateAPIGetCache  tally.Counter
	UpdateGetCache     tally.Counter
	UpdateGetCacheFail tally.Counter

	UpdateAPIAbort  tally.Counter
	UpdateAbort     tally.Counter
	UpdateAbortFail tally.Counter

	UpdateAPIPause  tally.Counter
	UpdatePause     tally.Counter
	UpdatePauseFail tally.Counter

	UpdateAPIResume  tally.Counter
	UpdateResume     tally.Counter
	UpdateResumeFail tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics
// initialized and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	UpdateSuccessScope := scope.Tagged(map[string]string{"result": "success"})
	UpdateFailScope := scope.Tagged(map[string]string{"result": "fail"})
	UpdateAPIScope := scope.SubScope("api")

	return &Metrics{
		UpdateAPICreate:  UpdateAPIScope.Counter("create"),
		UpdateCreate:     UpdateSuccessScope.Counter("create"),
		UpdateCreateFail: UpdateFailScope.Counter("create"),

		UpdateAPIGet:  UpdateAPIScope.Counter("get"),
		UpdateGet:     UpdateSuccessScope.Counter("get"),
		UpdateGetFail: UpdateFailScope.Counter("get"),

		UpdateAPIList:  UpdateAPIScope.Counter("list"),
		UpdateList:     UpdateSuccessScope.Counter("list"),
		UpdateListFail: UpdateFailScope.Counter("list"),

		UpdateAPIGetCache:  UpdateAPIScope.Counter("cache_get"),
		UpdateGetCache:     UpdateSuccessScope.Counter("cache_get"),
		UpdateGetCacheFail: UpdateFailScope.Counter("cache_get"),

		UpdateAPIAbort:  UpdateAPIScope.Counter("abort"),
		UpdateAbort:     UpdateSuccessScope.Counter("abort"),
		UpdateAbortFail: UpdateFailScope.Counter("abort"),

		UpdateAPIPause:  UpdateAPIScope.Counter("pause"),
		UpdatePause:     UpdateSuccessScope.Counter("pause"),
		UpdatePauseFail: UpdateFailScope.Counter("pause"),

		UpdateAPIResume:  UpdateAPIScope.Counter("resume"),
		UpdateResume:     UpdateSuccessScope.Counter("resume"),
		UpdateResumeFail: UpdateFailScope.Counter("resume"),
	}
}
