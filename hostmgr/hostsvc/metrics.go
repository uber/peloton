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

package hostsvc

import "github.com/uber-go/tally"

// Metrics is a placeholder for all metrics in host.svc
type Metrics struct {
	StartMaintenanceAPI     tally.Counter
	StartMaintenanceSuccess tally.Counter
	StartMaintenanceFail    tally.Counter

	CompleteMaintenanceAPI     tally.Counter
	CompleteMaintenanceSuccess tally.Counter
	CompleteMaintenanceFail    tally.Counter

	QueryHostsAPI     tally.Counter
	QueryHostsSuccess tally.Counter
	QueryHostsFail    tally.Counter
}

// NewMetrics returns a new instance of host.svc.Metrics
func NewMetrics(scope tally.Scope) *Metrics {
	apiScope := scope.SubScope("api")
	successScope := scope.Tagged(map[string]string{"result": "success"})
	failScope := scope.Tagged(map[string]string{"result": "fail"})
	return &Metrics{
		StartMaintenanceAPI:     apiScope.Counter("start_maintenance"),
		StartMaintenanceSuccess: successScope.Counter("start_maintenance"),
		StartMaintenanceFail:    failScope.Counter("start_maintenance"),

		CompleteMaintenanceAPI:     apiScope.Counter("complete_maintenance"),
		CompleteMaintenanceSuccess: successScope.Counter("complete_maintenance"),
		CompleteMaintenanceFail:    failScope.Counter("complete_maintenance"),

		QueryHostsAPI:     apiScope.Counter("query_hosts"),
		QueryHostsSuccess: successScope.Counter("query_hosts"),
		QueryHostsFail:    failScope.Counter("query_hosts"),
	}
}
