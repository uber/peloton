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

package manager

import (
	"github.com/uber-go/tally"
)

type Metrics struct {
	// Host pool cache snapshot metrics
	TotalHosts tally.Gauge
	TotalPools tally.Gauge

	// Host pool operating error metrics
	GetPoolErr           tally.Counter
	GetPoolByHostnameErr tally.Counter
	DeregisterPoolErr    tally.Counter
	ChangeHostPoolErr    tally.Counter
	ReconcileErr         tally.Counter
	UpdateCurrentPoolErr tally.Counter
	GetDesirePoolErr     tally.Counter
	UpdateDesiredPoolErr tally.Counter

	// Host pool manager performance metrics
	ReconcileTime tally.Timer
}

func NewMetrics(scope tally.Scope) *Metrics {
	hostPoolManagerScope := scope.SubScope("host_pool_manager")
	hostInfoScope := hostPoolManagerScope.SubScope("host_info")

	return &Metrics{
		TotalHosts:           hostPoolManagerScope.Gauge("total_hosts"),
		TotalPools:           hostPoolManagerScope.Gauge("total_pools"),
		GetPoolErr:           hostPoolManagerScope.Counter("get_pool_error"),
		GetPoolByHostnameErr: hostPoolManagerScope.Counter("get_pool_by_hostname_error"),
		DeregisterPoolErr:    hostPoolManagerScope.Counter("deregister_pool_error"),
		ChangeHostPoolErr:    hostPoolManagerScope.Counter("change_host_pool_error"),
		ReconcileErr:         hostPoolManagerScope.Counter("reconcile_err"),
		UpdateCurrentPoolErr: hostInfoScope.Counter("update_current_pool_err"),
		GetDesirePoolErr:     hostInfoScope.Counter("get_desired_pool_err"),
		UpdateDesiredPoolErr: hostInfoScope.Counter("update_desired_pool_err"),
		ReconcileTime:        hostPoolManagerScope.Timer("reconcile_time"),
	}
}
