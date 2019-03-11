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

package offerpool

import (
	"github.com/uber-go/tally"

	"github.com/uber/peloton/pkg/common/scalar"
)

// Metrics tracks various metrics at offer pool level.
type Metrics struct {
	// Revocable/Non-Revocable Resources in Ready/Placing status
	Ready            scalar.GaugeMaps
	ReadyRevocable   scalar.GaugeMaps
	Placing          scalar.GaugeMaps
	PlacingRevocable scalar.GaugeMaps

	// metrics for number of hosts on each status.
	ReadyHosts               tally.Gauge
	PlacingHosts             tally.Gauge
	AvailableHosts           tally.Gauge
	ReturnUnusedHosts        tally.Counter
	ResetExpiredPlacingHosts tally.Counter
	ResetExpiredHeldHosts    tally.Counter

	// metrics for offers
	UnavailableOffers tally.Counter
	AcceptableOffers  tally.Counter
	ExpiredOffers     tally.Counter
	RescindEvents     tally.Counter
	Decline           tally.Counter
	DeclineFail       tally.Counter
}

// NewMetrics returns a new Metrics struct, with all metrics initialized
// and rooted at the given tally.Scope
func NewMetrics(scope tally.Scope) *Metrics {
	poolScope := scope.SubScope("pool")

	// resources in ready & placing host status
	readyScope := poolScope.SubScope("ready")
	readyRevocableScope := poolScope.SubScope("ready_revocable")
	placingScope := poolScope.SubScope("placing")
	placingRevocableScope := poolScope.SubScope("placing_revocable")

	hostsScope := poolScope.SubScope("hosts")
	offersScope := poolScope.SubScope("offers")

	return &Metrics{
		Ready:            scalar.NewGaugeMaps(readyScope),
		ReadyRevocable:   scalar.NewGaugeMaps(readyRevocableScope),
		Placing:          scalar.NewGaugeMaps(placingScope),
		PlacingRevocable: scalar.NewGaugeMaps(placingRevocableScope),

		UnavailableOffers: offersScope.Counter("unavilable"),
		AcceptableOffers:  offersScope.Counter("acceptable"),
		RescindEvents:     offersScope.Counter("rescind"),
		ExpiredOffers:     offersScope.Counter("expired"),
		Decline:           offersScope.Counter("decline"),
		DeclineFail:       offersScope.Counter("decline_fail"),

		ReadyHosts:               hostsScope.Gauge("ready"),
		PlacingHosts:             hostsScope.Gauge("placing"),
		AvailableHosts:           hostsScope.Gauge("available"),
		ReturnUnusedHosts:        hostsScope.Counter("return_unused"),
		ResetExpiredPlacingHosts: hostsScope.Counter("reset_expired_placing"),
		ResetExpiredHeldHosts:    hostsScope.Counter("reset_expired_held"),
	}
}
