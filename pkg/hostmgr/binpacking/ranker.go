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

package binpacking

import "github.com/uber/peloton/pkg/hostmgr/summary"

// Ranker is the interface for bin packing strategy for ranking the host
// it returns the list of ordered list of hosts summary. Caller of the
// interface would get the list of ordered host summary and then match the
// host from 0->n to match the constiants with offer.
type Ranker interface {
	// Returns the name of the ranker implementation
	Name() string
	// returns the list of ranked ordered list
	GetRankedHostList(offerIndex map[string]summary.HostSummary) []interface{}
	// Refreshes the ranker based on new host summary index
	// we need to call this asynchronously to mitigate the
	// performance panality of bin packing.
	RefreshRanking(offerIndex map[string]summary.HostSummary)
}
