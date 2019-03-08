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

import (
	"github.com/uber/peloton/pkg/hostmgr/summary"

	log "github.com/sirupsen/logrus"
)

// firstFitRanker is the struct for implementation of
// First_Fit Ranker
type firstFitRanker struct {
	name string
}

// NewFirstFitRanker returns the first fit ranker object
func NewFirstFitRanker() Ranker {
	return &firstFitRanker{name: FirstFit}
}

// Name is implementation of Ranker.Name
func (f *firstFitRanker) Name() string {
	return f.name
}

// GetRankedHostList is implementation of Ranker.GetRankedHostList
// This returns the list ordered by Map.
// FirstFit implementation would be first host which can be fit
func (f *firstFitRanker) GetRankedHostList(offerIndex map[string]summary.HostSummary) []interface{} {
	log.Debugf(" %s ranker GetRankedHostList is been called", f.Name())
	var summaryList []interface{}
	for _, summary := range offerIndex {
		summaryList = append(summaryList, summary)
	}
	return summaryList
}

// RefreshRanking is implementation of Ranker.RefreshRanking
// This is no op for first fitranker
func (f *firstFitRanker) RefreshRanking(offerIndex map[string]summary.HostSummary) {
	return
}
