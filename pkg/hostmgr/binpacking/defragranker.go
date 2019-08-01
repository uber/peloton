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
	"context"
	"sync"

	"github.com/uber/peloton/pkg/common/sorter"
	"github.com/uber/peloton/pkg/hostmgr/summary"
	"github.com/uber/peloton/pkg/hostmgr/util"
)

// defragRanker is the struct for implementation of
// Defrag Ranker
type defragRanker struct {
	mu          sync.RWMutex
	name        string
	summaryList []interface{}
}

// NewDeFragRanker returns the Defrag Ranker
func NewDeFragRanker() Ranker {
	return &defragRanker{
		name: DeFrag,
	}
}

// Name is the implementation for Ranker interface.Name method
// returns the name
func (d *defragRanker) Name() string {
	return d.name
}

// GetRankedHostList returns the ranked host list.
// For defrag policy we are following sorted ascending order
// 1. GPU
// 2. CPU
// 3. Memory
// 4. Disk
// This checks if there is already a list present pass that
// and it depends on RefreshRanking to refresh the list
func (d *defragRanker) GetRankedHostList(
	ctx context.Context,
	offerIndex map[string]summary.HostSummary) []interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	// if d.summaryList is not initilized , call the getRankedHostList
	if len(d.summaryList) == 0 {
		d.summaryList = d.getRankedHostList(offerIndex)
	}
	return d.summaryList
}

// RefreshRanking refreshes the hostlist based on new host summary index
// This function has to be called periodically to refresh the list
func (d *defragRanker) RefreshRanking(
	ctx context.Context,
	offerIndex map[string]summary.HostSummary) {
	summaryList := d.getRankedHostList(offerIndex)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.summaryList = summaryList
}

// getRankedHostList this is the unprotected method for sorting
// the offer index to all the 4 dimensions
// 1. GPU
// 2. CPU
// 3. Memory
// 4. Disk
func (d *defragRanker) getRankedHostList(
	offerIndex map[string]summary.HostSummary) []interface{} {
	var summaryList []interface{}
	for _, summary := range offerIndex {
		summaryList = append(summaryList, summary)
	}

	gpus := func(c1, c2 interface{}) bool {
		return util.GetResourcesFromOffers(
			c1.(summary.HostSummary).GetOffers(summary.All)).GPU <
			util.GetResourcesFromOffers(
				c2.(summary.HostSummary).GetOffers(summary.All)).GPU
	}
	cpus := func(c1, c2 interface{}) bool {
		return util.GetResourcesFromOffers(
			c1.(summary.HostSummary).GetOffers(summary.All)).CPU <
			util.GetResourcesFromOffers(
				c2.(summary.HostSummary).GetOffers(summary.All)).CPU
	}
	memory := func(c1, c2 interface{}) bool {
		return util.GetResourcesFromOffers(
			c1.(summary.HostSummary).GetOffers(summary.All)).Mem <
			util.GetResourcesFromOffers(
				c2.(summary.HostSummary).GetOffers(summary.All)).Mem
	}
	disk := func(c1, c2 interface{}) bool {
		return util.GetResourcesFromOffers(
			c1.(summary.HostSummary).GetOffers(summary.All)).Disk <
			util.GetResourcesFromOffers(
				c2.(summary.HostSummary).GetOffers(summary.All)).Disk
	}

	sorter.OrderedBy(gpus, cpus, memory, disk).Sort(summaryList)
	return summaryList
}
