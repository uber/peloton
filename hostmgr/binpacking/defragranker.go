package binpacking

import (
	"sync"

	"github.com/uber/peloton/common/sorter"
	"github.com/uber/peloton/hostmgr/summary"
	"github.com/uber/peloton/hostmgr/util"

	log "github.com/sirupsen/logrus"
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
	offerIndex map[string]summary.HostSummary) []interface{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Debugf(" %s ranker GetRankedHostList is been called", d.Name())
	// if d.summaryList is not initilized , call the getRankedHostList
	if len(d.summaryList) == 0 {
		d.summaryList = d.getRankedHostList(offerIndex)
	}
	return d.summaryList
}

// RefreshRanking refreshes the hostlist based on new host summary index
// This function has to be called periodically to refresh the list
func (d *defragRanker) RefreshRanking(offerIndex map[string]summary.HostSummary) {
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
