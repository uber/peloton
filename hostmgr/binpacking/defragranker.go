package binpacking

import (
	"code.uber.internal/infra/peloton/common/sorter"
	"code.uber.internal/infra/peloton/hostmgr/summary"
	"code.uber.internal/infra/peloton/hostmgr/util"

	log "github.com/sirupsen/logrus"
)

// defragRanker is the struct for implementation of
// Defrag Ranker
type defragRanker struct {
	name string
}

// NewDeFragRanker returns the Defrag Ranker
func NewDeFragRanker() Ranker {
	return &defragRanker{name: DeFrag}
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
func (d *defragRanker) GetRankedHostList(
	offerIndex map[string]summary.HostSummary) []interface{} {
	log.Debugf(" %s ranker GetRankedHostList is been called", d.Name())

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
