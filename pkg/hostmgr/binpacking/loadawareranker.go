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
	"time"

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	"github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/summary"

	log "github.com/sirupsen/logrus"
)

const (
	_rpcTimeout = 15 * time.Second
	// _maxTryTimeout is of unit of seconds, 300 seconds
	_maxTryTimeout = float64(300)
)

// loadAwareRanker is the struct for implementation of
// LoadAware Ranker
type loadAwareRanker struct {
	mu             sync.RWMutex
	name           string
	summaryList    []interface{}
	cqosClient     cqos.QoSAdvisorServiceYARPCClient
	cqosLastUpTime time.Time
	cqosMetrics    *metrics.Metrics
}

type hostLoad struct {
	HostName string
	Load     int32
}

// NewLoadAwareRanker returns the LoadAware Ranker
func NewLoadAwareRanker(
	cqosClient cqos.QoSAdvisorServiceYARPCClient,
	cqosMetrics *metrics.Metrics) Ranker {
	return &loadAwareRanker{
		name:        LoadAware,
		cqosClient:  cqosClient,
		cqosMetrics: cqosMetrics,
	}
}

// Name is the implementation for Ranker interface.Name method
// returns the name
func (l *loadAwareRanker) Name() string {
	return l.name
}

// GetRankedHostList returns the ranked host list.
// For loadAware we are following sorted ascending order
// cQoS provides an abstract metric called Load which ranges from 0 to 100.
func (l *loadAwareRanker) GetRankedHostList(
	ctx context.Context,
	offerIndex map[string]summary.HostSummary) []interface{} {
	l.mu.Lock()
	defer l.mu.Unlock()

	// if d.summaryList is not initialized , call the getRankedHostList
	if len(l.summaryList) == 0 {
		l.summaryList = l.getRankedHostList(ctx, offerIndex)
	}
	return l.summaryList
}

// RefreshRanking refreshes the host list based on Load information
// This function has to be called periodically to refresh the list
func (l *loadAwareRanker) RefreshRanking(
	ctx context.Context,
	offerIndex map[string]summary.HostSummary,
) {
	l.mu.Lock()
	defer l.mu.Unlock()

	summaryList := l.getRankedHostList(ctx, offerIndex)
	l.summaryList = summaryList
}

// getRankedHostList sorts the offer index to one criteria, Load from CQos
// a int32 type
func (l *loadAwareRanker) getRankedHostList(
	ctx context.Context,
	offerIndex map[string]summary.HostSummary,
) []interface{} {
	var summaryList []interface{}
	offerIndexCopy := make(map[string]summary.HostSummary, len(offerIndex))
	for key, value := range offerIndex {
		offerIndexCopy[key] = value
	}
	//get the host Load map from cQos
	//loop through the hosts summary map
	//and sort the host summary map according to the host Load map
	loadMap, err := l.pollFromCQos(ctx)
	if err != nil {
		l.cqosMetrics.GetCqosAdvisorMetricFail.Inc(1)
		log.WithFields(log.Fields{
			"cqosLastUpTime": l.cqosLastUpTime,
			"downDuration":   time.Since(l.cqosLastUpTime).Seconds(),
		}).Debug("Cqos advisor is down")
		if time.Since(l.cqosLastUpTime).Seconds() >= _maxTryTimeout {
			// Cqos advisor is not reachable after 5 mins
			// expire the cache list, we fall back to first_fit ranker
			return l.getRandomHostList(offerIndex)
		}
		// using cache summaryList
		return l.summaryList
	}
	l.cqosMetrics.GetCqosAdvisorMetric.Inc(1)

	// loadHostMap key is the Load, value is an array of hosts of this Load
	loadHostMap := l.bucketSortByLoad(loadMap)
	var hostLoadOrderedList []hostLoad
	// loop through the hosts of same Load
	cqosLoadMin := int32(0)
	cqosLoadMax := int32(100)
	for i := cqosLoadMin; i <= cqosLoadMax; i++ {
		if _, ok := loadHostMap[i]; !ok {
			continue
		}
		for _, perHostLoad := range loadHostMap[i] {
			// ignore hosts returned in cqos while not in offerindex
			if hsummary, ok := offerIndex[perHostLoad.HostName]; ok {
				summaryList = append(summaryList, hsummary)
				delete(offerIndexCopy, perHostLoad.HostName)
				hostLoadOrderedList = append(hostLoadOrderedList, perHostLoad)
			}
		}
	}

	// this means some hosts are in mesos offers while not in cqos response.
	// We will temporary treat those hosts with Load of 100
	if len(offerIndexCopy) != 0 {
		for host := range offerIndexCopy {
			summaryList = append(summaryList, offerIndexCopy[host])
		}
	}

	log.WithFields(log.Fields{
		"hostLoadOrderedList": hostLoadOrderedList,
	}).Debug("Host load ordered List after sorting by load ranker")

	return summaryList
}

// bucketSortByLoad bucket sorting the Load
// the Load will be [0..100]
// map looks like 0 => {host1, host2}
//                1 => {host3}...
func (l *loadAwareRanker) bucketSortByLoad(
	loadMap *cqos.GetHostMetricsResponse) map[int32][]hostLoad {
	// loadHostMap records Load to hosts of the same Load
	loadHostMap := make(map[int32][]hostLoad)

	for hostName, load := range loadMap.GetHosts() {
		loadHostMap[load.Score] = append(loadHostMap[load.Score],
			hostLoad{hostName, load.Score})
	}
	return loadHostMap
}

func (l *loadAwareRanker) pollFromCQos(ctx context.Context) (*cqos.
	GetHostMetricsResponse, error) {
	req := &cqos.GetHostMetricsRequest{}
	ctx, cancelFunc := context.WithTimeout(
		ctx,
		_rpcTimeout,
	)
	defer cancelFunc()
	result, err := l.cqosClient.GetHostMetrics(
		ctx, req)
	if err != nil {
		// when cqos advisor is unreachable, we will keep using sortedlist from
		// cache. We expire the cache and fall back to firstFit ranker after
		// cqos advisor has been down after _maxTryTimeout
		log.WithError(err).
			Warn("Failed to reach CQos.")
		return nil, err
	}
	l.cqosLastUpTime = time.Now()
	return result, nil
}

// return a random host summarylist
func (l *loadAwareRanker) getRandomHostList(
	offerIndex map[string]summary.HostSummary) []interface{} {
	var summaryList []interface{}
	for _, summary := range offerIndex {
		summaryList = append(summaryList, summary)
	}
	return summaryList
}
