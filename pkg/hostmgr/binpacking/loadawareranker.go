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

	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	"github.com/uber/peloton/pkg/hostmgr/summary"

	log "github.com/sirupsen/logrus"
)

// loadAwareRanker is the struct for implementation of
// LoadAware Ranker
type loadAwareRanker struct {
	mu          sync.RWMutex
	name        string
	summaryList []interface{}
	cqosClient  cqos.QoSAdvisorServiceYARPCClient
}

type hostLoad struct {
	hostName string
	load     int32
}

// NewLoadAwareRanker returns the LoadAware Ranker
func NewLoadAwareRanker(cqosClient cqos.QoSAdvisorServiceYARPCClient) Ranker {
	return &loadAwareRanker{
		name:       LoadAware,
		cqosClient: cqosClient,
	}
}

// Name is the implementation for Ranker interface.Name method
// returns the name
func (l *loadAwareRanker) Name() string {
	return l.name
}

// GetRankedHostList returns the ranked host list.
// For loadAware we are following sorted ascending order
// cQoS provides an abstract metric called load which ranges from 0 to 100.
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

// RefreshRanking refreshes the host list based on load information
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

// getRankedHostList sorts the offer index to one criteria, load from CQos
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
	//get the host load map from cQos
	//loop through the hosts summary map
	//and sort the host summary map according to the host load map
	loadMap, err := l.pollFromCQos(ctx)
	if err != nil {
		return nil
	}

	// loadHostMap key is the load, value is an array of hosts of this load
	loadHostMap := l.bucketSortByLoad(loadMap)
	// loop through the hosts of same load
	cqosLoadMin := int32(0)
	cqosLoadMax := int32(100)
	for i := cqosLoadMin; i <= cqosLoadMax; i++ {
		if _, ok := loadHostMap[i]; !ok {
			continue
		}
		for _, perHostLoad := range loadHostMap[i] {
			hsummary := offerIndex[perHostLoad.hostName]
			summaryList = append(summaryList, hsummary)
			delete(offerIndexCopy, perHostLoad.hostName)
		}
	}

	// this means some hosts are in mesos offers while not in cqos response.
	// We will temporary treat those hosts with load of 100
	if len(offerIndexCopy) != 0 {
		for host := range offerIndexCopy {
			summaryList = append(summaryList, offerIndexCopy[host])
		}
	}
	return summaryList
}

// bucketSortByLoad bucket sorting the load
// the load will be [0..100]
// map looks like 0 => {host1, host2}
//                1 => {host3}...
func (l *loadAwareRanker) bucketSortByLoad(
	loadMap *cqos.GetHostMetricsResponse) map[int32][]hostLoad {
	// loadHostMap records load to hosts of the same load
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
	result, err := l.cqosClient.GetHostMetrics(
		ctx, req)
	if err != nil {
		log.WithError(err).
			Warn("Failed to reach CQos.")
		return nil, err
	}
	return result, nil
}
