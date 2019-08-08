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
	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"
	"github.com/uber/peloton/pkg/hostmgr/metrics"

	log "github.com/sirupsen/logrus"
)

const (
	// DeFrag is the name for the de-fragmentation policy
	DeFrag = "DEFRAG"

	// FirstFit is the name of the First Fit policy
	FirstFit = "FIRST_FIT"

	// LoadAware is the name of the Load Aware policy
	LoadAware = "LOAD_AWARE"
)

// map of ranker name to Ranker. Not thread-safe -> should be
// updated at initialization only; only reads are safe after
// initialization.
var rankers = make(map[string]Ranker)

// register creates all the rankers and keeps it in the
// ranker map.
func register(
	name string,
	rankerFunc func() Ranker,
) {
	log.WithField("name", name).Info("Registering ranker")
	if rankerFunc == nil {
		log.WithField("name", name).Error("invalid ranker creator function")
		return
	}
	if _, registered := rankers[name]; registered {
		log.WithField("name", name).Error("ranker already registered")
		return
	}
	ranker := rankerFunc()

	if ranker == nil {
		log.WithField("name", name).Error("nil ranker created")
		return
	}
	rankers[name] = ranker
}

// Init registers all the rankers
func Init(
	cqosClient cqos.QoSAdvisorServiceYARPCClient,
	metrics *metrics.Metrics) {
	register(DeFrag, NewDeFragRanker)
	register(FirstFit, NewFirstFitRanker)

	// if QosAdivsorService discovery address is not set
	if cqosClient == nil {
		return
	}
	if _, registered := rankers[LoadAware]; registered {
		log.WithField("name", LoadAware).Error("ranker already registered")
		return
	}
	log.WithField("name", LoadAware).Info("Registering ranker")
	rankers[LoadAware] = NewLoadAwareRanker(cqosClient, metrics)
}

// GetRankerByName returns a ranker with specified name
func GetRankerByName(name string) Ranker {
	return rankers[name]
}

// GetRankers returns all registered rankers
func GetRankers() []Ranker {
	result := make([]Ranker, 0, len(rankers))
	for _, r := range rankers {
		result = append(result, r)
	}
	return result
}

// CleanUpRanker is for testing purpose only.
func CleanUpRanker() {
	rankers = make(map[string]Ranker)
}
