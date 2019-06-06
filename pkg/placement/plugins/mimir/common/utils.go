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

package mimir

import (
	"fmt"

	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/labels"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"
)

// Group is a helper to be able to make structured logging of the Mimir library types.
type Group struct {
	Name      string             `json:"name"`
	Labels    map[string]int     `json:"labels"`
	Metrics   map[string]float64 `json:"metrics"`
	Relations map[string]int     `json:"relations"`
	Entities  map[string]*Entity `json:"entities"`
}

// DumpGroup converts a Mimir group into a group who's structure we can log.
func DumpGroup(group *placement.Group) *Group {
	entities := map[string]*Entity{}
	for name, entity := range group.Entities {
		entities[name] = DumpEntity(entity)
	}
	return &Group{
		Name:      group.Name,
		Labels:    dumpLabelBag(group.Labels),
		Metrics:   dumpMetricSet(group.Metrics),
		Relations: dumpLabelBag(group.Relations),
		Entities:  entities,
	}
}

// Entity is a helper to be able to make structured logging of the Mimir library types.
type Entity struct {
	Name      string             `json:"name"`
	Relations map[string]int     `json:"relations"`
	Metrics   map[string]float64 `json:"metrics"`
}

// DumpEntity converts a Mimir entity into an entity who's structure we can log.
func DumpEntity(entity *placement.Entity) *Entity {
	return &Entity{
		Name:      entity.Name,
		Relations: dumpLabelBag(entity.Relations),
		Metrics:   dumpMetricSet(entity.Metrics),
	}
}

// dumpLabelBag converts a Mimir label bag into map of labels to counts who's structure we can log.
func dumpLabelBag(bag *labels.Bag) map[string]int {
	result := map[string]int{}
	for _, label := range bag.Labels() {
		result[label.String()] = bag.Count(label)
	}
	return result
}

// dumpMetricSet converts a Mimir metric set into map of metrics to values who's structure we can log.
func dumpMetricSet(set *metrics.Set) map[string]float64 {
	result := map[string]float64{}
	for _, metricType := range set.Types() {
		result[fmt.Sprintf("%v (%v)", metricType.Name, metricType.Unit)] = set.Get(metricType)
	}
	return result
}
