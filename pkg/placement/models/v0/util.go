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

package models_v0

import (
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// NOTE: This file is needed because the following does not compile in Go:
// `var s1 []interface{} = []string{}`
// Even though `var s2 interface{} = ""` does.

// AssignmentsToPluginsTasks converts a slice of assignments into a slice
// of tasks for plugin use.
func AssignmentsToPluginsTasks(assignments []*Assignment) []plugins.Task {
	tasks := []plugins.Task{}
	for _, a := range assignments {
		tasks = append(tasks, a)
	}
	return tasks
}

// AssignmentsToTasks converts a slice of assignments into a slice
// of Tasks.
func AssignmentsToTasks(assignments []*Assignment) []models.Task {
	tasks := []models.Task{}
	for _, a := range assignments {
		tasks = append(tasks, a)
	}
	return tasks
}

// HostOffersToOffers converts a slice of HostOffers into a slice of
// Offers.
func HostOffersToOffers(offers []*HostOffers) []models.Offer {
	result := make([]models.Offer, len(offers))
	for i, offer := range offers {
		result[i] = offer
	}
	return result
}
