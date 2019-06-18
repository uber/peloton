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

package batch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v0"
	"github.com/uber/peloton/pkg/placement/testutil"
)

func TestBatchPlacePackLoadedHost(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}
	strategy := New()
	tasks := models.AssignmentsToTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)

	assert.Equal(t, 0, placements[0])
	assert.Equal(t, 1, placements[1])
	assert.Equal(t, -1, placements[2])
}

func TestBatchGetTaskPlacementsPackFreeHost(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[0].GetTask().GetTask().Resource.CpuLimit = 5
	assignments[1].GetTask().GetTask().Resource.CpuLimit = 5
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}

	strategy := New()
	tasks := models.AssignmentsToTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)
	assert.Equal(t, 0, placements[0])
	assert.Equal(t, 0, placements[1])
}

func TestBatchGetTaskPlacementsSpread(t *testing.T) {
	assignments := make([]*models.Assignment, 0)
	for i := 0; i < 5; i++ {
		a := testutil.SetupAssignment(time.Now().Add(10*time.Second), 1)
		a.GetTask().GetTask().Resource.CpuLimit = 5
		a.GetTask().GetTask().PlacementStrategy = job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB
		assignments = append(assignments, a)
	}
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}

	strategy := New()
	tasks := models.AssignmentsToTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)

	assert.Equal(t, 0, placements[0])
	assert.Equal(t, 1, placements[1])
	assert.Equal(t, 2, placements[2])
	assert.Equal(t, -1, placements[3])
	assert.Equal(t, -1, placements[4])
}

func TestBatchFiltersWithResources(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[2].GetTask().GetTask().Resource.CpuLimit += 1.0

	strategy := New()
	tasks := models.AssignmentsToTasks(assignments)
	tasksByNeeds := strategy.GroupTasksByPlacementNeeds(tasks)
	assert.Equal(t, 2, len(tasksByNeeds))
	for _, group := range tasksByNeeds {
		filter := v0_plugins.PlacementNeedsToHostFilter(group.PlacementNeeds)
		batch := group.Tasks
		assert.Equal(t, uint32(len(batch)), filter.GetQuantity().GetMaxHosts())
		switch filter.ResourceConstraint.Minimum.CpuLimit {
		case 32.0:
			assert.Equal(t, 2, len(batch))
		case 33.0:
			assert.Equal(t, 1, len(batch))
		}
	}
}

func TestBatchFiltersWithPorts(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[0].GetTask().GetTask().NumPorts = 1
	assignments[1].GetTask().GetTask().NumPorts = 1
	assignments[2].GetTask().GetTask().NumPorts = 2

	strategy := New()
	tasks := models.AssignmentsToTasks(assignments)
	tasksByNeeds := strategy.GroupTasksByPlacementNeeds(tasks)

	assert.Equal(t, 2, len(tasksByNeeds))
	for _, group := range tasksByNeeds {
		filter := v0_plugins.PlacementNeedsToHostFilter(group.PlacementNeeds)
		batch := group.Tasks
		assert.Equal(t, uint32(len(batch)), filter.GetQuantity().GetMaxHosts())
		switch filter.ResourceConstraint.NumPorts {
		case 1:
			assert.Equal(t, 2, len(batch))
		case 2:
			assert.Equal(t, 1, len(batch))
		}
	}
}

func TestBatchFiltersWithPlacementHint(t *testing.T) {
	assignments := make([]plugins.Task, 0)
	for i := 0; i < 5; i++ {
		a := testutil.SetupAssignment(time.Now().Add(10*time.Second), 1)
		a.GetTask().GetTask().NumPorts = 2
		if i < 2 {
			a.GetTask().GetTask().PlacementStrategy = job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB
			a.GetTask().GetTask().NumPorts = 1
		}
		assignments = append(assignments, a)
	}

	strategy := New()
	tasksByNeeds := strategy.GroupTasksByPlacementNeeds(assignments)
	assert.Equal(t, 2, len(tasksByNeeds))

	for _, group := range tasksByNeeds {
		filter := v0_plugins.PlacementNeedsToHostFilter(group.PlacementNeeds)
		batch := group.Tasks
		assert.Equal(t, uint32(len(batch)), filter.GetQuantity().GetMaxHosts())
		switch filter.ResourceConstraint.NumPorts {
		case 1:
			assert.Equal(t, 2, len(batch))
			assert.Equal(
				t,
				hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM,
				filter.GetHint().GetRankHint())
		case 2:
			assert.Equal(t, 3, len(batch))
			assert.Equal(
				t,
				hostsvc.FilterHint_FILTER_HINT_RANKING_INVALID,
				filter.GetHint().GetRankHint())
		}
	}
}
