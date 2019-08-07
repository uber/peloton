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

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/v0"
	"github.com/uber/peloton/pkg/placement/testutil"

	"github.com/stretchr/testify/suite"
)

type BatchStrategyTestSuite struct {
	suite.Suite
}

func TestBatchStrategyTestSuite(t *testing.T) {
	suite.Run(t, new(BatchStrategyTestSuite))
}

func (suite *BatchStrategyTestSuite) TestBatchPlacePackLoadedHost() {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}
	strategy := New(&config.PlacementConfig{})
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)

	suite.Equal(0, placements[0])
	suite.Equal(1, placements[1])
	suite.Equal(-1, placements[2])
}

func (suite *BatchStrategyTestSuite) TestBatchGetTaskPlacementsPackFreeHost() {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[0].GetTask().GetTask().Resource.CpuLimit = 5
	assignments[1].GetTask().GetTask().Resource.CpuLimit = 5
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}

	strategy := New(&config.PlacementConfig{})
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)
	suite.Equal(0, placements[0])
	suite.Equal(0, placements[1])
}

func (suite *BatchStrategyTestSuite) TestBatchGetTaskPlacementsSpread() {
	assignments := make([]*models_v0.Assignment, 0)
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

	strategy := New(&config.PlacementConfig{})
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)

	suite.Equal(0, placements[0])
	suite.Equal(1, placements[1])
	suite.Equal(2, placements[2])
	suite.Equal(-1, placements[3])
	suite.Equal(-1, placements[4])
}

// TODO: Add test cases for using host pool.
func (suite *BatchStrategyTestSuite) TestBatchFiltersWithResources() {
	testCases := map[string]struct {
		useHostPool bool
		taskType    resmgr.TaskType
	}{
		"not-use-host-pool": {
			useHostPool: false,
		},
	}

	for tcName, tc := range testCases {
		cfg := &config.PlacementConfig{
			UseHostPool: tc.useHostPool,
			TaskType:    tc.taskType,
		}
		assignments := []*models_v0.Assignment{
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		}
		assignments[2].GetTask().GetTask().Resource.CpuLimit += 1.0

		strategy := New(cfg)
		tasks := models_v0.AssignmentsToPluginsTasks(assignments)
		tasksByNeeds := strategy.GroupTasksByPlacementNeeds(tasks)
		suite.Equal(2, len(tasksByNeeds), "test case: %s", tcName)
		for _, group := range tasksByNeeds {
			filter := plugins_v0.PlacementNeedsToHostFilter(group.PlacementNeeds)
			batch := group.Tasks
			suite.Equal(
				uint32(len(batch)), filter.GetQuantity().GetMaxHosts(),
				"test case: %s", tcName)
			switch filter.ResourceConstraint.Minimum.CpuLimit {
			case 32.0:
				suite.Equal(2, len(batch), "test case: %s", tcName)
			case 33.0:
				suite.Equal(1, len(batch), "test case: %s", tcName)
			}
		}
	}
}

func (suite *BatchStrategyTestSuite) TestBatchFiltersWithPorts() {
	testCases := map[string]struct {
		enableHostPool bool
	}{
		"host-pool-disabled": {
			enableHostPool: false,
		},
	}

	for tcName, tc := range testCases {
		cfg := &config.PlacementConfig{
			UseHostPool: tc.enableHostPool,
		}
		assignments := []*models_v0.Assignment{
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
			testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		}
		assignments[0].GetTask().GetTask().NumPorts = 1
		assignments[1].GetTask().GetTask().NumPorts = 1
		assignments[2].GetTask().GetTask().NumPorts = 2

		strategy := New(cfg)
		tasks := models_v0.AssignmentsToPluginsTasks(assignments)
		tasksByNeeds := strategy.GroupTasksByPlacementNeeds(tasks)

		suite.Equal(2, len(tasksByNeeds), "test case: %s", tcName)
		for _, group := range tasksByNeeds {
			filter := plugins_v0.PlacementNeedsToHostFilter(group.PlacementNeeds)
			batch := group.Tasks
			suite.Equal(
				uint32(len(batch)),
				filter.GetQuantity().GetMaxHosts(),
				"test case: %s", tcName,
			)
			switch filter.ResourceConstraint.NumPorts {
			case 1:
				suite.Equal(2, len(batch), "test case: %s", tcName)
			case 2:
				suite.Equal(1, len(batch), "test case: %s", tcName)
			}
		}
	}
}

func (suite *BatchStrategyTestSuite) TestBatchFiltersWithPlacementHint() {
	testCases := map[string]struct {
		enableHostPool bool
	}{
		"host-pool-disabled": {
			enableHostPool: false,
		},
	}

	for tcName, tc := range testCases {
		cfg := &config.PlacementConfig{
			UseHostPool: tc.enableHostPool,
		}
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

		strategy := New(cfg)
		tasksByNeeds := strategy.GroupTasksByPlacementNeeds(assignments)
		suite.Equal(2, len(tasksByNeeds), "test case: %s", tcName)

		for _, group := range tasksByNeeds {
			filter := plugins_v0.PlacementNeedsToHostFilter(group.PlacementNeeds)
			batch := group.Tasks
			suite.Equal(
				uint32(len(batch)), filter.GetQuantity().GetMaxHosts(),
				"test case: %s", tcName)
			switch filter.ResourceConstraint.NumPorts {
			case 1:
				suite.Equal(2, len(batch), "test case: %s", tcName)
				suite.Equal(
					hostsvc.FilterHint_FILTER_HINT_RANKING_RANDOM,
					filter.GetHint().GetRankHint(), "test case: %s", tcName)
			case 2:
				suite.Equal(3, len(batch), "test case: %s", tcName)
				suite.Equal(
					hostsvc.FilterHint_FILTER_HINT_RANKING_INVALID,
					filter.GetHint().GetRankHint(), "test case: %s", tcName)
			}
		}
	}
}
