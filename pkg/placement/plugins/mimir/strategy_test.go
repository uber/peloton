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
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/plugins/v0"
	"github.com/uber/peloton/pkg/placement/testutil"

	"github.com/stretchr/testify/assert"
)

func setupStrategy() *mimir {
	config := &config.PlacementConfig{
		TaskDequeueLimit:     10,
		OfferDequeueLimit:    10,
		MaxPlacementDuration: 30 * time.Second,
		TaskDequeueTimeOut:   100,
		TaskType:             resmgr.TaskType_BATCH,
		FetchOfferTasks:      false,
	}
	placer := algorithms.NewPlacer(1, 100)
	return New(placer, config).(*mimir)
}

func TestMimirPlace(t *testing.T) {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []plugins.Host{
		testutil.SetupHostOffers(),
	}
	strategy := setupStrategy()
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)

	assert.Equal(t, 0, placements[0])
	assert.Equal(t, -1, placements[1])
}

// TestMimirPlacePreferHostWithMoreResource tests the case that
// a task would be placed on the host with more resources
func TestMimirPlacePreferHostWithMoreResource(t *testing.T) {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}

	hostWithEnoughResources := testutil.SetupHostOffers()
	hostWithEnoughResources.Offer.Hostname = "hostname1"

	hostWithScarceResources := testutil.SetupHostOffers()
	// cut each resource with scalar value by 1
	for _, resource := range hostWithScarceResources.Offer.Resources {
		if resource.GetScalar() != nil {
			value := resource.GetScalar().GetValue() - 1
			resource.Scalar = &mesos_v1.Value_Scalar{
				Value: &value,
			}
		}
	}
	hostWithScarceResources.Offer.Hostname = "hostname2"

	offers := []plugins.Host{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// the host will choose host with more free resources
	strategy := setupStrategy()
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)
	assert.Equal(t, 1, placements[0])
}

// TestMimirPlacePreferHostWithDesiredHost tests that the task
// would try to place a task on its desired host when there is
// enough resource for the task.
func TestMimirPlacePreferHostWithDesiredHost(t *testing.T) {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}

	hostWithEnoughResources := testutil.SetupHostOffers()
	hostWithEnoughResources.Offer.Hostname = "hostname1"

	hostWithScarceResources := testutil.SetupHostOffers()
	// cut each resource with scalar value by 1
	for _, resource := range hostWithScarceResources.Offer.Resources {
		if resource.GetScalar() != nil {
			value := resource.GetScalar().GetValue() - 1.0
			resource.Scalar = &mesos_v1.Value_Scalar{
				Value: &value,
			}
		}
	}
	hostWithScarceResources.Offer.Hostname = "hostname2"

	offers := []plugins.Host{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// if the task has a desired host, the task would be placed on the host
	// even if it has less resource
	assignments[0].Task.Task.DesiredHost = hostWithScarceResources.GetOffer().GetHostname()
	strategy := setupStrategy()
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)
	assert.Equal(t, 0, placements[0])
}

// TestMimirPlaceIgnoreDesiredHostWhenNoEnoughResource tests that the task
// would try to place on other hosts, if its desired host does not have
// enough resource for the task.
func TestMimirPlaceIgnoreDesiredHostWhenNoEnoughResource(t *testing.T) {
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}

	hostWithEnoughResources := testutil.SetupHostOffers()
	hostWithEnoughResources.Offer.Hostname = "hostname1"

	hostWithScarceResources := testutil.SetupHostOffers()
	for _, resource := range hostWithScarceResources.Offer.Resources {
		if resource.GetScalar() != nil {
			value := 0.1
			resource.Scalar = &mesos_v1.Value_Scalar{
				Value: &value,
			}
		}
	}
	hostWithScarceResources.Offer.Hostname = "hostname2"

	offers := []plugins.Host{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// if the task has a desired host, the task would try to place on the host.
	// But it could not as it does not have enough resources for the task.
	strategy := setupStrategy()
	assignments[0].Task.Task.DesiredHost = hostWithScarceResources.GetOffer().GetHostname()
	tasks := models_v0.AssignmentsToPluginsTasks(assignments)
	placements := strategy.GetTaskPlacements(tasks, offers)
	assert.Equal(t, 1, placements[0])
}

func TestMimirFilters(t *testing.T) {
	strategy := setupStrategy()

	deadline := time.Now().Add(30 * time.Second)
	hostConstraint := &task.Constraint{
		Type: task.Constraint_LABEL_CONSTRAINT,
		LabelConstraint: &task.LabelConstraint{
			Kind:      task.LabelConstraint_HOST,
			Condition: task.LabelConstraint_CONDITION_EQUAL,
			Label: &peloton.Label{
				Key:   "peloton/exclusive",
				Value: "ml",
			},
			Requirement: 1,
		},
	}
	assignments := []*models_v0.Assignment{
		testutil.SetupAssignment(deadline, 1),
		testutil.SetupAssignment(deadline, 1),
		testutil.SetupAssignment(deadline, 1),
		testutil.SetupAssignment(deadline, 1),
		testutil.SetupAssignment(deadline, 1),
		testutil.SetupAssignment(deadline, 1),
	}
	// assignment[0] requires 1 port, default resources, no constraints
	// assignment[1] requires 2 ports, default resources and default constraint
	// assignment[2] requires 3 ports, 8 cpu, 16 gpu, 4096 disk,
	// excl-host constraint
	// assignment[3] requires 4 port, default resources, excl-host constraint
	// assignment[4] requires 4 port, default resources, excl-host constraint
	// assignment[5] requires 4 port, default resources, excl-host constraint
	assignments[0].GetTask().GetTask().NumPorts = 1
	assignments[0].GetTask().GetTask().Constraint = nil

	assignments[1].GetTask().GetTask().NumPorts = 2

	assignments[2].GetTask().GetTask().NumPorts = 3
	assignments[2].GetTask().GetTask().Resource.CpuLimit = 8.0
	assignments[2].GetTask().GetTask().Resource.GpuLimit = 16.0
	assignments[2].GetTask().GetTask().Resource.DiskLimitMb = 4096.0
	assignments[2].GetTask().GetTask().Constraint = hostConstraint

	assignments[3].GetTask().GetTask().NumPorts = 4
	assignments[3].GetTask().GetTask().Constraint = hostConstraint

	assignments[4].GetTask().GetTask().NumPorts = 4
	assignments[4].GetTask().GetTask().Constraint = hostConstraint

	assignments[5].GetTask().GetTask().NumPorts = 4
	assignments[5].GetTask().GetTask().Constraint = hostConstraint

	taskTypes := []resmgr.TaskType{
		resmgr.TaskType_BATCH,
		resmgr.TaskType_STATELESS,
		resmgr.TaskType_DAEMON,
		resmgr.TaskType_STATEFUL,
	}
	for _, taskType := range taskTypes {
		strategy.config.TaskType = taskType

		tasks := models_v0.AssignmentsToPluginsTasks(assignments)
		tasksByNeeds := strategy.GroupTasksByPlacementNeeds(tasks)
		assert.Equal(t, 4, len(tasksByNeeds))

		for _, group := range tasksByNeeds {
			filter := plugins_v0.PlacementNeedsToHostFilter(group.PlacementNeeds)
			batch := group.Tasks
			assert.NotNil(t, filter)

			switch filter.ResourceConstraint.NumPorts {
			case 1:
				// assignment[0]
				assert.Equal(t, 1, len(batch))
				assert.Nil(t, filter.SchedulingConstraint)
				res := filter.GetResourceConstraint().GetMinimum()
				assert.Equal(t, 32.0, res.GetCpuLimit())
				assert.Equal(t, 10.0, res.GetGpuLimit())
				assert.Equal(t, 4096.0, res.GetMemLimitMb())
				assert.Equal(t, 1024.0, res.GetDiskLimitMb())
				assert.Equal(t, uint32(1), filter.GetQuantity().GetMaxHosts())
				assert.EqualValues(t, []int{0}, batch)

			case 2:
				// assignment[1]
				assert.Equal(t, 1, len(batch))
				assert.Equal(
					t,
					assignments[batch[0]].GetTask().GetTask().Constraint,
					filter.SchedulingConstraint)
				res := filter.GetResourceConstraint().GetMinimum()
				assert.Equal(t, 32.0, res.GetCpuLimit())
				assert.Equal(t, 10.0, res.GetGpuLimit())
				assert.Equal(t, 4096.0, res.GetMemLimitMb())
				assert.Equal(t, 1024.0, res.GetDiskLimitMb())
				assert.Equal(t, uint32(1), filter.GetQuantity().GetMaxHosts())
				assert.EqualValues(t, []int{1}, batch)

			case 3:
				// assignment[2]
				assert.Equal(t, 1, len(batch))
				assert.Equal(t, hostConstraint, filter.SchedulingConstraint)
				res := filter.GetResourceConstraint().GetMinimum()
				assert.Equal(t, 8.0, res.GetCpuLimit())
				assert.Equal(t, 16.0, res.GetGpuLimit())
				assert.Equal(t, 4096.0, res.GetMemLimitMb())
				assert.Equal(t, 4096.0, res.GetDiskLimitMb())
				assert.Equal(t, uint32(1), filter.GetQuantity().GetMaxHosts())
				assert.EqualValues(t, []int{2}, batch)
			case 4:
				// assignment[3], assignment[4], assignment[5]
				assert.Equal(t, 3, len(batch))
				assert.Equal(t, hostConstraint, filter.SchedulingConstraint)
				res := filter.GetResourceConstraint().GetMinimum()
				assert.Equal(t, 32.0, res.GetCpuLimit())
				assert.Equal(t, 10.0, res.GetGpuLimit())
				assert.Equal(t, 4096.0, res.GetMemLimitMb())
				assert.Equal(t, 1024.0, res.GetDiskLimitMb())
				assert.Equal(t, uint32(3), filter.GetQuantity().GetMaxHosts())
				assert.EqualValues(t, []int{3, 4, 5}, batch)
			default:
				assert.Fail(t, "Unexpected value for NumPorts")
			}
		}
	}
}
