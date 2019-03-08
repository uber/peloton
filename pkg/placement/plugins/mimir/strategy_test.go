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

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/testutil"
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
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []*models.HostOffers{
		testutil.SetupHostOffers(),
	}
	strategy := setupStrategy()
	strategy.PlaceOnce(assignments, offers)

	assert.Equal(t, offers[0], assignments[0].GetHost())
	assert.Nil(t, assignments[1].GetHost())
}

// TestMimirPlacePreferHostWithMoreResource tests the case that
// a task would be placed on the host with more resources
func TestMimirPlacePreferHostWithMoreResource(t *testing.T) {
	assignments := []*models.Assignment{
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

	offers := []*models.HostOffers{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// the host will choose host with more free resources
	strategy := setupStrategy()
	strategy.PlaceOnce(assignments, offers)
	assert.Equal(t, hostWithEnoughResources, assignments[0].GetHost())
}

// TestMimirPlacePreferHostWithDesiredHost tests that the task
// would try to place a task on its desired host when there is
// enough resource for the task.
func TestMimirPlacePreferHostWithDesiredHost(t *testing.T) {
	assignments := []*models.Assignment{
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

	offers := []*models.HostOffers{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// if the task has a desired host, the task would be placed on the host
	// even if it has less resource
	assignments[0].Task.Task.DesiredHost = hostWithScarceResources.GetOffer().GetHostname()
	strategy := setupStrategy()
	strategy.PlaceOnce(assignments, offers)
	assert.Equal(t, hostWithScarceResources, assignments[0].GetHost())
}

// TestMimirPlaceIgnoreDesiredHostWhenNoEnoughResource tests that the task
// would try to place on other hosts, if its desired host does not have
// enough resource for the task.
func TestMimirPlaceIgnoreDesiredHostWhenNoEnoughResource(t *testing.T) {
	assignments := []*models.Assignment{
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

	offers := []*models.HostOffers{
		hostWithScarceResources, hostWithEnoughResources,
	}

	// if the task has a desired host, the task would try to place on the host.
	// But it could not as it does not have enough resources for the task.
	strategy := setupStrategy()
	assignments[0].Task.Task.DesiredHost = hostWithScarceResources.GetOffer().GetHostname()
	strategy.PlaceOnce(assignments, offers)
	assert.Equal(t, hostWithEnoughResources, assignments[0].GetHost())
}

func TestMimirFilters(t *testing.T) {
	strategy := setupStrategy()

	deadline := time.Now().Add(30 * time.Second)
	assignments := []*models.Assignment{
		testutil.SetupAssignment(deadline, 1),
	}
	taskTypeToExpectedMaxHosts := map[resmgr.TaskType]uint32{
		resmgr.TaskType_BATCH:     1,
		resmgr.TaskType_STATELESS: 1,
		resmgr.TaskType_DAEMON:    1,
		resmgr.TaskType_STATEFUL:  1,
	}
	for taskType, expectedMaxHosts := range taskTypeToExpectedMaxHosts {
		strategy.config.TaskType = taskType
		for filter := range strategy.Filters(assignments) {
			assert.NotNil(t, filter)
			assert.Equal(t, expectedMaxHosts, filter.GetQuantity().GetMaxHosts())
			assert.Equal(t, uint32(3), filter.GetResourceConstraint().GetNumPorts())
			assert.Equal(t, 32.0, filter.GetResourceConstraint().GetMinimum().GetCpuLimit())
			assert.Equal(t, 10.0, filter.GetResourceConstraint().GetMinimum().GetGpuLimit())
			assert.Equal(t, 4096.0, filter.GetResourceConstraint().GetMinimum().GetMemLimitMb())
			assert.Equal(t, 1024.0, filter.GetResourceConstraint().GetMinimum().GetDiskLimitMb())
		}
	}
}
