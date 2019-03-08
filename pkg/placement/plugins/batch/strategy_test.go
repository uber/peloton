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
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/testutil"
)

func TestBatchPlace(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []*models.HostOffers{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}
	strategy := New()
	strategy.PlaceOnce(assignments, offers)

	assert.Equal(t, offers[0], assignments[0].GetHost())
	assert.Equal(t, offers[1], assignments[1].GetHost())
	assert.Nil(t, assignments[2].GetHost())
}

func TestBatchPlaceOneFreeHost(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[0].GetTask().GetTask().Resource.CpuLimit = 5
	assignments[1].GetTask().GetTask().Resource.CpuLimit = 5
	offers := []*models.HostOffers{
		testutil.SetupHostOffers(),
		testutil.SetupHostOffers(),
	}
	strategy := New()
	strategy.PlaceOnce(assignments, offers)

	assert.Equal(t, offers[0], assignments[0].GetHost())
	assert.Equal(t, offers[0], assignments[1].GetHost())
}

func TestBatchFiltersWithResources(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[2].GetTask().GetTask().Resource.CpuLimit += 1.0
	strategy := New()

	filters := strategy.Filters(assignments)

	assert.Equal(t, 2, len(filters))
	for filter, batch := range filters {
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

	filters := strategy.Filters(assignments)

	assert.Equal(t, 2, len(filters))
	for filter, batch := range filters {
		assert.Equal(t, uint32(len(batch)), filter.GetQuantity().GetMaxHosts())
		switch filter.ResourceConstraint.NumPorts {
		case 1:
			assert.Equal(t, 2, len(batch))
		case 2:
			assert.Equal(t, 1, len(batch))
		}
	}
}
