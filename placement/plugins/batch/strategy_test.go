package batch

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/placement/models"
	"code.uber.internal/infra/peloton/placement/testutil"
	"github.com/stretchr/testify/assert"
)

func TestBatchPlace(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	offers := []*models.Host{
		testutil.SetupHost(),
		testutil.SetupHost(),
	}
	strategy := New()
	strategy.PlaceOnce(assignments, offers)

	assert.Equal(t, offers[0], assignments[0].Host())
	assert.Equal(t, offers[1], assignments[1].Host())
	assert.Nil(t, assignments[2].Host())
}

func TestBatchPlaceOneFreeHost(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[0].Task().Task().Resource.CpuLimit = 5
	assignments[1].Task().Task().Resource.CpuLimit = 5
	offers := []*models.Host{
		testutil.SetupHost(),
		testutil.SetupHost(),
	}
	strategy := New()
	strategy.PlaceOnce(assignments, offers)

	assert.Equal(t, offers[0], assignments[0].Host())
	assert.Equal(t, offers[0], assignments[1].Host())
}

func TestBatchFiltersWithResources(t *testing.T) {
	assignments := []*models.Assignment{
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
		testutil.SetupAssignment(time.Now().Add(10*time.Second), 1),
	}
	assignments[2].Task().Task().Resource.CpuLimit += 1.0
	strategy := New()

	filters := strategy.Filters(assignments)

	assert.Equal(t, 2, len(filters))
	for filter, batch := range filters {
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
	assignments[0].Task().Task().NumPorts = 1
	assignments[1].Task().Task().NumPorts = 1
	assignments[2].Task().Task().NumPorts = 2
	strategy := New()

	filters := strategy.Filters(assignments)

	assert.Equal(t, 2, len(filters))
	for filter, batch := range filters {
		switch filter.ResourceConstraint.NumPorts {
		case 1:
			assert.Equal(t, 2, len(batch))
		case 2:
			assert.Equal(t, 1, len(batch))
		}
	}
}
