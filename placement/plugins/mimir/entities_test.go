package mimir

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"code.uber.internal/infra/peloton/placement/testutil"
)

func TestEntityMapper_Convert(t *testing.T) {
	task := testutil.SetupAssignment(time.Now(), 1).GetTask().GetTask()
	entity := TaskToEntity(task)
	assert.Equal(t, "task-id", entity.Name)
	assert.Equal(t, 1, entity.Relations.Count(labels.NewLabel("relationKey", "relationValue")))
	assert.NotNil(t, entity.Ordering)

	assert.Equal(t, 3200.0, entity.Metrics.Get(metrics.CPUUsed))
	assert.Equal(t, 1000.0, entity.Metrics.Get(metrics.GPUUsed))
	assert.Equal(t, 4096.0*metrics.MiB, entity.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 1024.0*metrics.MiB, entity.Metrics.Get(metrics.DiskUsed))
	assert.Equal(t, 3.0, entity.Metrics.Get(metrics.PortsUsed))

	assert.Equal(t, 5, len(entity.MetricRequirements))
	for _, requirement := range entity.MetricRequirements {
		assert.Equal(t, requirements.GreaterThanEqual, requirement.Comparison)
		switch requirement.MetricType {
		case metrics.CPUFree:
			assert.Equal(t, 3200.0, requirement.Value)
		case metrics.GPUFree:
			assert.Equal(t, 1000.0, requirement.Value)
		case metrics.MemoryFree:
			assert.Equal(t, 4096.0*metrics.MiB, requirement.Value)
		case metrics.DiskFree:
			assert.Equal(t, 1024.0*metrics.MiB, requirement.Value)
		case metrics.PortsFree:
			assert.Equal(t, 3.0, requirement.Value)
		}
	}

	or, ok := entity.AffinityRequirement.(*requirements.OrRequirement)
	assert.True(t, ok)
	assert.NotNil(t, or)
	assert.Equal(t, 1, len(or.AffinityRequirements))
	and, ok := or.AffinityRequirements[0].(*requirements.AndRequirement)
	assert.True(t, ok)
	assert.NotNil(t, and)
	assert.Equal(t, 2, len(and.AffinityRequirements))

	label, ok := and.AffinityRequirements[0].(*requirements.LabelRequirement)
	assert.True(t, ok)
	assert.NotNil(t, label)
	assert.Equal(t, labels.NewLabel("offer", "*"), label.AppliesTo)
	assert.Equal(t, labels.NewLabel("key1", "value1"), label.Label)
	assert.Equal(t, requirements.LessThan, label.Comparison)
	assert.Equal(t, 1, label.Occurrences)

	relation, ok := and.AffinityRequirements[1].(*requirements.RelationRequirement)
	assert.True(t, ok)
	assert.NotNil(t, relation)
	assert.Equal(t, labels.NewLabel("offer", "*"), relation.AppliesTo)
	assert.Equal(t, labels.NewLabel("key2", "value2"), relation.Relation)
	assert.Equal(t, requirements.Equal, relation.Comparison)
	assert.Equal(t, 1, relation.Occurrences)
}
