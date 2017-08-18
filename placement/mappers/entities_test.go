package mappers

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
	"github.com/stretchr/testify/assert"
)

func TestEntityMapper_Convert(t *testing.T) {
	relationKey := "relationKey"
	relationValue := "relationValue"
	task := &resmgr.Task{
		Name: "task",
		Id: &peloton.TaskID{
			Value: "id",
		},
		Labels: &mesos_v1.Labels{
			Labels: []*mesos_v1.Label{
				{
					Key:   &relationKey,
					Value: &relationValue,
				},
			},
		},
		Resource: &task.ResourceConfig{
			CpuLimit:    100.0,
			GpuLimit:    1000.0,
			DiskLimitMb: 42.0,
			MemLimitMb:  84.0,
			FdLimit:     32,
		},
		Constraint: &task.Constraint{
			Type: task.Constraint_AND_CONSTRAINT,
			AndConstraint: &task.AndConstraint{
				Constraints: []*task.Constraint{
					{
						Type: task.Constraint_OR_CONSTRAINT,
						OrConstraint: &task.OrConstraint{
							Constraints: []*task.Constraint{
								{
									Type: task.Constraint_LABEL_CONSTRAINT,
									LabelConstraint: &task.LabelConstraint{
										Kind:      task.LabelConstraint_TASK,
										Condition: task.LabelConstraint_CONDITION_EQUAL,
										Label: &peloton.Label{
											Key:   "key",
											Value: "value",
										},
										Requirement: 1,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	entity, err := TaskToEntity(task)
	assert.NoError(t, err)
	assert.Equal(t, "task-id", entity.Name)
	assert.Equal(t, 1, entity.Relations.Count(labels.NewLabel("relationKey", "relationValue")))
	assert.NotNil(t, entity.Ordering)

	assert.Equal(t, 100.0, entity.Metrics.Get(metrics.CPUUsed))
	assert.Equal(t, 1000.0, entity.Metrics.Get(metrics.GPUUsed))
	assert.Equal(t, 42.0*metrics.MiB, entity.Metrics.Get(metrics.DiskUsed))
	assert.Equal(t, 84.0*metrics.MiB, entity.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 32.0, entity.Metrics.Get(metrics.FileDescriptorsUsed))

	assert.Equal(t, 5, len(entity.MetricRequirements))
	for _, requirement := range entity.MetricRequirements {
		assert.Equal(t, requirements.GreaterThanEqual, requirement.Comparison)
		switch requirement.MetricType {
		case metrics.CPUFree:
			assert.Equal(t, 100.0, requirement.Value)
		case metrics.GPUFree:
			assert.Equal(t, 1000.0, requirement.Value)
		case metrics.MemoryFree:
			assert.Equal(t, 84.0*metrics.MiB, requirement.Value)
		case metrics.DiskFree:
			assert.Equal(t, 42.0*metrics.MiB, requirement.Value)
		case metrics.FileDescriptorsFree:
			assert.Equal(t, 32.0, requirement.Value)
		}
	}

	and, ok := entity.AffinityRequirement.(*requirements.AndRequirement)
	assert.True(t, ok)
	assert.NotNil(t, and)
	assert.Equal(t, 1, len(and.AffinityRequirements))
	or, ok := and.AffinityRequirements[0].(*requirements.OrRequirement)
	assert.True(t, ok)
	assert.NotNil(t, or)
	assert.Equal(t, 1, len(or.AffinityRequirements))
	relation, ok := or.AffinityRequirements[0].(*requirements.RelationRequirement)
	assert.True(t, ok)
	assert.NotNil(t, relation)
	assert.Equal(t, labels.NewLabel("offer", "*"), relation.AppliesTo)
	assert.Equal(t, labels.NewLabel("key", "value"), relation.Relation)
	assert.Equal(t, requirements.Equal, relation.Comparison)
	assert.Equal(t, 1, relation.Occurrences)
}
