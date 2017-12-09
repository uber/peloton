package models

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/labels"
	"code.uber.internal/infra/peloton/mimir-lib/model/metrics"
	"code.uber.internal/infra/peloton/mimir-lib/model/requirements"
)

func setupEntityMapperVariables() *resmgr.Task {
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
			CpuLimit:    1.0,
			GpuLimit:    10.0,
			MemLimitMb:  4096.0,
			DiskLimitMb: 1024.0,
			FdLimit:     32,
		},
		NumPorts: 2,
		Constraint: &task.Constraint{
			Type: task.Constraint_OR_CONSTRAINT,
			OrConstraint: &task.OrConstraint{
				Constraints: []*task.Constraint{
					{
						Type: task.Constraint_AND_CONSTRAINT,
						AndConstraint: &task.AndConstraint{
							Constraints: []*task.Constraint{
								{
									Type: task.Constraint_LABEL_CONSTRAINT,
									LabelConstraint: &task.LabelConstraint{
										Kind:      task.LabelConstraint_HOST,
										Condition: task.LabelConstraint_CONDITION_LESS_THAN,
										Label: &peloton.Label{
											Key:   "key1",
											Value: "value1",
										},
										Requirement: 1,
									},
								},
								{
									Type: task.Constraint_LABEL_CONSTRAINT,
									LabelConstraint: &task.LabelConstraint{
										Kind:      task.LabelConstraint_TASK,
										Condition: task.LabelConstraint_CONDITION_EQUAL,
										Label: &peloton.Label{
											Key:   "key2",
											Value: "value2",
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
	return task
}

func TestEntityMapper_Convert(t *testing.T) {
	task := setupEntityMapperVariables()
	entity := TaskToEntity(task)
	assert.Equal(t, "task-id", entity.Name)
	assert.Equal(t, 1, entity.Relations.Count(labels.NewLabel("relationKey", "relationValue")))
	assert.NotNil(t, entity.Ordering)

	assert.Equal(t, 100.0, entity.Metrics.Get(metrics.CPUUsed))
	assert.Equal(t, 1000.0, entity.Metrics.Get(metrics.GPUUsed))
	assert.Equal(t, 4096.0*metrics.MiB, entity.Metrics.Get(metrics.MemoryUsed))
	assert.Equal(t, 1024.0*metrics.MiB, entity.Metrics.Get(metrics.DiskUsed))
	assert.Equal(t, 2.0, entity.Metrics.Get(metrics.PortsUsed))

	assert.Equal(t, 5, len(entity.MetricRequirements))
	for _, requirement := range entity.MetricRequirements {
		assert.Equal(t, requirements.GreaterThanEqual, requirement.Comparison)
		switch requirement.MetricType {
		case metrics.CPUFree:
			assert.Equal(t, 100.0, requirement.Value)
		case metrics.GPUFree:
			assert.Equal(t, 1000.0, requirement.Value)
		case metrics.MemoryFree:
			assert.Equal(t, 4096.0*metrics.MiB, requirement.Value)
		case metrics.DiskFree:
			assert.Equal(t, 1024.0*metrics.MiB, requirement.Value)
		case metrics.PortsFree:
			assert.Equal(t, 2.0, requirement.Value)
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
