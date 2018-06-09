package testutil

import (
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/placement/models"
)

// SetupAssignment creates an assignment.
func SetupAssignment(deadline time.Time, maxRounds int) *models.Assignment {
	relationKey := "relationKey"
	relationValue := "relationValue"
	resmgrTask := &resmgr.Task{
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
			CpuLimit:    32.0,
			GpuLimit:    10.0,
			MemLimitMb:  4096.0,
			DiskLimitMb: 1024.0,
			FdLimit:     32,
		},
		NumPorts: 3,
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
										Condition: task.LabelConstraint_CONDITION_LESS_THAN,
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
		Type: resmgr.TaskType_BATCH,
	}
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	task := models.NewTask(resmgrGang, resmgrTask, deadline, maxRounds)
	return models.NewAssignment(task)
}
