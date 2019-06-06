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

package v0_testutil

import (
	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/pborman/uuid"
)

// SetupRMTask creates a new resource manager task.
func SetupRMTask() *resmgr.Task {
	relationKey := "relationKey"
	relationValue := "relationValue"
	taskID := uuid.New()
	return &resmgr.Task{
		Name: "task",
		Id: &peloton.TaskID{
			Value: taskID,
		},
		TaskId: &mesos_v1.TaskID{
			Value: &[]string{taskID + "-1"}[0],
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
}
