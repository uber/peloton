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

package plugins

import (
	"fmt"
	"testing"

	peloton_api_v0_peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_api_v0_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

// PluginsHelperTestSuite is the test suite for helper functions in plugins package.
type PluginsHelperTestSuite struct {
	suite.Suite

	ctrl *gomock.Controller
}

func TestPluginHelperTestSuite(t *testing.T) {
	suite.Run(t, new(PluginsHelperTestSuite))
}

// TestGroupByPlacementNeeds tests grouping tasks based on their placement needs.
func (suite *PluginsHelperTestSuite) TestGroupByPlacementNeeds() {
	tasks := suite.setupMockTasks()

	testCases := map[string]struct {
		tasks         []Task
		config        *Config
		expectedNeeds []*TasksByPlacementNeeds
	}{
		"host-pool-disabled": {
			tasks: tasks,
			config: &Config{
				UseHostPool: false,
			},
			expectedNeeds: []*TasksByPlacementNeeds{
				{
					PlacementNeeds: PlacementNeeds{
						Constraint: &peloton_api_v0_task.LabelConstraint{
							Kind:      peloton_api_v0_task.LabelConstraint_HOST,
							Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
							Label: &peloton_api_v0_peloton.Label{
								Key:   fmt.Sprintf("key%d", 0),
								Value: fmt.Sprintf("value%d", 0),
							},
							Requirement: 0,
						},
					},
					Tasks: []int{0},
				},
				{
					PlacementNeeds: PlacementNeeds{
						Constraint: &peloton_api_v0_task.LabelConstraint{
							Kind:      peloton_api_v0_task.LabelConstraint_HOST,
							Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
							Label: &peloton_api_v0_peloton.Label{
								Key:   fmt.Sprintf("key%d", 1),
								Value: fmt.Sprintf("value%d", 1),
							},
							Requirement: 1,
						},
					},
					Tasks: []int{1},
				},
				{
					PlacementNeeds: PlacementNeeds{
						Constraint: &peloton_api_v0_task.LabelConstraint{
							Kind:      peloton_api_v0_task.LabelConstraint_HOST,
							Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
							Label: &peloton_api_v0_peloton.Label{
								Key:   fmt.Sprintf("key%d", 2),
								Value: fmt.Sprintf("value%d", 2),
							},
							Requirement: 2,
						},
					},
					Tasks: []int{2},
				},
			},
		},
	}

	for tcName, tc := range testCases {
		needs := GroupByPlacementNeeds(tc.tasks, tc.config)
		suite.ElementsMatch(tc.expectedNeeds, needs, "test case: %s", tcName)
	}
}

// TestUpsertConstraint tests upserting given task constraints into placement needs.
func (suite *PluginsHelperTestSuite) TestUpsertConstraint() {
	testCases := map[string]struct {
		placementConstraint interface{}
		taskConstraint      *peloton_api_v0_task.Constraint
		expectedNeeds       PlacementNeeds
		expectedErrMsg      string
	}{
		"nil-task-constraint": {
			expectedNeeds: PlacementNeeds{
				Constraint: nil,
			},
		},
		"empty-task-constraint": {
			taskConstraint: &peloton_api_v0_task.Constraint{},
			expectedNeeds: PlacementNeeds{
				Constraint: nil,
			},
		},
		"nil-placement-constraint": {
			taskConstraint: getFakeLabelConstraint("key1", "value1"),
			expectedErrMsg: "failed to convert constraint <nil> to " +
				"*peloton_api_v0_task.Constraint",
		},
		"invalid-placement-constraint": {
			placementConstraint: "I'm not a valid placement constraint",
			taskConstraint:      getFakeLabelConstraint("key1", "value1"),
			expectedErrMsg: "failed to convert constraint " +
				"I'm not a valid placement constraint to " +
				"*peloton_api_v0_task.Constraint",
		},
		"empty-placement-constraint-with-label-task-constraint": {
			placementConstraint: &peloton_api_v0_task.Constraint{},
			taskConstraint:      getFakeLabelConstraint("key1", "value1"),
			expectedNeeds: PlacementNeeds{
				Constraint: getFakeLabelConstraint("key1", "value1"),
			},
		},
		"empty-placement-constraint-with-or-task-constraint": {
			placementConstraint: &peloton_api_v0_task.Constraint{},
			taskConstraint:      getFakeOrConstraint("key1", "value1"),
			expectedNeeds: PlacementNeeds{
				Constraint: getFakeOrConstraint("key1", "value1"),
			},
		},
		"empty-placement-constraint-with-and-task-constraint": {
			placementConstraint: &peloton_api_v0_task.Constraint{},
			taskConstraint:      getFakeAndConstraint("key1", "value1"),
			expectedNeeds: PlacementNeeds{
				Constraint: getFakeAndConstraint("key1", "value1"),
			},
		},
		"label-placement-constraint-with-label-task-constraint": {
			placementConstraint: getFakeLabelConstraint("key1", "value1"),
			taskConstraint:      getFakeLabelConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeLabelConstraint("key1", "value1"),
							getFakeLabelConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"label-placement-constraint-with-or-task-constraint": {
			placementConstraint: getFakeLabelConstraint("key1", "value1"),
			taskConstraint:      getFakeOrConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeLabelConstraint("key1", "value1"),
							getFakeOrConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"label-placement-constraint-with-and-task-constraint": {
			placementConstraint: getFakeLabelConstraint("key1", "value1"),
			taskConstraint:      getFakeAndConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeLabelConstraint("key1", "value1"),
							getFakeAndConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"or-placement-constraint-with-label-task-constraint": {
			placementConstraint: getFakeOrConstraint("key1", "value1"),
			taskConstraint:      getFakeLabelConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeOrConstraint("key1", "value1"),
							getFakeLabelConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"or-placement-constraint-with-or-task-constraint": {
			placementConstraint: getFakeOrConstraint("key1", "value1"),
			taskConstraint:      getFakeOrConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeOrConstraint("key1", "value1"),
							getFakeOrConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"or-placement-constraint-with-and-task-constraint": {
			placementConstraint: getFakeOrConstraint("key1", "value1"),
			taskConstraint:      getFakeAndConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeOrConstraint("key1", "value1"),
							getFakeAndConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"and-placement-constraint-with-label-task-constraint": {
			placementConstraint: getFakeAndConstraint("key1", "value1"),
			taskConstraint:      getFakeLabelConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeAndConstraint("key1", "value1"),
							getFakeLabelConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"and-placement-constraint-with-or-task-constraint": {
			placementConstraint: getFakeAndConstraint("key1", "value1"),
			taskConstraint:      getFakeOrConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeAndConstraint("key1", "value1"),
							getFakeOrConstraint("key2", "value2"),
						},
					},
				},
			},
		},
		"and-placement-constraint-with-and-task-constraint": {
			placementConstraint: getFakeAndConstraint("key1", "value1"),
			taskConstraint:      getFakeAndConstraint("key2", "value2"),
			expectedNeeds: PlacementNeeds{
				Constraint: &peloton_api_v0_task.Constraint{
					Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
					AndConstraint: &peloton_api_v0_task.AndConstraint{
						Constraints: []*peloton_api_v0_task.Constraint{
							getFakeAndConstraint("key1", "value1"),
							getFakeAndConstraint("key2", "value2"),
						},
					},
				},
			},
		},
	}

	for tcName, tc := range testCases {
		needs := PlacementNeeds{
			Constraint: tc.placementConstraint,
		}

		err := needs.upsertConstraint(tc.taskConstraint)
		if tc.expectedErrMsg != "" {
			suite.EqualError(
				err,
				tc.expectedErrMsg,
				"test case: %s", tcName,
			)
		} else {
			suite.EqualValues(
				tc.expectedNeeds,
				needs,
				"test case: %s", tcName,
			)
			suite.NoError(err, "test case: %s", tcName)
		}
	}
}

// setupMockTasks set up mock tasks for testing purpose.
func (suite *PluginsHelperTestSuite) setupMockTasks() []Task {
	var mockTasks []Task

	// Set up 3 mock tasks.
	for i := 0; i < 3; i++ {
		mt := newFakeTask(i)
		mockTasks = append(mockTasks, mt)

	}
	return mockTasks
}

// fakeTask is a fake implementation of Task interface.
// It should be only used for testing purpose
// since we can't use mock task due to circular dependency.
type fakeTask struct {
	constraint interface{}
}

// newFakeTask returns a new fakeTask.
func newFakeTask(index int) *fakeTask {
	return &fakeTask{
		constraint: &peloton_api_v0_task.LabelConstraint{
			Kind:      peloton_api_v0_task.LabelConstraint_HOST,
			Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
			Label: &peloton_api_v0_peloton.Label{
				Key:   fmt.Sprintf("key%d", index),
				Value: fmt.Sprintf("value%d", index),
			},
			Requirement: uint32(index),
		},
	}
}

// PelotonID returns an empty string.
func (t *fakeTask) PelotonID() string {
	return ""
}

// Fits returns empty resources, 0 and false.
func (t *fakeTask) Fits(
	resLeft scalar.Resources,
	portsLeft uint64,
) (scalar.Resources, uint64, bool) {
	return scalar.Resources{}, 0, false
}

// SetPlacementFailure is an empty implementation.
func (t *fakeTask) SetPlacementFailure(string) {}

// ToMimirEntity returns nil.
func (t *fakeTask) ToMimirEntity() *placement.Entity {
	return nil
}

// NeedsSpread returns false.
func (t *fakeTask) NeedsSpread() bool {
	return false
}

// PreferredHost returns empty string.
func (t *fakeTask) PreferredHost() string {
	return ""
}

// GetPlacementNeeds returns constraint of fakeTask.
func (t *fakeTask) GetPlacementNeeds() PlacementNeeds {
	return PlacementNeeds{
		Constraint: t.constraint,
	}
}

// GetResmgrTaskV0 returns resmgr task of fakeTask.
func (t *fakeTask) GetResmgrTaskV0() *resmgr.Task {
	return &resmgr.Task{}
}

func getFakeLabelConstraint(key, value string) *peloton_api_v0_task.Constraint {
	return &peloton_api_v0_task.Constraint{
		Type: peloton_api_v0_task.Constraint_LABEL_CONSTRAINT,
		LabelConstraint: &peloton_api_v0_task.LabelConstraint{
			Kind:      peloton_api_v0_task.LabelConstraint_HOST,
			Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
			Label: &peloton_api_v0_peloton.Label{
				Key:   key,
				Value: value,
			},
		},
	}
}

func getFakeOrConstraint(key, value string) *peloton_api_v0_task.Constraint {
	return &peloton_api_v0_task.Constraint{
		Type: peloton_api_v0_task.Constraint_OR_CONSTRAINT,
		OrConstraint: &peloton_api_v0_task.OrConstraint{
			Constraints: []*peloton_api_v0_task.Constraint{
				getFakeLabelConstraint(key, value),
			},
		},
	}
}

func getFakeAndConstraint(key, value string) *peloton_api_v0_task.Constraint {
	return &peloton_api_v0_task.Constraint{
		Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
		AndConstraint: &peloton_api_v0_task.AndConstraint{
			Constraints: []*peloton_api_v0_task.Constraint{
				getFakeLabelConstraint(key, value),
			},
		},
	}
}
