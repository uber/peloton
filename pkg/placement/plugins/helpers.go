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
	"encoding/json"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	peloton_api_v0_peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	peloton_api_v0_task "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/pkg/common"
)

// TasksByPlacementNeeds is a group of a list of tasks that have the
// same PlacementNeeds.
type TasksByPlacementNeeds struct {
	PlacementNeeds PlacementNeeds
	Tasks          []int
}

// GroupByPlacementNeeds groups the given tasks into a list of
// TasksByPlacementNeeds.
func GroupByPlacementNeeds(tasks []Task, config *Config) []*TasksByPlacementNeeds {
	groupByPlacementNeeds := map[string]*TasksByPlacementNeeds{}
	for i, task := range tasks {
		needs := task.GetPlacementNeeds()
		// TODO: This is ok for now since this is the only place getting constraint
		// from task placement needs; once host pool is enabled,
		// host pool constraint upsert should be moved into task.GetPlacementNeeds().
		if config.UseHostPool {
			var poolID string
			if config.TaskType == resmgr.TaskType_BATCH {
				if task.GetResmgrTaskV0().GetPreemptible() {
					poolID = common.SharedHostPoolID
				} else {
					poolID = common.BatchReservedHostPoolID
				}
			} else if config.TaskType == resmgr.TaskType_STATELESS {
				poolID = common.StatelessHostPoolID
			}

			hostPoolConstraint := &peloton_api_v0_task.Constraint{
				Type: peloton_api_v0_task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &peloton_api_v0_task.LabelConstraint{
					Kind:      peloton_api_v0_task.LabelConstraint_HOST,
					Condition: peloton_api_v0_task.LabelConstraint_CONDITION_EQUAL,
					Label: &peloton_api_v0_peloton.Label{
						Key: common.HostPoolKey,
						// TODO: Need a different way to annotate required
						// host pool when supporting more host pool types
						// other than batch and stateless.
						Value: poolID,
					},
					Requirement: 1,
				},
			}
			err := needs.upsertConstraint(hostPoolConstraint)
			if err != nil {
				log.WithFields(log.Fields{
					"task_type": config.TaskType.String(),
				}).Error("failed to upsert host pool constraint")
			}
		}

		key := needs.ToMapKey()
		if _, found := groupByPlacementNeeds[key]; !found {
			groupByPlacementNeeds[key] = &TasksByPlacementNeeds{
				PlacementNeeds: needs,
				Tasks:          []int{},
			}
		}
		groupByPlacementNeeds[key].Tasks = append(groupByPlacementNeeds[key].Tasks, i)
	}
	result := []*TasksByPlacementNeeds{}
	for _, group := range groupByPlacementNeeds {
		result = append(result, group)
	}
	return result
}

// upsertConstraint combines given constraint with existing constraint in
// placement needs as an new constraint, inserts and updates it into PlacementNeeds.
// TODO: It assumes PlacementNeeds.Constraint is *peloton_api_v0_task.Constraint,
//  though it is actually defined as interface{}
func (needs *PlacementNeeds) upsertConstraint(newConstraint *peloton_api_v0_task.Constraint) error {
	// Validate given new constraint
	if newConstraint == nil ||
		newConstraint.GetType() == peloton_api_v0_task.Constraint_UNKNOWN_CONSTRAINT {
		return nil
	}

	// Convert placement constraint to strong typed *peloton_api_v0_task.Constraint.
	constraint, ok := needs.Constraint.(*peloton_api_v0_task.Constraint)
	if !ok {
		return errors.Errorf(
			"failed to convert constraint %v to "+
				"*peloton_api_v0_task.Constraint",
			needs.Constraint,
		)
	}

	// If placement constraint is empty, use given new constraint as placement constraint.
	if constraint.GetType() == peloton_api_v0_task.Constraint_UNKNOWN_CONSTRAINT {
		needs.Constraint = newConstraint
		return nil
	}

	// Combine placement constraint and given new constraint as an AndConstraint.
	needs.Constraint = &peloton_api_v0_task.Constraint{
		Type: peloton_api_v0_task.Constraint_AND_CONSTRAINT,
		AndConstraint: &peloton_api_v0_task.AndConstraint{
			Constraints: []*peloton_api_v0_task.Constraint{
				constraint,
				newConstraint,
			},
		},
	}
	return nil
}

// ToMapKey returns a stringified version of the placement needs.
// It zeroes out HostHints first to make sure
func (needs PlacementNeeds) ToMapKey() string {
	needs.HostHints = nil
	content, _ := json.Marshal(needs)
	return string(content)
}
