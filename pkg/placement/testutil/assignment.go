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

package testutil

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/testutil/v0"
)

// SetupAssignment creates an assignment.
func SetupAssignment(deadline time.Time, maxRounds int) *models_v0.Assignment {
	resmgrTask := v0_testutil.SetupRMTask()
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	task := models_v0.NewTask(resmgrGang, resmgrTask, deadline, deadline, maxRounds)
	return models_v0.NewAssignment(task)
}
