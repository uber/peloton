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

package objects

import (
	"context"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type JobUpdateEventsObjectTestSuite struct {
	suite.Suite
	updateID *peloton.UpdateID
}

func (s *JobUpdateEventsObjectTestSuite) SetupTest() {
	setupTestStore()
	s.updateID = &peloton.UpdateID{Value: uuid.New()}
}

func TestJobUpdateEventsObjectTestSuite(t *testing.T) {
	suite.Run(t, new(JobUpdateEventsObjectTestSuite))
}

func (s *JobUpdateEventsObjectTestSuite) TestAddJobUpdateEvents() {
	db := NewJobUpdateEventsOps(testStore)
	ctx := context.Background()

	// Cassandra timestamp accuracy is microsecond.
	// If Creation happens within the same microsecond, there is no guarantee of order.
	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_INITIALIZED))
	time.Sleep(10 * time.Microsecond)
	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_ROLLING_FORWARD))
	time.Sleep(10 * time.Microsecond)
	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_SUCCEEDED))

	workflowEvents, err := db.GetAll(ctx, s.updateID)
	s.NoError(err)
	s.Len(workflowEvents, 3)

	for _, workflow := range workflowEvents {
		s.Equal(workflow.GetType(), stateless.WorkflowType_WORKFLOW_TYPE_UPDATE)
	}

	// test the events are in descending order
	s.Equal(workflowEvents[0].GetState(), stateless.WorkflowState_WORKFLOW_STATE_SUCCEEDED)
	s.Equal(workflowEvents[1].GetState(), stateless.WorkflowState_WORKFLOW_STATE_ROLLING_FORWARD)
	s.Equal(workflowEvents[2].GetState(), stateless.WorkflowState_WORKFLOW_STATE_INITIALIZED)
}

func (s *JobUpdateEventsObjectTestSuite) TestDeleteJobUpdateEvents() {
	db := NewJobUpdateEventsOps(testStore)
	ctx := context.Background()

	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_INITIALIZED))
	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_ROLLING_FORWARD))
	s.NoError(db.Create(ctx, s.updateID, models.WorkflowType_UPDATE, update.State_SUCCEEDED))

	s.NoError(db.Delete(ctx, s.updateID))

	workflowEvents, err := db.GetAll(ctx, s.updateID)
	s.NoError(err)
	s.Empty(workflowEvents)
}
