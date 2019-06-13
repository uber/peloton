// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law orupd agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objects

import (
	"context"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type PodEventsObjectTestSuite struct {
	suite.Suite
}

func (s *PodEventsObjectTestSuite) SetupTest() {
	setupTestStore()
}

func TestPodEventsObjectSuite(t *testing.T) {
	suite.Run(t, new(PodEventsObjectTestSuite))
}

func (s *PodEventsObjectTestSuite) TestAddPodEvents() {
	db := NewPodEventsOps(testStore)
	ctx := context.Background()

	hostName := "mesos-slave-01"
	testTable := []struct {
		mesosTaskID        string
		prevMesosTaskID    string
		desiredMesosTaskID string
		actualState        task.TaskState
		goalState          task.TaskState
		jobID              peloton.JobID
		healthy            task.HealthState
		returnErr          bool
	}{
		{
			mesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-" +
				"1a37d6ee-5da1-4d7a-9e91-91185990fbb1",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-test",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_HEALTH_UNKNOWN,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: "incorrect-jobID"},
			healthy:            task.HealthState_HEALTH_UNKNOWN,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2",
			prevMesosTaskID:    "",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2",
			actualState:        task.TaskState_RUNNING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: "incorrect-jobID"},
			healthy:            task.HealthState_HEALTHY,
			returnErr:          true,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-0",
			desiredMesosTaskID: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          false,
		},
		{
			mesosTaskID:        "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1",
			prevMesosTaskID:    "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-0",
			desiredMesosTaskID: "",
			actualState:        task.TaskState_PENDING,
			goalState:          task.TaskState_RUNNING,
			jobID:              peloton.JobID{Value: uuid.NewRandom().String()},
			healthy:            task.HealthState_DISABLED,
			returnErr:          false,
		},
	}

	for _, tt := range testTable {
		runtime := &task.RuntimeInfo{
			StartTime:      time.Now().String(),
			CompletionTime: time.Now().String(),
			State:          tt.actualState,
			GoalState:      tt.goalState,
			Healthy:        tt.healthy,
			Host:           hostName,
			MesosTaskId: &mesos.TaskID{
				Value: &tt.mesosTaskID,
			},
			PrevMesosTaskId: &mesos.TaskID{
				Value: &tt.prevMesosTaskID,
			},
			DesiredMesosTaskId: &mesos.TaskID{
				Value: &tt.desiredMesosTaskID,
			},
		}
		jobIDToAdd := &tt.jobID
		err := db.Create(ctx, jobIDToAdd, 0, runtime)
		if tt.returnErr {
			s.Error(err)
		} else {
			s.NoError(err)
		}
	}
}

func (s *PodEventsObjectTestSuite) TestGetPodEvents() {
	db := NewPodEventsOps(testStore)
	dummyJobID := &peloton.JobID{Value: "dummy id"}
	_, err := db.GetAll(
		context.Background(),
		dummyJobID.GetValue(),
		0)
	s.Error(err)

	jobID := &peloton.JobID{Value: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06"}
	mesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	prevMesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-1"
	desiredMesosTaskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	runtime := &task.RuntimeInfo{
		StartTime:      time.Now().String(),
		CompletionTime: time.Now().String(),
		State:          task.TaskState_RUNNING,
		GoalState:      task.TaskState_SUCCEEDED,
		Healthy:        task.HealthState_HEALTHY,
		Host:           "mesos-slave-01",
		Message:        "",
		Reason:         "",
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		PrevMesosTaskId: &mesos.TaskID{
			Value: &prevMesosTaskID,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &desiredMesosTaskID,
		},
		ConfigVersion:        3,
		DesiredConfigVersion: 4,
	}

	db.Create(context.Background(), jobID, 0, runtime)
	podEvents, err := db.GetAll(
		context.Background(),
		jobID.GetValue(),
		0,
		"7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2")
	s.Equal(len(podEvents), 1)
	s.NoError(err)

	mesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3"
	prevMesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-2"
	desiredMesosTaskID = "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3"
	runtime = &task.RuntimeInfo{
		StartTime:      time.Now().String(),
		CompletionTime: time.Now().String(),
		State:          task.TaskState_RUNNING,
		GoalState:      task.TaskState_SUCCEEDED,
		Healthy:        task.HealthState_HEALTHY,
		Host:           "mesos-slave-01",
		Message:        "",
		Reason:         "",
		MesosTaskId: &mesos.TaskID{
			Value: &mesosTaskID,
		},
		PrevMesosTaskId: &mesos.TaskID{
			Value: &prevMesosTaskID,
		},
		DesiredMesosTaskId: &mesos.TaskID{
			Value: &desiredMesosTaskID,
		},
		ConfigVersion:        3,
		DesiredConfigVersion: 4,
	}

	db.Create(context.Background(), jobID, 0, runtime)
	podEvents, err = db.GetAll(
		context.Background(),
		jobID.GetValue(),
		0)
	s.Equal(len(podEvents), 1)
	s.NoError(err)

	podEvents, err = db.GetAll(
		context.Background(),
		jobID.GetValue(),
		0,
		"7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-3")
	s.Equal(len(podEvents), 1)
	s.NoError(err)
}
