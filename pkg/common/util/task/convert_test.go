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

package task

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
)

func TestGetTaskType(t *testing.T) {
	tt := []struct {
		cfg      *task.TaskConfig
		jobType  job.JobType
		taskType resmgr.TaskType
	}{
		{
			cfg: &task.TaskConfig{
				Volume: &task.PersistentVolumeConfig{},
			},
			jobType:  job.JobType_SERVICE,
			taskType: resmgr.TaskType_STATEFUL,
		},
		{
			cfg:      &task.TaskConfig{},
			jobType:  job.JobType_BATCH,
			taskType: resmgr.TaskType_BATCH,
		},
		{
			cfg:      &task.TaskConfig{},
			jobType:  job.JobType_SERVICE,
			taskType: resmgr.TaskType_STATELESS,
		},
	}

	for _, test := range tt {
		assert.Equal(t, test.taskType, getTaskType(test.cfg, test.jobType))
	}
}

func TestConvertTaskToResMgrTask(t *testing.T) {
	jobID := peloton.JobID{Value: uuid.New()}
	taskInfos := []*task.TaskInfo{
		{
			InstanceId: 0,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_RUNNING,
				Host:  "hostname",
			},
		},
		{
			InstanceId: 1,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_INITIALIZED,
			},
		},
		{
			InstanceId: 2,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_LAUNCHED,
			},
		},
		{
			InstanceId: 3,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_STARTING,
			},
		},
		{
			InstanceId: 4,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_FAILED,
				Host:  "hostname",
			},
		},
		{
			InstanceId: 5,
			JobId:      &jobID,
			Config: &task.TaskConfig{
				Ports: []*task.PortConfig{{Name: "http", Value: 0}},
			},
			Runtime: &task.RuntimeInfo{
				State: task.TaskState_SUCCEEDED,
				Host:  "hostname",
			},
		},
	}

	jobConfig := &job.JobConfig{
		SLA:               &job.SlaConfig{},
		PlacementStrategy: job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
	}
	for _, taskInfo := range taskInfos {
		rmTask := ConvertTaskToResMgrTask(taskInfo, jobConfig)
		assert.Equal(t, taskInfo.JobId.Value, rmTask.JobId.Value)
		assert.Equal(t, uint32(len(taskInfo.Config.Ports)), rmTask.NumPorts)
		taskState := taskInfo.Runtime.GetState()
		if taskState == task.TaskState_LAUNCHED ||
			taskState == task.TaskState_STARTING ||
			taskState == task.TaskState_RUNNING {
			assert.Equal(t, taskInfo.GetRuntime().GetHost(), rmTask.GetHostname())
		} else {
			assert.Empty(t, rmTask.GetHostname())
		}
		assert.Equal(
			t,
			job.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
			rmTask.GetPlacementStrategy())
	}
}

func TestConvertToResMgrGangs(t *testing.T) {
	jobConfig := &job.JobConfig{
		SLA: &job.SlaConfig{
			MinimumRunningInstances: 2,
		},
	}

	gangs := ConvertToResMgrGangs(
		[]*task.TaskInfo{
			{
				InstanceId: 0,
			},
			{
				InstanceId: 1,
			},
			{
				InstanceId: 2,
			},
			{
				InstanceId: 3,
			}},
		jobConfig)

	assert.Len(t, gangs, 3)
}

func TestConvertTaskToResMgrTaskPreemptible(t *testing.T) {
	tt := []struct {
		name        string
		taskInfo    *task.TaskInfo
		jobConfig   *job.JobConfig
		preemptible bool
	}{
		{
			name: "no override should use job value: false",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_INVALID,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: false},
			},
			preemptible: false,
		},
		{
			name: "no override should use job value: true",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_INVALID,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: true},
			},
			preemptible: true,
		},
		{
			name: "same override should use job value: true",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_PREEMPTIBLE,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: true},
			},
			preemptible: true,
		},
		{
			name: "task override should use task value: true",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_PREEMPTIBLE,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: false},
			},
			preemptible: true,
		},
		{
			name: "same override should use job value: false",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: false},
			},
			preemptible: false,
		},
		{
			name: "task override should use task value: false",
			taskInfo: &task.TaskInfo{
				Config: &task.TaskConfig{
					PreemptionPolicy: &task.PreemptionPolicy{
						Type: task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE,
					},
				},
			},
			jobConfig: &job.JobConfig{
				SLA: &job.SlaConfig{Preemptible: true},
			},
			preemptible: false,
		},
	}

	for _, test := range tt {
		r := ConvertTaskToResMgrTask(test.taskInfo, test.jobConfig)
		assert.Equal(t, test.preemptible, r.Preemptible, test.name)
	}
}
