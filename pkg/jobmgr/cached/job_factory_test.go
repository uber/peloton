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

package cached

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	pbupdate "github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// TestInitJobFactory tests initialization of the job factory
func TestInitJobFactory(t *testing.T) {
	f := InitJobFactory(nil, nil, nil, nil, nil, tally.NoopScope, nil)
	assert.NotNil(t, f)
}

// TestAddAndGetAndClearJob tests adding, getting and
// clearing of a job in the factory.
func TestAddAndGetAndClearJob(t *testing.T) {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	f := &jobFactory{
		jobs:    map[string]*job{},
		running: true,
	}

	assert.Nil(t, f.GetJob(jobID))

	j := f.AddJob(jobID)

	assert.NotNil(t, j)

	assert.Equal(t, j, f.GetJob(jobID))
	assert.Equal(t, j, f.AddJob(jobID))
	assert.Equal(t, 1, len(f.GetAllJobs()))

	f.ClearJob(jobID)
	assert.Equal(t, 0, len(f.GetAllJobs()))
	assert.Nil(t, f.GetJob(jobID))
}

// TestStartStop tests starting and then stopping the factory.
func TestStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	f := &jobFactory{
		jobs: map[string]*job{},
		mtx:  NewMetrics(tally.NoopScope),
	}

	f.Start()

	assert.True(t, f.running)
	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f595c"}
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{}
	taskInfos[1] = &pbtask.TaskInfo{}
	taskInfos[2] = &pbtask.TaskInfo{}

	j := f.AddJob(jobID)
	j.ReplaceTasks(taskInfos, false)
	assert.Equal(t, 3, len(j.GetAllTasks()))

	f.Stop()
	assert.Nil(t, f.GetJob(jobID))
}

// TestPublishMetrics tests publishing metrics from the job factory.
func TestPublishMetrics(t *testing.T) {
	testScope := tally.NewTestScope("", nil)
	f := &jobFactory{
		jobs:        map[string]*job{},
		mtx:         NewMetrics(testScope),
		running:     true,
		taskMetrics: NewTaskMetrics(testScope),
	}

	jobID := &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f5222"}
	taskInfos := make(map[uint32]*pbtask.TaskInfo)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Host:      "host1",
			Healthy:   pbtask.HealthState_HEALTHY,
		},
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Host:      "host1",
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	taskInfos[2] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_DELETED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	j := f.AddJob(jobID)
	cachedJob := j.(*job)
	cachedJob.config = &cachedConfig{
		placementStrategy: pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
		jobType:           pbjob.JobType_SERVICE,
		sla: &pbjob.SlaConfig{
			MaximumUnavailableInstances: 2,
		},
	}
	cachedJob.runtime = &pbjob.RuntimeInfo{}
	cachedJob.jobType = pbjob.JobType_SERVICE
	j.ReplaceTasks(taskInfos, true)

	jobID = &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f111"}
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_SUCCEEDED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Host:      "host2",
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	taskInfos[2] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_INITIALIZED,
			GoalState: pbtask.TaskState_DELETED,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	taskInfos[3] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_UNKNOWN,
			GoalState: pbtask.TaskState_DELETED,
			Revision:  &peloton.ChangeLog{Version: 1},
		},
	}

	j = f.AddJob(jobID)
	cachedJob = j.(*job)
	cachedJob.config = &cachedConfig{
		placementStrategy: pbjob.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD_JOB,
		sla: &pbjob.SlaConfig{
			MaximumUnavailableInstances: 0,
		},
	}
	cachedJob.runtime = &pbjob.RuntimeInfo{}
	j.ReplaceTasks(taskInfos, true)

	j.AddWorkflow(&peloton.UpdateID{
		Value: "update0",
	})

	stateCount := f.publishMetrics()
	assert.Equal(t,
		stateCount[pbtask.TaskState_RUNNING][pbtask.TaskState_RUNNING],
		2)
	assert.Equal(t,
		stateCount[pbtask.TaskState_PENDING][pbtask.TaskState_RUNNING],
		1)
	assert.Equal(t,
		stateCount[pbtask.TaskState_INITIALIZED][pbtask.TaskState_DELETED],
		2)
	assert.Equal(t,
		stateCount[pbtask.TaskState_PENDING][pbtask.TaskState_SUCCEEDED],
		1)

	sm, ok := testScope.Snapshot().Gauges()["mean_spread_quotient+"]
	assert.True(t, ok)
	assert.Equal(t, 1.5, sm.Value())
	f.ClearJob(jobID)

	// sla violation check
	jobID = &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f112"}
	taskInfos = make(map[uint32]*pbtask.TaskInfo, 2)
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	taskInfos[1] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_PENDING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTH_UNKNOWN,
		},
	}
	j = f.AddJob(jobID)
	cachedJob = j.(*job)
	cachedJob.config = &cachedConfig{
		jobType: pbjob.JobType_SERVICE,
		sla: &pbjob.SlaConfig{
			MaximumUnavailableInstances: 1,
		},
	}
	cachedJob.jobType = pbjob.JobType_SERVICE
	cachedJob.runtime = &pbjob.RuntimeInfo{}
	j.ReplaceTasks(taskInfos, true)

	f.publishMetrics()
	svm, ok := testScope.Snapshot().Gauges()["sla_violated_jobs+"]
	assert.True(t, ok)
	f.ClearJob(jobID)

	// sla violation check for a job with no unavailable instances
	jobID = &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f115"}
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTHY,
		},
	}
	j = f.AddJob(jobID)
	cachedJob = j.(*job)
	cachedJob.config = &cachedConfig{
		jobType: pbjob.JobType_SERVICE,
		sla: &pbjob.SlaConfig{
			MaximumUnavailableInstances: 0,
		},
	}
	cachedJob.jobType = pbjob.JobType_SERVICE
	cachedJob.runtime = &pbjob.RuntimeInfo{}
	j.ReplaceTasks(taskInfos, true)

	f.publishMetrics()
	svm, ok = testScope.Snapshot().Gauges()["sla_violated_jobs+"]
	assert.True(t, ok)
	assert.Equal(t, 0.0, svm.Value())
	f.ClearJob(jobID)

	// sla violation check for a job with ongoing update
	jobID = &peloton.JobID{Value: "3c8a3c3e-71e3-49c5-9aed-2929823f115"}
	taskInfos[0] = &pbtask.TaskInfo{
		Runtime: &pbtask.RuntimeInfo{
			State:     pbtask.TaskState_RUNNING,
			GoalState: pbtask.TaskState_RUNNING,
			Revision:  &peloton.ChangeLog{Version: 1},
			Healthy:   pbtask.HealthState_HEALTHY,
		},
	}
	j = f.AddJob(jobID)
	testUpdate := &peloton.UpdateID{
		Value: "update0",
	}
	cachedJob = j.(*job)
	cachedJob.config = &cachedConfig{
		jobType: pbjob.JobType_SERVICE,
		sla: &pbjob.SlaConfig{
			MaximumUnavailableInstances: 0,
		},
	}
	cachedJob.jobType = pbjob.JobType_SERVICE
	cachedJob.runtime = &pbjob.RuntimeInfo{}
	cachedJob.workflows[testUpdate.GetValue()] = &update{
		workflowType: models.WorkflowType_UPDATE,
		state:        pbupdate.State_ROLLING_FORWARD,
	}
	j.ReplaceTasks(taskInfos, true)

	f.publishMetrics()
	svm, ok = testScope.Snapshot().Gauges()["sla_violated_jobs+"]
	assert.True(t, ok)
	assert.Equal(t, 0.0, svm.Value())
	f.ClearJob(jobID)
}

// BenchmarkPublishMetrics benchmarks the time needed to call publishMetrics
// when other goroutines are competing for locks with jobFactory.AddJob and
// jobFactory.GetJob
func BenchmarkPublishMetrics(b *testing.B) {
	b.StopTimer()

	numberOfJob := 600
	numberOfTaskPerJob := 1400
	f := createJobFactoryWithMockTasks(numberOfJob, numberOfTaskPerJob)

	stopChan := make(chan struct{})
	numberOfPeriodicalJobAddGetWorker := 5
	// readyWg to make sure the benchmark enters goroutine that simulates add/get
	// job before running publishMetrics
	readyWg := sync.WaitGroup{}
	// finishWg to make sure all go routine exists before test exists
	finishWg := sync.WaitGroup{}
	readyWg.Add(numberOfPeriodicalJobAddGetWorker)
	finishWg.Add(numberOfPeriodicalJobAddGetWorker)

	jobIDs := createRandomJobIds(numberOfJob)
	// simulate the lock contention due to add/get job in other goroutines
	periodicalJobAddGet := func() {
		readyWg.Done()
		ticker := time.Tick(time.Millisecond)
		for {
			select {
			case <-ticker:
				go f.AddJob(jobIDs[rand.Intn(len(jobIDs))])
				go f.GetJob(jobIDs[rand.Intn(len(jobIDs))])
			case <-stopChan:
				finishWg.Done()
				return
			}
		}
	}

	for i := 0; i < numberOfPeriodicalJobAddGetWorker; i++ {
		go periodicalJobAddGet()
	}
	readyWg.Wait()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.publishMetrics()
	}

	b.StopTimer()
	close(stopChan)
	finishWg.Wait()
}

// BenchmarkAddJobWhilePublishingMetrics benchmarks jobFactory.AddJob
// while publishMetrics is going on
func BenchmarkAddJobWhilePublishingMetrics(b *testing.B) {
	b.StopTimer()

	numberOfJob := 600
	numberOfTaskPerJob := 1400
	f := createJobFactoryWithMockTasks(numberOfJob, numberOfTaskPerJob)

	stopChan := make(chan struct{})
	// readyWg to make sure the benchmark enters publishMetrics goroutine before
	// benchmarking
	readyWg := sync.WaitGroup{}
	// finishWg to make sure all go routine exists before test exists
	finishWg := sync.WaitGroup{}
	readyWg.Add(1)
	finishWg.Add(1)

	jobIDs := createRandomJobIds(numberOfJob)
	// simulate the lock contention due to add/get job in other goroutines
	publishMetrics := func() {
		readyWg.Done()
		for {
			select {
			case <-stopChan:
				finishWg.Done()
				return
			default:
				f.publishMetrics()
			}
		}
	}

	go publishMetrics()
	readyWg.Wait()

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.AddJob(jobIDs[rand.Intn(len(jobIDs))])
	}

	b.StopTimer()
	close(stopChan)
	finishWg.Wait()
}

// BenchmarkAddJob benchmarks jobFactory.AddJob
// without contention
func BenchmarkAddJob(b *testing.B) {
	numberOfJob := 600
	f := &jobFactory{
		jobs: map[string]*job{},
	}

	jobIDs := createRandomJobIds(numberOfJob)
	for i := 0; i < b.N; i++ {
		f.AddJob(jobIDs[rand.Intn(len(jobIDs))])
	}
}

func createJobFactoryWithMockTasks(
	numberOfJob int,
	numberOfTaskPerJob int) *jobFactory {

	f := &jobFactory{
		jobs:    map[string]*job{},
		mtx:     NewMetrics(tally.NoopScope),
		running: true,
	}

	// populate job factory with mock jobs and tasks
	for i := 0; i < numberOfJob; i++ {
		jobID := &peloton.JobID{Value: uuid.New()}
		cachedJob := f.AddJob(jobID)
		cachedJob.(*job).jobType = pbjob.JobType_SERVICE
		for j := uint32(0); j < uint32(numberOfTaskPerJob); j++ {
			cachedTask := newTask(jobID, j, f, cachedJob.GetJobType())
			// randomly populate the states
			cachedTask.runtime = &pbtask.RuntimeInfo{
				State:     pbtask.TaskState(uint32(rand.Intn(len(pbtask.TaskState_name)))),
				GoalState: pbtask.TaskState(uint32(rand.Intn(len(pbtask.TaskState_name)))),
			}
			cachedJob.(*job).tasks[j] = cachedTask
		}
	}
	return f
}

func createRandomJobIds(n int) []*peloton.JobID {
	result := make([]*peloton.JobID, n, n)
	for i := 0; i < n; i++ {
		result[i] = &peloton.JobID{Value: uuid.New()}
	}
	return result
}
