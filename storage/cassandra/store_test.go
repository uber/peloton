// +build !unit

package cassandra

import (
	"context"
	"fmt"
	"testing"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/storage"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	_resPoolOwner = "teamPeloton"
)

type CassandraStoreTestSuite struct {
	suite.Suite
	store *Store
}

// NOTE(gabe): using this method of setup is definitely less elegant
// than a SetupTest() and TearDownTest() helper functions of suite,
// but has the unfortunate sideeffect of making test runs on my MBP go
// from ~3m wall to 10m wall. For now, keep using init() until a fix
// for this is found

var store *Store

func init() {
	conf := MigrateForTest()
	var err error
	store, err = NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}

func TestCassandraStore(t *testing.T) {
	suite.Run(t, new(CassandraStoreTestSuite))
}

func (suite *CassandraStoreTestSuite) TestQueryJob() {
	var jobStore storage.JobStore
	jobStore = store

	var originalJobs []*job.JobConfig
	var jobIDs []*peloton.JobID
	var records = 3

	var keys0 = []string{"test0", "test1", "test2", "test3"}
	var vals0 = []string{"testValue0", "testValue1", "testValue2", "testValue3"}

	var keys1 = []string{"key0", "key1", "key2", "key3"}
	var vals1 = []string{"valX0", "valX1", "valX2", "valX3"}
	keyCommon := "keyX"
	valCommon := "valX"

	// Create 3 jobs with different labels and a common label
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("TestQueryJob%d", i)}
		jobIDs = append(jobIDs, &jobID)
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		var labels = mesos.Labels{
			Labels: []*mesos.Label{
				{Key: &keys0[i], Value: &vals0[i]},
				{Key: &keys1[i], Value: &vals1[i]},
				{Key: &keyCommon, Value: &valCommon},
			},
		}
		var jobConfig = job.JobConfig{
			Name:          fmt.Sprintf("TestJob_%d", i),
			OwningTeam:    "owner",
			LdapGroups:    []string{"money", "team6", "gign"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
			Labels:        &labels,
			Description:   fmt.Sprintf("A test job with awesome keyword%v keytest%v", i, i),
		}
		originalJobs = append(originalJobs, &jobConfig)
		err := jobStore.CreateJob(&jobID, &jobConfig, "uber")
		suite.NoError(err)
	}
	// Run the following query to trigger rebuild the lucene index
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobsTable).Where("expr(jobs_index, '{refresh:true}')")
	_, err := store.DataStore.Execute(context.Background(), stmt)
	suite.NoError(err)

	// query by common label should return all jobs
	result1, err := jobStore.Query(&mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keyCommon, Value: &valCommon},
		},
	}, nil)
	suite.NoError(err)
	suite.Equal(records, len(result1))
	for i := 0; i < records; i++ {
		suite.Equal(fmt.Sprintf("TestJob_%d", i), result1[jobIDs[i].Value].Name)
	}

	// query by specific label returns one job
	for i := 0; i < records; i++ {
		result1, err := jobStore.Query(&mesos.Labels{
			Labels: []*mesos.Label{
				{Key: &keys0[i], Value: &vals0[i]},
				{Key: &keys1[i], Value: &vals1[i]},
			},
		}, nil)
		suite.NoError(err)
		suite.Equal(1, len(result1))
		suite.Equal(fmt.Sprintf("TestJob_%d", i), result1[jobIDs[i].Value].Name)
	}

	// query for non-exist label return nothing
	var other = "other"
	result1, err = jobStore.Query(&mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys0[0], Value: &other},
			{Key: &keys1[1], Value: &vals1[0]},
		},
	}, nil)
	suite.NoError(err)
	suite.Equal(0, len(result1))

	// Test query with keyword
	result1, err = jobStore.Query(nil, []string{"team6", "test", "awesome"})
	suite.NoError(err)
	suite.Equal(3, len(result1))

	result1, err = jobStore.Query(nil, []string{"team6", "test", "awesome", "keytest1"})
	suite.NoError(err)
	suite.Equal(1, len(result1))

	result1, err = jobStore.Query(nil, []string{"team6", "test", "awesome", "nonexistkeyword"})
	suite.NoError(err)
	suite.Equal(0, len(result1))

	// Query with both labels and keyword
	result1, err = jobStore.Query(&mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys0[0], Value: &vals0[0]},
		},
	}, []string{"team6", "test", "awesome"})
	suite.NoError(err)
	suite.Equal(1, len(result1))
}

func (suite *CassandraStoreTestSuite) TestCreateGetJobConfig() {
	var jobStore storage.JobStore
	jobStore = store
	var originalJobs []*job.JobConfig
	var records = 1
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("TestCreateGetJobConfig%d", i)}
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000 + uint32(i),
			},
		}
		var labels = mesos.Labels{
			Labels: []*mesos.Label{
				{Key: &keys[0], Value: &vals[0]},
				{Key: &keys[1], Value: &vals[1]},
				{Key: &keys[2], Value: &vals[2]},
			},
		}
		// Add some special label to job0 and job1
		if i < 2 {
			labels.Labels = append(labels.Labels,
				&mesos.Label{Key: &keys[3], Value: &vals[3]})
		}

		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}
		var jobconfig = job.JobConfig{
			Name:          fmt.Sprintf("TestJob_%d", i),
			OwningTeam:    owner,
			LdapGroups:    []string{"money", "team6", "otto"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
			Labels:        &labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := jobStore.CreateJob(&jobID, &jobconfig, "uber")
		suite.NoError(err)

		// Create job with same job id would be no op
		jobconfig.Labels = nil
		jobconfig.Name = "random"
		err = jobStore.CreateJob(&jobID, &jobconfig, "uber2")
		suite.Error(err)

		var jobconf *job.JobConfig
		jobconf, err = jobStore.GetJobConfig(&jobID)
		suite.NoError(err)
		suite.Equal(jobconf.Name, fmt.Sprintf("TestJob_%d", i))
		suite.Equal(len((*(jobconf.Labels)).Labels), 4)
	}
}

func (suite *CassandraStoreTestSuite) TestFrameworkInfo() {
	var frameworkStore storage.FrameworkInfoStore
	frameworkStore = store
	err := frameworkStore.SetMesosFrameworkID("framework1", "12345")
	suite.NoError(err)
	var frameworkID string
	frameworkID, err = frameworkStore.GetFrameworkID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetFrameworkID("framework2")
	suite.Error(err)

	err = frameworkStore.SetMesosStreamID("framework1", "s-12345")
	suite.NoError(err)

	frameworkID, err = frameworkStore.GetFrameworkID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetMesosStreamID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "s-12345")
}

func (suite *CassandraStoreTestSuite) TestAddTasks() {
	var jobStore storage.JobStore
	jobStore = store
	var taskStore storage.TaskStore
	taskStore = store
	var nJobs = 3
	var nTasks = uint32(3)
	var jobIDs []*peloton.JobID
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("TestAddTasks_%d", i)}
		jobIDs = append(jobIDs, &jobID)
		jobConfig := createJobConfig()
		jobConfig.Name = fmt.Sprintf("TestAddTasks_%d", i)
		jobs = append(jobs, jobConfig)
		err := jobStore.CreateJob(&jobID, jobConfig, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := uint32(0); j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			taskInfo.Runtime.State = task.TaskState(j)
			err = taskStore.CreateTask(&jobID, j, taskInfo, "test")
			suite.NoError(err)
			// Create same task should error
			err = taskStore.CreateTask(&jobID, j, taskInfo, "test")
			suite.Error(err)
			err = taskStore.UpdateTask(taskInfo)
			suite.NoError(err)
		}
	}
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.JobId.Value, jobIDs[i].Value)
		}
	}

	// List tasks for a job in certain state
	// TODO: change the task.runtime.State to string type

	// Update task
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			task.Runtime.Host = fmt.Sprintf("compute_%d", i)
			err := store.UpdateTask(task)
			suite.NoError(err)
		}
	}
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.Runtime.Host, fmt.Sprintf("compute_%d", i))
		}
	}

	for i := 0; i < nJobs; i++ {
		for j := 0; j < int(nTasks); j++ {
			taskID := fmt.Sprintf("%s-%d", jobIDs[i].Value, j)
			taskInfo, err := taskStore.GetTaskByID(taskID)
			suite.NoError(err)
			suite.Equal(taskInfo.JobId.Value, jobIDs[i].Value)
			suite.Equal(taskInfo.InstanceId, uint32(j))

			var taskMap map[uint32]*task.TaskInfo
			taskMap, err = taskStore.GetTaskForJob(jobIDs[i], uint32(j))
			suite.NoError(err)
			taskInfo = taskMap[uint32(j)]
			suite.Equal(taskInfo.JobId.Value, jobIDs[i].Value)
			suite.Equal(taskInfo.InstanceId, uint32(j))
		}
		// TaskID does not exist
	}
	task, err := taskStore.GetTaskByID("taskdoesnotexist")
	suite.Error(err)
	suite.Nil(task)
}

// TestCreateTasks ensures mysql task create batching works as expected.
func (suite *CassandraStoreTestSuite) TestCreateTasks() {
	jobTasks := map[string]int{
		"TestJob1": 10,
		"TestJob2": store.Conf.MaxBatchSize,
		"TestJob3": store.Conf.MaxBatchSize*3 + 10,
	}
	for jobID, nTasks := range jobTasks {
		var jobID = peloton.JobID{Value: jobID}
		var sla = job.SlaConfig{
			Priority:                22,
			MaximumRunningInstances: 3,
			Preemptible:             false,
		}
		var taskConfig = task.TaskConfig{
			Resource: &task.ResourceConfig{
				CpuLimit:    0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000,
			},
		}
		var jobConfig = job.JobConfig{
			Name:          jobID.Value,
			OwningTeam:    "team6",
			LdapGroups:    []string{"money", "team6", "otto"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
		}
		err := store.CreateJob(&jobID, &jobConfig, "uber")
		suite.NoError(err)

		// now, create a mess of tasks
		taskInfos := []*task.TaskInfo{}
		for j := 0; j < nTasks; j++ {
			var tID = fmt.Sprintf("%s-%d", jobID.Value, j)
			var taskInfo = task.TaskInfo{
				Runtime: &task.RuntimeInfo{
					TaskId: &mesos.TaskID{Value: &tID},
					State:  task.TaskState(j),
				},
				Config:     jobConfig.GetDefaultConfig(),
				InstanceId: uint32(j),
				JobId:      &jobID,
			}
			taskInfos = append(taskInfos, &taskInfo)
		}
		err = store.CreateTasks(&jobID, taskInfos, "test")
		suite.NoError(err)
	}

	// List all tasks by job, ensure they were created properly, and
	// have the right parent
	for jobID, nTasks := range jobTasks {
		job := peloton.JobID{Value: jobID}
		tasks, err := store.GetTasksForJob(&job)
		suite.NoError(err)
		suite.Equal(nTasks, len(tasks))
		for _, task := range tasks {
			suite.Equal(jobID, task.JobId.Value)
		}
	}
}

func (suite *CassandraStoreTestSuite) TestGetTasksByHostState() {
	var jobStore storage.JobStore
	jobStore = store
	var taskStore storage.TaskStore
	taskStore = store
	var nJobs = 2
	var nTasks = uint32(6)
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("TestGetTasksByHostState%d", i)}
		jobConfig := createJobConfig()
		jobConfig.InstanceCount = uint32(nTasks)
		jobs = append(jobs, jobConfig)
		err := jobStore.CreateJob(&jobID, jobConfig, "uber")
		suite.NoError(err)
		for j := uint32(0); j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			err = taskStore.CreateTask(&jobID, j, taskInfo, "test")
			suite.NoError(err)
			taskInfo.Runtime.State = task.TaskState(j)
			taskInfo.Runtime.Host = fmt.Sprintf("compute2-%d", j)
			err = taskStore.UpdateTask(taskInfo)
			suite.NoError(err)
		}
	}
	// GetTaskByState
	for j := 0; j < int(nTasks); j++ {
		jobID := peloton.JobID{Value: "TestGetTasksByHostState0"}
		tasks, err := store.GetTasksForJobAndState(
			&jobID, task.TaskState(j).String())
		suite.NoError(err)
		suite.Equal(len(tasks), 1)

		for tid, taskInfo := range tasks {
			suite.Equal(tid, uint32(j))
			suite.Equal(taskInfo.Runtime.Host, fmt.Sprintf("compute2-%d", j))
		}

		jobID = peloton.JobID{Value: "TestGetTasksByHostState1"}
		tasks, err = store.GetTasksForJobAndState(
			&jobID, task.TaskState(j).String())
		suite.NoError(err)

		for tid, taskInfo := range tasks {
			suite.Equal(tid, uint32(j))
			suite.Equal(taskInfo.Runtime.Host, fmt.Sprintf("compute2-%d", j))
		}
	}

	// GetTaskByHost
	for j := 0; j < int(nTasks); j++ {
		host := fmt.Sprintf("compute2-%d", j)
		tasks, err := store.GetTasksOnHost(host)
		suite.NoError(err)

		suite.Equal(len(tasks), 2)
		suite.Equal(tasks[fmt.Sprintf("TestGetTasksByHostState0-%d", j)],
			task.TaskState(j).String())
		suite.Equal(tasks[fmt.Sprintf("TestGetTasksByHostState1-%d", j)],
			task.TaskState(j).String())
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskStateChanges() {
	var jobStore storage.JobStore
	jobStore = store
	var taskStore storage.TaskStore
	taskStore = store
	nTasks := 2
	host1 := "compute1"
	host2 := "compute2"
	var jobID = peloton.JobID{Value: "TestGetTaskStateChanges"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err := jobStore.CreateJob(&jobID, jobConfig, "uber")
	suite.NoError(err)

	taskInfo := createTaskInfo(jobConfig, &jobID, 0)
	err = taskStore.CreateTask(&jobID, 0, taskInfo, "test")
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_PENDING
	err = taskStore.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_RUNNING
	taskInfo.Runtime.Host = host1
	err = taskStore.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_PREEMPTING
	taskInfo.Runtime.Host = ""
	err = taskStore.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_RUNNING
	taskInfo.Runtime.Host = host2
	err = taskStore.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_SUCCEEDED
	taskInfo.Runtime.Host = host2
	err = taskStore.UpdateTask(taskInfo)
	suite.NoError(err)

	taskID := fmt.Sprintf("%s-%d", jobID.Value, 0)

	stateRecords, err := store.GetTaskStateChanges(taskID)
	suite.NoError(err)

	suite.Equal(stateRecords[0].TaskState, task.TaskState_INITIALIZED.String())
	suite.Equal(stateRecords[1].TaskState, task.TaskState_PENDING.String())
	suite.Equal(stateRecords[2].TaskState, task.TaskState_RUNNING.String())
	suite.Equal(stateRecords[3].TaskState, task.TaskState_PREEMPTING.String())
	suite.Equal(stateRecords[4].TaskState, task.TaskState_RUNNING.String())
	suite.Equal(stateRecords[5].TaskState, task.TaskState_SUCCEEDED.String())

	suite.Equal(stateRecords[0].TaskHost, "")
	suite.Equal(stateRecords[1].TaskHost, "")
	suite.Equal(stateRecords[2].TaskHost, host1)
	suite.Equal(stateRecords[3].TaskHost, "")
	suite.Equal(stateRecords[4].TaskHost, host2)
	suite.Equal(stateRecords[5].TaskHost, host2)

	stateRecords, err = store.GetTaskStateChanges("taskdoesnotexist")
	suite.Error(err)

}

func (suite *CassandraStoreTestSuite) TestGetJobsByOwner() {
	nJobs := 2
	owner := "uberx"
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.Value
		err := store.CreateJob(&jobID, jobConfig, owner)
		suite.Nil(err)
	}

	owner = "team6s"
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.Value
		err := store.CreateJob(&jobID, jobConfig, owner)
		suite.Nil(err)
	}

	jobs, err := store.GetJobsByOwner("uberx")
	suite.Nil(err)
	suite.Equal(len(jobs), 2)
	suite.Equal(jobs["uberx0"].Name, "uberx0")
	suite.Equal(jobs["uberx1"].Name, "uberx1")

	jobs, err = store.GetJobsByOwner("team6s")
	suite.Nil(err)
	suite.Equal(len(jobs), 2)
	suite.Equal(jobs["team6s0"].Name, "team6s0")
	suite.Equal(jobs["team6s1"].Name, "team6s1")

	jobs, err = store.GetJobsByOwner("teamdoesnotexist")
	suite.Nil(err)
	suite.Equal(len(jobs), 0)
}

func (suite *CassandraStoreTestSuite) TestGetJobsByRespoolID() {
	nJobs := 2
	owner := "TestGetJobsByRespoolID"
	respoolBase := "respool"
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.Value
		jobConfig.RespoolID = &respool.ResourcePoolID{
			Value: fmt.Sprintf("%s%d", respoolBase, i),
		}
		err := store.CreateJob(&jobID, jobConfig, owner)
		suite.Nil(err)
	}

	owner = "TestGetJobsByRespoolID2"
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.Value
		jobConfig.RespoolID = &respool.ResourcePoolID{
			Value: fmt.Sprintf("%s%d", respoolBase, i),
		}
		err := store.CreateJob(&jobID, jobConfig, owner)
		suite.Nil(err)
	}

	jobs, err := store.GetJobsByRespoolID(&respool.ResourcePoolID{Value: respoolBase + "0"})
	suite.Nil(err)
	suite.Equal(len(jobs), 2)
	suite.Equal(respoolBase+"0", jobs["TestGetJobsByRespoolID0"].RespoolID.GetValue())
	suite.Equal(respoolBase+"0", jobs["TestGetJobsByRespoolID20"].RespoolID.GetValue())

	jobs, err = store.GetJobsByRespoolID(&respool.ResourcePoolID{Value: respoolBase + "1"})
	suite.Nil(err)
	suite.Equal(len(jobs), 2)

	suite.Equal(respoolBase+"1", jobs["TestGetJobsByRespoolID1"].RespoolID.GetValue())
	suite.Equal(respoolBase+"1", jobs["TestGetJobsByRespoolID21"].RespoolID.GetValue())

	jobs, err = store.GetJobsByRespoolID(&respool.ResourcePoolID{Value: "pooldoesnotexist"})
	suite.Nil(err)
	suite.Equal(len(jobs), 0)
}

func (suite *CassandraStoreTestSuite) TestGetTaskStateSummary() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: "TestGetTaskStateSummary"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(2 * len(task.TaskState_name))
	err := store.CreateJob(&jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := uint32(0); i < uint32(2*len(task.TaskState_name)); i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTask(&jobID, i, taskInfo, "user1")
		suite.Nil(err)
		taskInfo.Runtime.State = task.TaskState(i / 2)
		err = taskStore.UpdateTask(taskInfo)
		suite.Nil(err)
	}

	taskStateSummary, err := store.GetTaskStateSummaryForJob(&jobID)
	suite.Nil(err)
	suite.Equal(len(taskStateSummary), len(task.TaskState_name))
	for _, state := range task.TaskState_name {
		suite.Equal(taskStateSummary[state], 2)
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskByRange() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: "TestGetTaskByRange"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(100)
	err := store.CreateJob(&jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTask(&jobID, i, taskInfo, "user1")
		suite.Nil(err)
	}
	suite.validateRange(&jobID, 0, 30)
	suite.validateRange(&jobID, 30, 65)
	suite.validateRange(&jobID, 60, 83)
	suite.validateRange(&jobID, 70, 97)
	suite.validateRange(&jobID, 70, 120)
}

func (suite *CassandraStoreTestSuite) validateRange(jobID *peloton.JobID, from, to int) {
	var taskStore storage.TaskStore
	taskStore = store
	jobConfig, err := store.GetJobConfig(jobID)
	suite.NoError(err)

	if to > int(jobConfig.InstanceCount) {
		to = int(jobConfig.InstanceCount)
	}
	r := &task.InstanceRange{
		From: uint32(from),
		To:   uint32(to),
	}
	var taskInRange map[uint32]*task.TaskInfo
	taskInRange, _ = taskStore.GetTasksForJobByRange(jobID, r)

	suite.Equal(to-from, len(taskInRange))
	for i := from; i < to; i++ {
		tID := fmt.Sprintf("%s-%d", jobID.Value, i)
		suite.Equal(tID, *(taskInRange[uint32(i)].Runtime.TaskId.Value))
	}
}

func (suite *CassandraStoreTestSuite) TestCreateGetResourcePoolConfig() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	testCases := []struct {
		resourcePoolID string
		owner          string
		config         *respool.ResourcePoolConfig
		expectedErr    error
		msg            string
	}{
		{
			resourcePoolID: "first",
			owner:          "team1",
			config:         createResourcePoolConfig(),
			expectedErr:    nil,
			msg:            "testcase: create resource pool",
		},
		{
			resourcePoolID: "second",
			owner:          "",
			config:         createResourcePoolConfig(),
			expectedErr:    nil,
			msg:            "testcase: create resource pool, no owner",
		},
		{
			resourcePoolID: "",
			owner:          "team2",
			config:         createResourcePoolConfig(),
			expectedErr:    errors.New("Key may not be empty"),
			msg:            "testcase: create resource pool, no resource ID",
		},
		{
			resourcePoolID: "first",
			owner:          "team1",
			config:         createResourcePoolConfig(),
			expectedErr:    errors.New("first is not applied, item could exist already"),
			msg:            "testcase: create resource pool, duplicate ID",
		},
	}

	for _, tc := range testCases {
		actualErr := resourcePoolStore.CreateResourcePool(&respool.ResourcePoolID{Value: tc.resourcePoolID},
			tc.config, tc.owner)
		if tc.expectedErr == nil {
			suite.Nil(actualErr, tc.msg)
		} else {
			suite.EqualError(actualErr, tc.expectedErr.Error(), tc.msg)
		}
	}
}

func (suite *CassandraStoreTestSuite) GetAllResourcePools() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 2

	// todo move to setup once ^^^ issue resolves
	for i := 0; i < nResourcePools; i++ {
		resourcePoolID := &respool.ResourcePoolID{Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		resourcePoolConfig := createResourcePoolConfig()
		resourcePoolConfig.Name = resourcePoolID.Value
		err := resourcePoolStore.CreateResourcePool(resourcePoolID, resourcePoolConfig, _resPoolOwner)
		suite.Nil(err)
	}

	resourcePools, actualErr := resourcePoolStore.GetAllResourcePools()
	suite.NoError(actualErr)
	suite.Len(resourcePools, nResourcePools)

}

func (suite *CassandraStoreTestSuite) GetAllResourcePoolsEmptyResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 0
	resourcePools, actualErr := resourcePoolStore.GetAllResourcePools()
	suite.NoError(actualErr)
	suite.Len(resourcePools, nResourcePools)
}

func (suite *CassandraStoreTestSuite) TestGetResourcePoolsByOwner() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 2

	// todo move to setup once ^^^ issue resolves
	for i := 0; i < nResourcePools; i++ {
		resourcePoolID := &respool.ResourcePoolID{Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		resourcePoolConfig := createResourcePoolConfig()
		resourcePoolConfig.Name = resourcePoolID.Value
		err := resourcePoolStore.CreateResourcePool(resourcePoolID, resourcePoolConfig, _resPoolOwner)
		suite.Nil(err)
	}

	testCases := []struct {
		expectedErr    error
		owner          string
		nResourcePools int
		msg            string
	}{
		{
			expectedErr:    nil,
			owner:          "idon'texist",
			nResourcePools: 0,
			msg:            "testcase: fetch resource pools by non existent owner",
		},
		{
			expectedErr:    nil,
			owner:          _resPoolOwner,
			nResourcePools: nResourcePools,
			msg:            "testcase: fetch resource pools owner",
		},
	}

	for _, tc := range testCases {
		resourcePools, actualErr := resourcePoolStore.GetResourcePoolsByOwner(tc.owner)
		if tc.expectedErr == nil {
			suite.Nil(actualErr, tc.msg)
			suite.Len(resourcePools, tc.nResourcePools, tc.msg)
		} else {
			suite.EqualError(actualErr, tc.expectedErr.Error(), tc.msg)
		}
	}
}

func (suite *CassandraStoreTestSuite) TestJobRuntime() {
	var jobStore = store
	nTasks := 20

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: "TestJobRuntime"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err := jobStore.CreateJob(&jobID, jobConfig, "uber")
	suite.NoError(err)

	runtime, err := jobStore.GetJobRuntime(&jobID)
	suite.NoError(err)
	suite.Equal(job.JobState_INITIALIZED, runtime.State)
	suite.Equal(0, len(runtime.TaskStats))

	// update job runtime
	runtime.State = job.JobState_RUNNING
	runtime.TaskStats[task.TaskState_PENDING.String()] = 5
	runtime.TaskStats[task.TaskState_PLACED.String()] = 5
	runtime.TaskStats[task.TaskState_RUNNING.String()] = 5
	runtime.TaskStats[task.TaskState_SUCCEEDED.String()] = 5

	err = jobStore.UpdateJobRuntime(&jobID, runtime)
	suite.NoError(err)

	runtime, err = jobStore.GetJobRuntime(&jobID)
	suite.NoError(err)
	suite.Equal(job.JobState_RUNNING, runtime.State)
	suite.Equal(4, len(runtime.TaskStats))

	jobIds, err := store.GetJobsByState(job.JobState_RUNNING)
	suite.NoError(err)
	idFound := false
	for _, id := range jobIds {
		if id.Value == jobID.Value {
			idFound = true
		}
	}
	suite.True(idFound)

	jobIds, err = store.GetJobsByState(120)
	suite.NoError(err)
	suite.Equal(0, len(jobIds))
}

func (suite *CassandraStoreTestSuite) TestPersistentVolumeInfo() {
	var volumeStore storage.PersistentVolumeStore
	volumeStore = store
	pv := &volume.PersistentVolumeInfo{
		Id: &peloton.VolumeID{
			Value: "volume1",
		},
		State:     volume.VolumeState_INITIALIZED,
		GoalState: volume.VolumeState_CREATED,
		JobId: &peloton.JobID{
			Value: "job",
		},
		Hostname:      "host",
		InstanceId:    uint32(0),
		SizeMB:        uint32(10),
		ContainerPath: "testpath",
	}
	err := volumeStore.CreatePersistentVolume(pv)
	suite.NoError(err)

	rpv, err := volumeStore.GetPersistentVolume("volume1")
	suite.NoError(err)
	suite.Equal(rpv.Id.Value, "volume1")
	suite.Equal(rpv.State.String(), "INITIALIZED")
	suite.Equal(rpv.GoalState.String(), "CREATED")
	suite.Equal(rpv.JobId.Value, "job")
	suite.Equal(rpv.InstanceId, uint32(0))
	suite.Equal(rpv.Hostname, "host")
	suite.Equal(rpv.SizeMB, uint32(10))
	suite.Equal(rpv.ContainerPath, "testpath")

	// Verify get non-existent volume returns error.
	_, err = volumeStore.GetPersistentVolume("volume2")
	suite.Error(err)

	err = volumeStore.UpdatePersistentVolume("volume1", volume.VolumeState_CREATED)
	suite.NoError(err)

	// Verfy updated persistent volume info.
	rpv, err = volumeStore.GetPersistentVolume("volume1")
	suite.NoError(err)
	suite.Equal(rpv.Id.Value, "volume1")
	suite.Equal(rpv.State.String(), "CREATED")
	suite.Equal(rpv.State.String(), "CREATED")
	suite.Equal(rpv.JobId.Value, "job")
	suite.Equal(rpv.InstanceId, uint32(0))
	suite.Equal(rpv.Hostname, "host")
	suite.Equal(rpv.SizeMB, uint32(10))
	suite.Equal(rpv.ContainerPath, "testpath")

	err = volumeStore.DeletePersistentVolume("volume1")
	suite.NoError(err)

	// Verify volume has been deleted.
	_, err = volumeStore.GetPersistentVolume("volume1")
	suite.Error(err)
}

func createJobConfig() *job.JobConfig {
	var sla = job.SlaConfig{
		Priority:                22,
		MaximumRunningInstances: 6,
		Preemptible:             false,
	}
	var jobConfig = job.JobConfig{
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		Sla:           &sla,
		InstanceCount: uint32(6),
	}
	return &jobConfig
}

func createTaskInfo(
	jobConfig *job.JobConfig, jobID *peloton.JobID, i uint32) *task.TaskInfo {

	var tID = fmt.Sprintf("%s-%d", jobID.Value, i)
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{Value: &tID},
		},
		Config:     jobConfig.GetDefaultConfig(),
		InstanceId: uint32(i),
		JobId:      jobID,
	}
	return &taskInfo
}

// Returns mock resource pool config
func createResourcePoolConfig() *respool.ResourcePoolConfig {
	return &respool.ResourcePoolConfig{
		Name:        "TestResourcePool_1",
		ChangeLog:   nil,
		Description: "test resource pool",
		LdapGroups:  []string{"l1", "l2"},
		OwningTeam:  "team1",
		Parent:      nil,
		Policy:      1,
		Resources:   createResourceConfigs(),
	}
}

// Returns mock list of resource configs
func createResourceConfigs() []*respool.ResourceConfig {
	return []*respool.ResourceConfig{
		{
			Kind:        "cpu",
			Limit:       1000.0,
			Reservation: 100.0,
			Share:       1.0,
		},
		{
			Kind:        "gpu",
			Limit:       4.0,
			Reservation: 2.0,
			Share:       1.0,
		},
	}
}
