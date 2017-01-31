package stapi_test

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage/stapi"
	"fmt"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	mesos "mesos/v1"
	"peloton/job"
	"peloton/task"
	"testing"
)

type STAPIStoreTestSuite struct {
	suite.Suite
	store *stapi.Store
}

// NOTE(gabe): using this method of setup is definitely less elegant than a SetupTest() and TearDownTest()
// helper functions of suite, but has the unfortunate sideeffect of making test runs on my MBP go from ~3m
// wall to 10m wall. For now, keep using init() until a fix for this is found

var store *stapi.Store

func init() {
	conf := stapi.MigrateForTest()
	var err error
	store, err = stapi.NewStore(conf, tally.NoopScope)
	if err != nil {
		log.Fatal(err)
	}
}

func TestSTAPIStore(t *testing.T) {
	suite.Run(t, new(STAPIStoreTestSuite))
}

func (suite *STAPIStoreTestSuite) TestCreateGetJobConfig() {
	var originalJobs []*job.JobConfig
	var records = 1
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: fmt.Sprintf("TestCreateGetJobConfig%d", i)}
		var sla = job.SlaConfig{
			Priority:               22,
			MinimumInstanceCount:   3 + uint32(i),
			MinimumInstancePercent: 50,
			Preemptible:            false,
		}
		var resourceConfig = job.ResourceConfig{
			CpusLimit:   0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000 + uint32(i),
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
			labels.Labels = append(labels.Labels, &mesos.Label{Key: &keys[3], Value: &vals[3]})
		}

		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}
		var jobconfig = job.JobConfig{
			Name:       fmt.Sprintf("TestJob_%d", i),
			OwningTeam: owner,
			LdapGroups: []string{"money", "team6", "otto"},
			Sla:        &sla,
			Resource:   &resourceConfig,
			Labels:     &labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := store.CreateJob(&jobID, &jobconfig, "uber")
		suite.NoError(err)

		// Create job with same job id would be no op
		jobconfig.Labels = nil
		jobconfig.Name = "random"
		err = store.CreateJob(&jobID, &jobconfig, "uber2")
		suite.Error(err)

		var jobconf *job.JobConfig
		jobconf, err = store.GetJob(&jobID)
		suite.NoError(err)
		suite.Equal(jobconf.Name, fmt.Sprintf("TestJob_%d", i))
		suite.Equal(len((*(jobconf.Labels)).Labels), 4)
	}
}

func (suite *STAPIStoreTestSuite) TestFrameworkInfo() {
	err := store.SetMesosFrameworkID("framework1", "12345")
	suite.NoError(err)
	var frameworkID string
	frameworkID, err = store.GetFrameworkID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = store.GetFrameworkID("framework2")
	suite.Error(err)

	err = store.SetMesosStreamID("framework1", "s-12345")
	suite.NoError(err)

	frameworkID, err = store.GetFrameworkID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = store.GetMesosStreamID("framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "s-12345")
}

func (suite *STAPIStoreTestSuite) TestCreateTask() {
	var nJobs = 3
	var nTasks = 3
	var jobIDs []*job.JobID
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = job.JobID{Value: fmt.Sprintf("TestAddTasks_%d", i)}
		jobIDs = append(jobIDs, &jobID)
		jobConfig := createJobConfig()
		jobConfig.Name = fmt.Sprintf("TestAddTasks_%d", i)
		jobs = append(jobs, jobConfig)
		err := store.CreateJob(&jobID, jobConfig, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := 0; j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			taskInfo.Runtime.State = task.RuntimeInfo_TaskState(j)
			err = store.CreateTask(&jobID, j, taskInfo, "test")
			suite.NoError(err)
			// Create same task should error
			err = store.CreateTask(&jobID, j, taskInfo, "test")
			suite.Error(err)
			err = store.UpdateTask(taskInfo)
			suite.NoError(err)
		}
	}
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := store.GetTasksForJob(jobIDs[i])
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
		tasks, err := store.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			task.Runtime.Host = fmt.Sprintf("compute_%d", i)
			err := store.UpdateTask(task)
			suite.NoError(err)
		}
	}
	for i := 0; i < nJobs; i++ {
		tasks, err := store.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.Runtime.Host, fmt.Sprintf("compute_%d", i))
		}
	}

	for i := 0; i < nJobs; i++ {
		for j := 0; j < nTasks; j++ {
			taskID := fmt.Sprintf("%s-%d", jobIDs[i].Value, j)
			task, err := store.GetTaskByID(taskID)
			suite.NoError(err)
			suite.Equal(task.JobId.Value, jobIDs[i].Value)
			suite.Equal(task.InstanceId, uint32(j))
		}
		// TaskID does not exist
	}
	task, err := store.GetTaskByID("taskdoesnotexist")
	suite.Error(err)
	suite.Nil(task)
}

// TestCreateTasks ensures mysql task create batching works as expected.
func (suite *STAPIStoreTestSuite) TestCreateTasks() {
	jobTasks := map[string]int{
		"TestJob1": 10,
		"TestJob2": store.Conf.MaxBatchSize,
		"TestJob3": store.Conf.MaxBatchSize*3 + 10,
	}
	for jobID, nTasks := range jobTasks {
		var jobID = job.JobID{Value: jobID}
		var sla = job.SlaConfig{
			Priority:               22,
			MinimumInstanceCount:   3,
			MinimumInstancePercent: 50,
			Preemptible:            false,
		}
		var jobConfig = job.JobConfig{
			Name:       jobID.Value,
			OwningTeam: "team6",
			LdapGroups: []string{"money", "team6", "otto"},
			Sla:        &sla,
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
					State:  task.RuntimeInfo_TaskState(j),
				},
				JobConfig:  &jobConfig,
				InstanceId: uint32(j),
				JobId:      &jobID,
			}
			taskInfos = append(taskInfos, &taskInfo)
		}
		err = store.CreateTasks(&jobID, taskInfos, "test")
		suite.NoError(err)
	}

	// List all tasks by job, ensure they were created properly, and have the right parent
	for jobID, nTasks := range jobTasks {
		job := job.JobID{Value: jobID}
		tasks, err := store.GetTasksForJob(&job)
		suite.NoError(err)
		suite.Equal(nTasks, len(tasks))
		for _, task := range tasks {
			suite.Equal(jobID, task.JobId.Value)
		}
	}
}

func (suite *STAPIStoreTestSuite) TestGetTasksByHostState() {
	var nJobs = 2
	var nTasks = 6
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = job.JobID{Value: fmt.Sprintf("TestGetTasksByHostState%d", i)}
		jobConfig := createJobConfig()
		jobConfig.InstanceCount = uint32(nTasks)
		jobs = append(jobs, jobConfig)
		err := store.CreateJob(&jobID, jobConfig, "uber")
		suite.NoError(err)
		for j := 0; j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			err = store.CreateTask(&jobID, j, taskInfo, "test")
			suite.NoError(err)
			taskInfo.Runtime.State = task.RuntimeInfo_TaskState(j)
			taskInfo.Runtime.Host = fmt.Sprintf("compute2-%d", j)
			err = store.UpdateTask(taskInfo)
			suite.NoError(err)
		}
	}
	// GetTaskByState
	for j := 0; j < nTasks; j++ {
		jobID := job.JobID{Value: "TestGetTasksByHostState0"}
		tasks, err := store.GetTasksForJobAndState(&jobID, task.RuntimeInfo_TaskState(j).String())
		suite.NoError(err)
		suite.Equal(len(tasks), 1)

		for tid, host := range tasks {
			suite.Equal(tid, fmt.Sprintf("%s-%d", jobID.Value, j))
			suite.Equal(host, fmt.Sprintf("compute2-%d", j))
		}

		jobID = job.JobID{Value: "TestGetTasksByHostState1"}
		tasks, err = store.GetTasksForJobAndState(&jobID, task.RuntimeInfo_TaskState(j).String())
		suite.NoError(err)

		for tid, host := range tasks {
			suite.Equal(tid, fmt.Sprintf("%s-%d", jobID.Value, j))
			suite.Equal(host, fmt.Sprintf("compute2-%d", j))
		}
	}

	// GetTaskByHost
	for j := 0; j < nTasks; j++ {
		host := fmt.Sprintf("compute2-%d", j)
		tasks, err := store.GetTasksOnHost(host)
		suite.NoError(err)

		suite.Equal(len(tasks), 2)
		suite.Equal(tasks[fmt.Sprintf("TestGetTasksByHostState0-%d", j)], task.RuntimeInfo_TaskState(j).String())
		suite.Equal(tasks[fmt.Sprintf("TestGetTasksByHostState1-%d", j)], task.RuntimeInfo_TaskState(j).String())
	}
}

func (suite *STAPIStoreTestSuite) TestGetTaskStateChanges() {
	nTasks := 2
	host1 := "compute1"
	host2 := "compute2"
	var jobID = job.JobID{Value: "TestGetTaskStateChanges"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err := store.CreateJob(&jobID, jobConfig, "uber")
	suite.NoError(err)

	taskInfo := createTaskInfo(jobConfig, &jobID, 0)
	err = store.CreateTask(&jobID, 0, taskInfo, "test")
	suite.NoError(err)

	taskInfo.Runtime.State = task.RuntimeInfo_SCHEDULING
	err = store.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.RuntimeInfo_RUNNING
	taskInfo.Runtime.Host = host1
	err = store.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.RuntimeInfo_PREEMPTING
	taskInfo.Runtime.Host = ""
	err = store.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.RuntimeInfo_RUNNING
	taskInfo.Runtime.Host = host2
	err = store.UpdateTask(taskInfo)
	suite.NoError(err)

	taskInfo.Runtime.State = task.RuntimeInfo_SUCCEEDED
	taskInfo.Runtime.Host = host2
	err = store.UpdateTask(taskInfo)
	suite.NoError(err)

	taskID := fmt.Sprintf("%s-%d", jobID.Value, 0)

	stateRecords, err := store.GetTaskStateChanges(taskID)
	suite.NoError(err)

	suite.Equal(stateRecords[0].TaskState, task.RuntimeInfo_INITIALIZED.String())
	suite.Equal(stateRecords[1].TaskState, task.RuntimeInfo_SCHEDULING.String())
	suite.Equal(stateRecords[2].TaskState, task.RuntimeInfo_RUNNING.String())
	suite.Equal(stateRecords[3].TaskState, task.RuntimeInfo_PREEMPTING.String())
	suite.Equal(stateRecords[4].TaskState, task.RuntimeInfo_RUNNING.String())
	suite.Equal(stateRecords[5].TaskState, task.RuntimeInfo_SUCCEEDED.String())

	suite.Equal(stateRecords[0].TaskHost, "")
	suite.Equal(stateRecords[1].TaskHost, "")
	suite.Equal(stateRecords[2].TaskHost, host1)
	suite.Equal(stateRecords[3].TaskHost, "")
	suite.Equal(stateRecords[4].TaskHost, host2)
	suite.Equal(stateRecords[5].TaskHost, host2)

	stateRecords, err = store.GetTaskStateChanges("taskdoesnotexist")
	suite.Error(err)

}

func (suite *STAPIStoreTestSuite) TestGetJobsByOwner() {
	nJobs := 2
	owner := "uberx"
	for i := 0; i < nJobs; i++ {
		var jobID = job.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.Value
		err := store.CreateJob(&jobID, jobConfig, owner)
		suite.Nil(err)
	}

	owner = "team6s"
	for i := 0; i < nJobs; i++ {
		var jobID = job.JobID{Value: fmt.Sprintf("%s%d", owner, i)}
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

func (suite *STAPIStoreTestSuite) TestGetTaskStateSummary() {
	var jobID = job.JobID{Value: "TestGetTaskStateSummary"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(2 * len(task.RuntimeInfo_TaskState_name))
	err := store.CreateJob(&jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := 0; i < 2*len(task.RuntimeInfo_TaskState_name); i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := store.CreateTask(&jobID, i, taskInfo, "user1")
		suite.Nil(err)
		taskInfo.Runtime.State = task.RuntimeInfo_TaskState(i / 2)
		err = store.UpdateTask(taskInfo)
		suite.Nil(err)
	}

	taskStateSummary, err := store.GetTaskStateSummaryForJob(&jobID)
	suite.Nil(err)
	suite.Equal(len(taskStateSummary), len(task.RuntimeInfo_TaskState_name))
	for _, state := range task.RuntimeInfo_TaskState_name {
		suite.Equal(taskStateSummary[state], 2)
	}
}

func (suite *STAPIStoreTestSuite) TestGetTaskByRange() {
	var jobID = job.JobID{Value: "TestGetTaskByRange"}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(100)
	err := store.CreateJob(&jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := 0; i < int(jobConfig.InstanceCount); i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := store.CreateTask(&jobID, i, taskInfo, "user1")
		suite.Nil(err)
		err = store.CreateTask(&jobID, i, taskInfo, "user1")
		suite.Error(err)
	}
	suite.validateRange(&jobID, 0, 30)
	suite.validateRange(&jobID, 30, 65)
	suite.validateRange(&jobID, 60, 83)
	suite.validateRange(&jobID, 70, 97)
	suite.validateRange(&jobID, 70, 120)
}

func (suite *STAPIStoreTestSuite) validateRange(jobID *job.JobID, from int, to int) {
	jobConfig, err := store.GetJob(jobID)
	suite.NoError(err)

	r := &task.InstanceRange{
		From: uint32(from),
		To:   uint32(to),
	}
	var taskInRange map[uint32]*task.TaskInfo
	taskInRange = store.GetTasksForJobByRange(jobID, r)
	if to >= int(jobConfig.InstanceCount) {
		to = int(jobConfig.InstanceCount - 1)
	}
	suite.Equal(to-from+1, len(taskInRange))
	for i := from; i <= to; i++ {
		tID := fmt.Sprintf("%s-%d", jobID.Value, i)
		suite.Equal(tID, *(taskInRange[uint32(i)].Runtime.TaskId.Value))
	}
}

func createJobConfig() *job.JobConfig {
	var sla = job.SlaConfig{
		Priority:               22,
		MinimumInstanceCount:   6,
		MinimumInstancePercent: 50,
		Preemptible:            false,
	}
	var jobConfig = job.JobConfig{
		OwningTeam:    "uber",
		LdapGroups:    []string{"money", "team6", "otto"},
		Sla:           &sla,
		InstanceCount: uint32(6),
	}
	return &jobConfig
}

func createTaskInfo(jobConfig *job.JobConfig, jobID *job.JobID, i int) *task.TaskInfo {
	var tID = fmt.Sprintf("%s-%d", jobID.Value, i)
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			TaskId: &mesos.TaskID{Value: &tID},
		},
		JobConfig:  jobConfig,
		InstanceId: uint32(i),
		JobId:      jobID,
	}
	return &taskInfo
}
