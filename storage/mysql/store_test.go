package mysql

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/task"
	"peloton/api/task/config"
)

type MysqlStoreTestSuite struct {
	suite.Suite
	store *JobStore
	db    *sqlx.DB
}

func (suite *MysqlStoreTestSuite) SetupTest() {
	conf := LoadConfigWithDB()

	suite.db = conf.Conn
	suite.store = NewJobStore(*conf, tally.NoopScope)
}

func (suite *MysqlStoreTestSuite) TearDownTest() {
	fmt.Println("tearing down")
	//_, err := suite.db.Exec("DELETE FROM task")
	//suite.NoError(err)
}

func TestMysqlStore(t *testing.T) {
	suite.Run(t, new(MysqlStoreTestSuite))
}

func (suite *MysqlStoreTestSuite) TestCreateGetTaskInfo() {
	// Insert 2 jobs
	var nJobs = 3
	var jobIDs []*job.JobID
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		jobIDs = append(jobIDs, &jobID)
		var sla = job.SlaConfig{
			Priority:                22,
			Preemptible:             false,
			MaximumRunningInstances: 3 + uint32(i),
		}
		var taskConfig = config.TaskConfig{
			Resource: &config.ResourceConfig{
				CpusLimit:   0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000,
			},
		}
		var jobConfig = job.JobConfig{
			Name:          "TestJob_" + strconv.Itoa(i),
			OwningTeam:    "team6",
			LdapGroups:    []string{"money", "team6", "otto"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
		}
		jobs = append(jobs, &jobConfig)
		err := suite.store.CreateJob(&jobID, &jobConfig, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := uint32(0); j < 3; j++ {
			var tID = fmt.Sprintf("%s-%d", jobID.Value, j)
			var taskInfo = task.TaskInfo{
				Runtime: &task.RuntimeInfo{
					TaskId: &mesos.TaskID{Value: &tID},
					State:  task.RuntimeInfo_TaskState(j),
				},
				Config:     jobConfig.GetDefaultConfig(),
				InstanceId: uint32(j),
				JobId:      &jobID,
			}
			err = suite.store.CreateTask(&jobID, j, &taskInfo, "test")
			suite.NoError(err)
		}
	}
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := suite.store.GetTasksForJob(jobIDs[i])
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
		tasks, err := suite.store.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			task.Runtime.Host = fmt.Sprintf("compute-%d", i)
			err := suite.store.UpdateTask(task)
			suite.NoError(err)
		}
	}
	for i := 0; i < nJobs; i++ {
		tasks, err := suite.store.GetTasksForJob(jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.Runtime.Host, fmt.Sprintf("compute-%d", i))
		}
	}
}

// TestCreateTasks ensures mysql task create batching works as expected.
func (suite *MysqlStoreTestSuite) TestCreateTasks() {
	jobTasks := map[string]int{
		"TestJob1": 10,
		"TestJob2": suite.store.Conf.MaxBatchSize,
		"TestJob3": suite.store.Conf.MaxBatchSize * 3,
		"TestJob4": suite.store.Conf.MaxBatchSize*3 + 10,
	}
	for jobID, nTasks := range jobTasks {
		var jobID = job.JobID{Value: jobID}
		var sla = job.SlaConfig{
			Priority:                22,
			Preemptible:             false,
			MaximumRunningInstances: 3,
		}
		var taskConfig = config.TaskConfig{
			Resource: &config.ResourceConfig{
				CpusLimit:   0.8,
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
		err := suite.store.CreateJob(&jobID, &jobConfig, "uber")
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
				Config:     jobConfig.GetDefaultConfig(),
				InstanceId: uint32(j),
				JobId:      &jobID,
			}
			taskInfos = append(taskInfos, &taskInfo)
		}
		err = suite.store.CreateTasks(&jobID, taskInfos, "test")
		suite.NoError(err)
	}

	// List all tasks by job, ensure they were created properly, and have the right parent
	for jobID, nTasks := range jobTasks {
		job := job.JobID{Value: jobID}
		tasks, err := suite.store.GetTasksForJob(&job)
		suite.NoError(err)
		suite.Equal(nTasks, len(tasks))
		for _, task := range tasks {
			suite.Equal(jobID, task.JobId.Value)
		}
	}
}

func (suite *MysqlStoreTestSuite) TestCreateGetJobConfig() {
	// Create 10 jobs in db
	var originalJobs []*job.JobConfig
	var records = 10
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		var sla = job.SlaConfig{
			Priority:                22,
			Preemptible:             false,
			MaximumRunningInstances: 3,
		}
		var taskConfig = config.TaskConfig{
			Resource: &config.ResourceConfig{
				CpusLimit:   0.8,
				MemLimitMb:  800,
				DiskLimitMb: 1500,
				FdLimit:     1000,
			},
		}
		var labels = mesos.Labels{
			Labels: []*mesos.Label{
				{Key: &keys[0], Value: &vals[0]},
				{Key: &keys[1], Value: &vals[1]},
				{Key: &keys[2], Value: &vals[2]},
			},
		}
		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}

		// Add some special label to job0 and job1
		if i < 2 {
			labels.Labels = append(labels.Labels,
				&mesos.Label{Key: &keys[3], Value: &vals[3]})
		}

		var jobconfig = job.JobConfig{
			Name:          "TestJob_" + strconv.Itoa(i),
			Sla:           &sla,
			OwningTeam:    owner,
			LdapGroups:    []string{"money", "team6", "otto"},
			DefaultConfig: &taskConfig,
			Labels:        &labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := suite.store.CreateJob(&jobID, &jobconfig, "uber")
		suite.NoError(err)

		err = suite.store.CreateJob(&jobID, &jobconfig, "uber")
		suite.Error(err)
	}
	// search by job ID
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		result, err := suite.store.GetJob(&jobID)
		suite.NoError(err)
		suite.Equal(result.Name, originalJobs[i].Name)
		suite.Equal(result.DefaultConfig.Resource.FdLimit,
			originalJobs[i].DefaultConfig.Resource.FdLimit)
		suite.Equal(result.Sla.MaximumRunningInstances,
			originalJobs[i].Sla.MaximumRunningInstances)
	}

	// Query by owner
	var jobs map[string]*job.JobConfig
	var err error
	var labelQuery = mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys[0], Value: &vals[0]},
			{Key: &keys[1], Value: &vals[1]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), len(originalJobs))

	labelQuery = mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys[0], Value: &vals[0]},
			{Key: &keys[1], Value: &vals[1]},
			{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)
	labelQuery = mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)

	labelQuery = mesos.Labels{
		Labels: []*mesos.Label{
			{Key: &keys[2], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 0)

	labelQuery = mesos.Labels{
		Labels: []*mesos.Label{},
	}
	// test get all jobs if no labels
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 10)

	jobs, err = suite.store.GetJobsByOwner("team6")
	suite.NoError(err)
	suite.Equal(len(jobs), records-2)

	jobs, err = suite.store.GetJobsByOwner("money")
	suite.NoError(err)
	suite.Equal(len(jobs), 2)

	jobs, err = suite.store.GetJobsByOwner("owner")
	suite.NoError(err)
	suite.Equal(len(jobs), 0)

	// Delete job
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		err := suite.store.DeleteJob(&jobID)
		suite.NoError(err)
	}
	// Get should not return anything
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		result, err := suite.store.GetJob(&jobID)
		suite.NoError(err)
		suite.Nil(result)
	}
}
