package mysql

import (
	"code.uber.internal/go-common.git/x/log"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/mattes/migrate/migrate"
	"github.com/stretchr/testify/suite"
	mesos_v1 "mesos/v1"
	"peloton/job"
	"peloton/task"
	"strconv"
	"testing"
)

func downSync(cfg *Config) []error {
	connString := cfg.MigrateString()
	errors, ok := migrate.DownSync(connString, cfg.Migrations)
	if !ok {
		return errors
	}
	return nil
}

// LoadConfig loads test configuration
// TODO: figure out how to install mysql 5.7.x before testing
func prepareTest() *Config {
	conf := &Config{
		User:       "peloton",
		Password:   "peloton",
		Host:       "127.0.0.1",
		Port:       3306,
		Database:   "peloton",
		Migrations: "migrations",
	}
	err := conf.Connect()
	if err != nil {
		panic(err)
	}
	if errs := downSync(conf); errs != nil {
		log.Warnf(fmt.Sprintf("downSync is having the following error: %+v", errs))
	}

	// bring the schema up to date
	if errs := conf.AutoMigrate(); errs != nil {
		panic(fmt.Sprintf("%+v", errs))
	}
	fmt.Println("setting up again")
	return conf
}

type MysqlStoreTestSuite struct {
	suite.Suite
	store *MysqlJobStore
	db    *sqlx.DB
}

func (suite *MysqlStoreTestSuite) SetupTest() {
	conf := prepareTest()

	suite.db = conf.Conn
	suite.store = NewMysqlJobStore(conf.Conn)
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
			Priority:               22,
			MinimumInstanceCount:   3 + uint32(i),
			MinimumInstancePercent: 50,
			Preemptible:            false,
		}
		var jobConfig = job.JobConfig{
			Name:       "TestJob_" + strconv.Itoa(i),
			OwningTeam: "team6",
			LdapGroups: []string{"money", "team6", "otto"},
			Sla:        &sla,
		}
		jobs = append(jobs, &jobConfig)
		err := suite.store.CreateJob(&jobID, &jobConfig, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := 0; j < 3; j++ {
			var tID = fmt.Sprintf("%s-%d", jobID.Value, j)
			var taskInfo = task.TaskInfo{
				Runtime: &task.RuntimeInfo{
					TaskId: &mesos_v1.TaskID{Value: &tID},
					State:  task.RuntimeInfo_TaskState(j),
				},
				JobConfig:  &jobConfig,
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

func (suite *MysqlStoreTestSuite) TestCreateGetJobConfig() {
	// Create 10 jobs in db
	var originalJobs []*job.JobConfig
	var records = 10
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	for i := 0; i < records; i++ {
		var jobID = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
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
		var labels = mesos_v1.Labels{
			Labels: []*mesos_v1.Label{
				&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
				&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
				&mesos_v1.Label{Key: &keys[2], Value: &vals[2]},
			},
		}
		// Add some special label to job0 and job1
		if i < 2 {
			labels.Labels = append(labels.Labels, &mesos_v1.Label{Key: &keys[3], Value: &vals[3]})
		}

		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}
		var jobconfig = job.JobConfig{
			Name:       "TestJob_" + strconv.Itoa(i),
			OwningTeam: owner,
			LdapGroups: []string{"money", "team6", "otto"},
			Sla:        &sla,
			Resource:   &resourceConfig,
			Labels:     &labels,
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
		suite.Equal(result.Resource.FdLimit, originalJobs[i].Resource.FdLimit)
		suite.Equal(result.Sla.MinimumInstanceCount, originalJobs[i].Sla.MinimumInstanceCount)
	}

	// Query by owner
	var jobs map[string]*job.JobConfig
	var err error
	var labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
			&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), len(originalJobs))

	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
			&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
			&mesos_v1.Label{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)
	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)

	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[2], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 0)

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
