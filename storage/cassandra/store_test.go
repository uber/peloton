// +build !unit

package cassandra

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/storage"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
var testScope = tally.NewTestScope("", map[string]string{})

func init() {
	conf := MigrateForTest()
	var err error
	store, err = NewStore(conf, testScope)
	if err != nil {
		log.Fatal(err)
	}
}

func TestCassandraStore(t *testing.T) {
	suite.Run(t, new(CassandraStoreTestSuite))
	assert.True(t, testScope.Snapshot().Counters()["execute.execute+result=success,store=peloton_test"].Value() > 0)
	assert.True(t, testScope.Snapshot().Counters()["executeBatch.executeBatch+result=success,store=peloton_test"].Value() > 0)
}

func (suite *CassandraStoreTestSuite) createJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, owner string) error {
	if err := store.CreateJob(ctx, id, jobConfig, owner); err != nil {
		return err
	}

	return store.CreateTaskConfigs(ctx, id, jobConfig)
}

func (suite *CassandraStoreTestSuite) TestQueryJobPaging() {
	var jobStore storage.JobStore
	jobStore = store

	var originalJobs []*job.JobConfig
	var jobIDs []*peloton.JobID
	var records = 300
	respool := &peloton.ResourcePoolID{Value: uuid.New()}

	var keys0 = []string{"test0", "test1", "test2", "test3"}
	var vals0 = []string{"testValue0", "testValue1", `"testValue2"`, "testValue3"}

	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
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
		var labels = []*peloton.Label{
			{Key: keys0[i%len(keys0)], Value: vals0[i%len(keys0)]},
		}
		var jobConfig = job.JobConfig{
			Name:          fmt.Sprintf("TestJob_%d", i),
			OwningTeam:    fmt.Sprintf("owner_%d", 1000+i),
			LdapGroups:    []string{"TestQueryJobPaging", "team6", "gign"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
			Labels:        labels,
			Description:   fmt.Sprintf("A test job with awesome keyword%v keytest%v", i, i),
			RespoolID:     respool,
		}
		originalJobs = append(originalJobs, &jobConfig)
		err := suite.createJob(context.Background(), &jobID, &jobConfig, "uber")
		suite.NoError(err)

		// Update job runtime to different values
		runtime, err := jobStore.GetJobRuntime(context.Background(), &jobID)
		suite.NoError(err)

		runtime.State = job.JobState(i + 1)
		err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
		suite.NoError(err)
	}
	// Run the following query to trigger rebuild the lucene index
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobIndexTable).Where("expr(job_index_lucene, '{refresh:true}')")
	_, err := store.DataStore.Execute(context.Background(), stmt)
	suite.NoError(err)

	result1, total, err := jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
		},
	})
	suite.NoError(err)
	suite.Equal(25, len(result1))
	suite.Equal(_defaultQueryMaxLimit, uint32(total))

	var owner = query.PropertyPath{Value: "owner"}
	var orderByOwner = query.OrderBy{
		Property: &owner,
		Order:    query.OrderBy_ASC,
	}

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
			OrderBy: []*query.OrderBy{
				&orderByOwner,
			},
		},
	})
	suite.NoError(err)
	suite.Equal(25, len(result1))
	for i, c := range result1 {
		suite.Equal(fmt.Sprintf("owner_%d", 1010+i), c.Config.OwningTeam)
	}
	suite.Equal(_defaultQueryMaxLimit, uint32(total))

	orderByOwner.Order = query.OrderBy_DESC
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"TestQueryJobPaging", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 10,
			Limit:  25,
			OrderBy: []*query.OrderBy{
				&orderByOwner,
			},
		},
	})
	suite.NoError(err)
	suite.Equal(25, len(result1))
	for i, c := range result1 {
		suite.Equal(fmt.Sprintf("owner_%d", 1289-i), c.Config.OwningTeam)
	}
	suite.Equal(_defaultQueryMaxLimit, uint32(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), respool, &job.QuerySpec{})
	suite.NoError(err)
	suite.Equal(int(_defaultQueryLimit), len(result1))
	suite.Equal(_defaultQueryMaxLimit, uint32(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), &peloton.ResourcePoolID{Value: uuid.New()}, &job.QuerySpec{})
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(0, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), respool, &job.QuerySpec{
		Pagination: &query.PaginationSpec{
			Limit: 1000,
		},
	})
	suite.NoError(err)
	suite.Equal(_defaultQueryMaxLimit, uint32(len(result1)))
	suite.Equal(_defaultQueryMaxLimit, uint32(total))
}

func (suite *CassandraStoreTestSuite) TestQueryJob() {
	var jobStore storage.JobStore
	jobStore = store

	var originalJobs []*job.JobConfig
	var jobIDs []*peloton.JobID
	var records = 5

	var keys0 = []string{"test0", "test1", "test2", "test3", "test4", "test5"}
	var vals0 = []string{"testValue0", "testValue1", `"testValue2"`, "testValue3", "testValue4", "testValue5"}

	var keys1 = []string{"key0", "key1", "key2", "key3", "key4", "key5"}
	var vals1 = []string{"valX0", "valX1", "valX2", "valX3", "valX4", "valX5"}
	keyCommon := "keyX"
	valCommon := "valX"

	// Create 3 jobs with different labels and a common label
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()} // fmt.Sprintf("TestQueryJob%d", i)}
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
		instanceConfig := make(map[uint32]*task.TaskConfig)
		instanceConfig[0] = &taskConfig
		var labels = []*peloton.Label{
			{Key: keys0[i], Value: vals0[i]},
			{Key: keys1[i], Value: vals1[i]},
			{Key: keyCommon, Value: valCommon},
		}
		var jobConfig = job.JobConfig{
			Name:           fmt.Sprintf("TestQueryJob_%d", i),
			OwningTeam:     "owner",
			LdapGroups:     []string{"money", "team6", "gign"},
			Sla:            &sla,
			DefaultConfig:  &taskConfig,
			Labels:         labels,
			Description:    fmt.Sprintf("A test job with awesome keyword%v keytest%v", i, i),
			InstanceConfig: instanceConfig,
		}
		originalJobs = append(originalJobs, &jobConfig)
		err := suite.createJob(context.Background(), &jobID, &jobConfig, "uber")
		suite.NoError(err)

		// Update job runtime to different values
		runtime, err := jobStore.GetJobRuntime(context.Background(), &jobID)
		suite.NoError(err)

		runtime.State = job.JobState(i + 1)
		err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
		suite.NoError(err)
	}

	// Run the following query to trigger rebuild the lucene index
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobIndexTable).Where("expr(job_index_lucene, '{refresh:true}')")
	_, err := store.DataStore.Execute(context.Background(), stmt)
	suite.NoError(err)

	// query by common label should return all jobs
	result1, total, err := jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
	})
	suite.NoError(err)
	suite.Equal(records, len(result1))
	suite.Equal(records, int(total))

	asMap := map[string]*job.JobInfo{}
	for _, r := range result1 {
		asMap[r.Id.Value] = r
	}

	for i := 0; i < records; i++ {
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)
	}

	// query by specific state returns one job
	for i := 0; i < records; i++ {
		result1, total, err := jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
			Labels: []*peloton.Label{
				{Key: keyCommon, Value: valCommon},
			},
			JobStates: []job.JobState{job.JobState(i + 1)},
		})
		suite.NoError(err)
		suite.Equal(1, len(result1))
		suite.Equal(1, int(total))
		suite.Equal(i+1, int(result1[0].Runtime.State))
		suite.Nil(result1[0].GetConfig().GetInstanceConfig())
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)
	}
	// Update tasks to different states, and query by state
	for i := 0; i < records; i++ {
		result1, total, err := jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
			Labels: []*peloton.Label{
				{Key: keys0[i], Value: vals0[i]},
				{Key: keys1[i], Value: vals1[i]},
			},
		})
		suite.NoError(err)
		suite.Equal(1, len(result1))
		suite.Equal(1, int(total))
		suite.Equal(fmt.Sprintf("TestQueryJob_%d", i), asMap[jobIDs[i].Value].Config.Name)
	}

	// query for non-exist label return nothing
	var other = "other"
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keys0[0], Value: other},
			{Key: keys1[1], Value: vals1[0]},
		},
	})
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(0, int(total))

	// Test query with keyword
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
	})
	suite.NoError(err)
	suite.Equal(records, len(result1))
	suite.Equal(records, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome", "keytest1"},
	})
	suite.NoError(err)
	suite.Equal(1, len(result1))
	suite.Equal(1, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome", "nonexistkeyword"},
	})
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(0, int(total))

	// Query with both labels and keyword
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keys0[0], Value: vals0[0]},
		},
		Keywords: []string{"team6", "test", "awesome"},
	})
	suite.NoError(err)
	suite.Equal(1, len(result1))
	suite.Equal(1, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 0,
			Limit:  0,
		},
	})
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(records, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 1,
			Limit:  0,
		},
	})
	suite.NoError(err)
	suite.Equal(0, len(result1))
	suite.Equal(records, int(total))

	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset: 0,
			Limit:  1,
		},
	})
	suite.NoError(err)
	suite.Equal(1, len(result1))
	suite.Equal(records, int(total))

	// Test max limit should cap total returned.
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Keywords: []string{"team6", "test", "awesome"},
		Pagination: &query.PaginationSpec{
			Offset:   0,
			Limit:    1,
			MaxLimit: 2,
		},
	})
	suite.NoError(err)
	suite.Equal(1, len(result1))
	// Total should be 2, same as MaxLimit, instead of 5.
	suite.Equal(2, int(total))

	// Search for label with quote.
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{{
			Key:   keys0[2],
			Value: vals0[2],
		}},
	})
	suite.NoError(err)
	suite.Equal(1, len(result1))
	suite.Equal(1, int(total))

	// Query for multiple states
	for i := 0; i < records; i++ {
		runtime, err := jobStore.GetJobRuntime(context.Background(), jobIDs[i])
		suite.NoError(err)
		runtime.State = job.JobState(i)
		store.UpdateJobRuntime(context.Background(), jobIDs[i], runtime)
	}
	queryBuilder = store.DataStore.NewQuery()
	stmt = queryBuilder.Select("*").From(jobIndexTable).Where("expr(job_index_lucene, '{refresh:true}')")
	_, err = store.DataStore.Execute(context.Background(), stmt)
	suite.NoError(err)

	jobStates := []job.JobState{job.JobState_PENDING, job.JobState_RUNNING, job.JobState_SUCCEEDED}
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
		JobStates: jobStates,
	})

	suite.NoError(err)
	suite.Equal(len(jobStates), len(result1))
	suite.Equal(len(jobStates), int(total))

	jobStates = []job.JobState{job.JobState_PENDING, job.JobState_INITIALIZED, job.JobState_RUNNING, job.JobState_SUCCEEDED}
	result1, total, err = jobStore.QueryJobs(context.Background(), nil, &job.QuerySpec{
		Labels: []*peloton.Label{
			{Key: keyCommon, Value: valCommon},
		},
		JobStates: jobStates,
	})

	suite.NoError(err)
	suite.Equal(len(jobStates), len(result1))
	suite.Equal(len(jobStates), int(total))

}

func (suite *CassandraStoreTestSuite) TestCreateGetJobConfig() {
	var jobStore storage.JobStore
	jobStore = store
	var originalJobs []*job.JobConfig
	var records = 1
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	// Wait for 5 * 50ms for DB to be cleaned up
	var maxAttempts = 5
	for i := 0; i < records; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
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
		var labels = []*peloton.Label{
			{Key: keys[0], Value: vals[0]},
			{Key: keys[1], Value: vals[1]},
			{Key: keys[2], Value: vals[2]},
		}
		// Add some special label to job0 and job1
		if i < 2 {
			labels = append(labels, &peloton.Label{Key: keys[3], Value: vals[3]})
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
			Labels:        labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := suite.createJob(context.Background(), &jobID, &jobconfig, "uber")
		suite.NoError(err)

		// Create job with same job id would be no op
		jobconfig.Labels = nil
		jobconfig.Name = "random"
		err = suite.createJob(context.Background(), &jobID, &jobconfig, "uber2")
		suite.Error(err)

		var jobconf *job.JobConfig
		jobconf, err = jobStore.GetJobConfig(context.Background(), &jobID)
		suite.NoError(err)
		suite.Equal(jobconf.Name, fmt.Sprintf("TestJob_%d", i))
		suite.Equal(len(jobconf.Labels), 4)

		suite.NoError(jobStore.DeleteJob(context.Background(), &jobID))

		for i := 0; i < maxAttempts; i++ {
			jobconf, err = jobStore.GetJobConfig(context.Background(), &jobID)
			if err != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		suite.EqualError(err, fmt.Sprintf("Cannot find job wth jobID %s", jobID.GetValue()))
		suite.Nil(jobconf)

		var jobRuntime *job.RuntimeInfo
		for i = 0; i < maxAttempts; i++ {
			jobRuntime, err = jobStore.GetJobRuntime(context.Background(), &jobID)
			if err != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		suite.EqualError(err, fmt.Sprintf("Cannot find job wth jobID %s", jobID.GetValue()))
		suite.Nil(jobRuntime)

		tasks, err := store.GetTasksForJob(context.Background(), &jobID)
		suite.Len(tasks, 0)
		suite.NoError(err)
	}
}

func (suite *CassandraStoreTestSuite) TestFrameworkInfo() {
	var frameworkStore storage.FrameworkInfoStore
	frameworkStore = store
	err := frameworkStore.SetMesosFrameworkID(context.Background(), "framework1", "12345")
	suite.NoError(err)
	var frameworkID string
	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework2")
	suite.Error(err)

	err = frameworkStore.SetMesosStreamID(context.Background(), "framework1", "s-12345")
	suite.NoError(err)

	frameworkID, err = frameworkStore.GetFrameworkID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "12345")

	frameworkID, err = frameworkStore.GetMesosStreamID(context.Background(), "framework1")
	suite.NoError(err)
	suite.Equal(frameworkID, "s-12345")
}

func (suite *CassandraStoreTestSuite) TestAddTasks() {
	var taskStore storage.TaskStore
	taskStore = store
	var nJobs = 3
	var nTasks = uint32(3)
	var jobIDs []*peloton.JobID
	var jobs []*job.JobConfig
	for i := 0; i < nJobs; i++ {
		var jobID = peloton.JobID{Value: uuid.New()}
		jobIDs = append(jobIDs, &jobID)
		jobConfig := createJobConfig()
		jobConfig.Name = fmt.Sprintf("TestAddTasks_%d", i)
		jobs = append(jobs, jobConfig)
		err := suite.createJob(context.Background(), &jobID, jobConfig, "uber")
		suite.NoError(err)

		// For each job, create 3 tasks
		for j := uint32(0); j < nTasks; j++ {
			taskInfo := createTaskInfo(jobConfig, &jobID, j)
			taskInfo.Runtime.State = task.TaskState(j)
			err = taskStore.CreateTaskRuntime(context.Background(), &jobID, j, taskInfo.Runtime, "test")
			suite.NoError(err)
			// Create same task should error
			err = taskStore.CreateTaskRuntime(context.Background(), &jobID, j, taskInfo.Runtime, "test")
			suite.Error(err)
			err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, j, taskInfo.Runtime)
			suite.NoError(err)
		}
	}
	// List all tasks by job
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
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
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			task.Runtime.Host = fmt.Sprintf("compute_%d", i)
			err := store.UpdateTaskRuntime(context.Background(), task.JobId, task.InstanceId, task.Runtime)
			suite.NoError(err)
		}
	}
	for i := 0; i < nJobs; i++ {
		tasks, err := taskStore.GetTasksForJob(context.Background(), jobIDs[i])
		suite.NoError(err)
		suite.Equal(len(tasks), 3)
		for _, task := range tasks {
			suite.Equal(task.Runtime.Host, fmt.Sprintf("compute_%d", i))
		}
	}

	for i := 0; i < nJobs; i++ {
		for j := 0; j < int(nTasks); j++ {
			taskID := fmt.Sprintf("%s-%d", jobIDs[i].Value, j)
			taskInfo, err := taskStore.GetTaskByID(context.Background(), taskID)
			suite.NoError(err)
			suite.Equal(taskInfo.JobId.Value, jobIDs[i].Value)
			suite.Equal(taskInfo.InstanceId, uint32(j))

			var taskMap map[uint32]*task.TaskInfo
			taskMap, err = taskStore.GetTaskForJob(context.Background(), jobIDs[i], uint32(j))
			suite.NoError(err)
			taskInfo = taskMap[uint32(j)]
			suite.Equal(taskInfo.JobId.Value, jobIDs[i].Value)
			suite.Equal(taskInfo.InstanceId, uint32(j))
		}
		// TaskID does not exist
	}
	task, err := taskStore.GetTaskByID(context.Background(), "taskdoesnotexist")
	suite.Error(err)
	suite.Nil(task)
}

/* Disable the test as we need to temporary disable LWT for C* writes. See T1176379

func (suite *CassandraStoreTestSuite) TestUpdateTaskConcurrently() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}
	before := time.Now()

	suite.NoError(store.createTaskConfig(context.Background(), jobID, 0, &task.TaskConfig{}, 0))
	suite.NoError(store.CreateTaskRuntime(context.Background(), jobID, 0, &task.RuntimeInfo{}, ""))

	c := 25

	var wg sync.WaitGroup
	wg.Add(c)
	// Let `c` number of go-routines race around updating the jobID, and see that
	// eventually exactly `c` writes was done.
	for i := 0; i < c; i++ {
		go func() {
			for {
				info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
				suite.NoError(err)
				if err := store.UpdateTaskRuntime(context.Background(), jobID, 0, info.Runtime); err == nil {
					wg.Done()
					return
				} else if !yarpcerrors.IsAlreadyExists(err) {
					suite.Fail(err.Error())
				}
			}
		}()
	}

	wg.Wait()

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)

	suite.Equal(uint64(c), info.GetRuntime().GetRevision().GetVersion())
	suite.True(info.GetRuntime().GetRevision().UpdatedAt >= uint64(before.UnixNano()))
}
*/

func (suite *CassandraStoreTestSuite) TestTaskVersionMigration() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	// Create legacy task with missing version field.
	suite.NoError(store.createTaskConfig(context.Background(), jobID, 0, &task.TaskConfig{}, 0))
	queryBuilder := store.DataStore.NewQuery()
	stmt := queryBuilder.Insert(taskRuntimeTable).
		Columns(
			"job_id",
			"instance_id").
		Values(
			jobID.GetValue(),
			0).
		IfNotExist()
	suite.NoError(store.applyStatement(context.Background(), stmt, ""))

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetVersion())
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetCreatedAt())
	suite.Equal(uint64(0), info.GetRuntime().GetRevision().GetUpdatedAt())

	before := time.Now()
	suite.NoError(store.UpdateTaskRuntime(context.Background(), jobID, 0, info.Runtime))

	info, err = store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint64(1), info.GetRuntime().GetRevision().GetVersion())
	suite.True(info.GetRuntime().GetRevision().UpdatedAt >= uint64(before.UnixNano()))

	suite.NoError(store.UpdateTaskRuntime(context.Background(), jobID, 0, info.Runtime))

	info, err = store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)
	suite.Equal(uint64(2), info.GetRuntime().GetRevision().GetVersion())
}

// TestCreateTasks ensures mysql task create batching works as expected.
func (suite *CassandraStoreTestSuite) TestCreateTasks() {
	jobTasks := map[string]int{
		uuid.New(): 10,
		uuid.New(): store.Conf.MaxBatchSize,
		uuid.New(): store.Conf.MaxBatchSize*3 + 10,
		uuid.New(): store.Conf.MaxParallelBatches*store.Conf.MaxBatchSize + 4,
		uuid.New(): store.Conf.MaxParallelBatches*store.Conf.MaxBatchSize - 4,
		uuid.New(): store.Conf.MaxParallelBatches * store.Conf.MaxBatchSize,
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
			Name:          jobID.GetValue(),
			OwningTeam:    "team6",
			LdapGroups:    []string{"money", "team6", "otto"},
			Sla:           &sla,
			DefaultConfig: &taskConfig,
		}
		err := store.CreateJob(context.Background(), &jobID, &jobConfig, "uber")
		suite.NoError(err)

		// now, create a mess of tasks
		taskRuntimes := []*task.RuntimeInfo{}
		for j := 0; j < nTasks; j++ {
			tID := fmt.Sprintf("%s-%d", jobID.GetValue(), j)
			var runtime = &task.RuntimeInfo{
				MesosTaskId: &mesos.TaskID{Value: &tID},
				State:       task.TaskState(j),
			}
			taskRuntimes = append(taskRuntimes, runtime)
		}
		err = store.CreateTaskRuntimes(context.Background(), &jobID, taskRuntimes, "test")
		suite.NoError(err)
	}

	// List all tasks by job, ensure they were created properly, and
	// have the right parent
	for jobID, nTasks := range jobTasks {
		job := peloton.JobID{Value: jobID}
		tasks, err := store.GetTasksForJob(context.Background(), &job)
		suite.NoError(err)
		suite.Equal(nTasks, len(tasks))
		for _, task := range tasks {
			suite.Equal(jobID, task.JobId.Value)
		}
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskStateChanges() {
	var taskStore storage.TaskStore
	taskStore = store
	nTasks := 2
	host1 := "compute1"
	host2 := "compute2"
	var jobID = peloton.JobID{Value: uuid.New()}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err := suite.createJob(context.Background(), &jobID, jobConfig, "uber")
	suite.NoError(err)

	taskInfo := createTaskInfo(jobConfig, &jobID, 0)
	err = taskStore.CreateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime, "test")
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_PENDING
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_RUNNING
	taskInfo.Runtime.Host = host1
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_PREEMPTING
	taskInfo.Runtime.Host = ""
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_RUNNING
	taskInfo.Runtime.Host = host2
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_SUCCEEDED
	taskInfo.Runtime.Host = host2
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, 0, taskInfo.Runtime)
	suite.NoError(err)

	taskInfo.Runtime.State = task.TaskState_LOST
	taskInfo.Runtime.Host = host2
	taskInfo.Runtime.Reason = "Reconciliation reason"
	taskInfo.Runtime.Message = "Reconciliation message"
	err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, taskInfo.InstanceId, taskInfo.Runtime)
	suite.NoError(err)

	stateRecords, err := store.GetTaskStateChanges(context.Background(), &jobID, 0)
	suite.NoError(err)

	suite.Equal(stateRecords[0].TaskState, task.TaskState_INITIALIZED.String())
	suite.Equal(stateRecords[1].TaskState, task.TaskState_PENDING.String())
	suite.Equal(stateRecords[2].TaskState, task.TaskState_RUNNING.String())
	suite.Equal(stateRecords[3].TaskState, task.TaskState_PREEMPTING.String())
	suite.Equal(stateRecords[4].TaskState, task.TaskState_RUNNING.String())
	suite.Equal(stateRecords[5].TaskState, task.TaskState_SUCCEEDED.String())
	suite.Equal(stateRecords[6].TaskState, task.TaskState_LOST.String())

	suite.Equal(stateRecords[0].TaskHost, "")
	suite.Equal(stateRecords[1].TaskHost, "")
	suite.Equal(stateRecords[2].TaskHost, host1)
	suite.Equal(stateRecords[3].TaskHost, "")
	suite.Equal(stateRecords[4].TaskHost, host2)
	suite.Equal(stateRecords[5].TaskHost, host2)
	suite.Equal(stateRecords[6].TaskHost, host2)

	suite.Empty(stateRecords[5].Reason)
	suite.Empty(stateRecords[5].Message)
	suite.Equal(stateRecords[6].Reason, "Reconciliation reason")
	suite.Equal(stateRecords[6].Message, "Reconciliation message")

	stateRecords, err = store.GetTaskStateChanges(context.Background(), &jobID, 99999)
	suite.Error(err)
}

func (suite *CassandraStoreTestSuite) TestGetAllJobs() {
	jobs := []string{uuid.New(), uuid.New()}
	respoolPagination := uuid.New()
	respoolPagination = respoolPagination[:len(respoolPagination)-1]
	for i, job := range jobs {
		var jobID = peloton.JobID{Value: job}
		jobConfig := createJobConfig()
		jobConfig.Name = jobID.GetValue()
		jobConfig.RespoolID = &peloton.ResourcePoolID{
			Value: fmt.Sprintf("%s%d", respoolPagination, i),
		}
		err := suite.createJob(context.Background(), &jobID, jobConfig, "uber")
		suite.Nil(err)
	}
	all, err := store.GetAllJobs(context.Background())
	suite.Nil(err)
	for i, job := range jobs {
		r := all[job]
		suite.NotNil(r)
		cfg, err := store.GetJobConfig(context.Background(), &peloton.JobID{Value: job})
		suite.NoError(err)
		suite.Equal(respoolPagination+strconv.Itoa(i), cfg.RespoolID.GetValue())
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskStateSummary() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(2 * len(task.TaskState_name))
	err := suite.createJob(context.Background(), &jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := uint32(0); i < uint32(2*len(task.TaskState_name)); i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTaskRuntime(context.Background(), &jobID, i, taskInfo.Runtime, "user1")
		suite.Nil(err)
		taskInfo.Runtime.State = task.TaskState(i / 2)
		err = taskStore.UpdateTaskRuntime(context.Background(), &jobID, i, taskInfo.Runtime)
		suite.Nil(err)
	}

	taskStateSummary, err := store.GetTaskStateSummaryForJob(context.Background(), &jobID)
	suite.Nil(err)
	suite.Equal(len(taskStateSummary), len(task.TaskState_name))
	for _, state := range task.TaskState_name {
		suite.Equal(taskStateSummary[state], uint32(2))
	}
}

func (suite *CassandraStoreTestSuite) TestGetTaskByRange() {
	var taskStore storage.TaskStore
	taskStore = store
	var jobID = peloton.JobID{Value: uuid.New()}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(100)
	err := suite.createJob(context.Background(), &jobID, jobConfig, "user1")
	suite.Nil(err)

	for i := uint32(0); i < jobConfig.InstanceCount; i++ {
		taskInfo := createTaskInfo(jobConfig, &jobID, i)
		err := taskStore.CreateTaskRuntime(context.Background(), &jobID, i, taskInfo.Runtime, "user1")
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
	jobConfig, err := store.GetJobConfig(context.Background(), jobID)
	suite.NoError(err)

	if to > int(jobConfig.InstanceCount) {
		to = int(jobConfig.InstanceCount - 1)
	}
	r := &task.InstanceRange{
		From: uint32(from),
		To:   uint32(to),
	}
	var taskInRange map[uint32]*task.TaskInfo
	taskInRange, err = taskStore.GetTasksForJobByRange(context.Background(), jobID, r)
	suite.NoError(err)

	suite.Equal(to-from, len(taskInRange))
	for i := from; i < to; i++ {
		tID := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		suite.Equal(tID, *(taskInRange[uint32(i)].Runtime.MesosTaskId.Value))
	}

	var tasks []*task.TaskInfo
	tasks, n, err := taskStore.QueryTasks(context.Background(), jobID, &task.QuerySpec{
		Pagination: &query.PaginationSpec{
			Offset: uint32(from),
			Limit:  uint32(to - from + 1),
		},
	})
	suite.NoError(err)
	suite.Equal(n, jobConfig.InstanceCount)

	for i := from; i < to; i++ {
		tID := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		suite.Equal(tID, *(tasks[uint32(i-from)].Runtime.MesosTaskId.Value))
	}

	tasks, n, err = taskStore.QueryTasks(context.Background(), jobID, &task.QuerySpec{})
	suite.NoError(err)
	suite.Equal(jobConfig.InstanceCount, n)
	suite.Equal(int(_defaultQueryLimit), len(tasks))

	for i, t := range tasks {
		tID := fmt.Sprintf("%s-%d", jobID.GetValue(), i)
		suite.Equal(tID, *(t.Runtime.MesosTaskId.Value))
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
			expectedErr:    errors.New("code:already-exists message:first is not applied, item could exist already"),
			msg:            "testcase: create resource pool, duplicate ID",
		},
	}

	for _, tc := range testCases {
		actualErr := resourcePoolStore.CreateResourcePool(context.Background(), &peloton.ResourcePoolID{Value: tc.resourcePoolID},
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
		resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		resourcePoolConfig := createResourcePoolConfig()
		resourcePoolConfig.Name = resourcePoolID.Value
		err := resourcePoolStore.CreateResourcePool(context.Background(), resourcePoolID, resourcePoolConfig, _resPoolOwner)
		suite.Nil(err)
	}

	resourcePools, actualErr := resourcePoolStore.GetAllResourcePools(context.Background())
	suite.NoError(actualErr)
	suite.Len(resourcePools, nResourcePools)
}

func (suite *CassandraStoreTestSuite) GetAllResourcePoolsEmptyResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 0
	resourcePools, actualErr := resourcePoolStore.GetAllResourcePools(context.Background())
	suite.NoError(actualErr)
	suite.Len(resourcePools, nResourcePools)
}

func (suite *CassandraStoreTestSuite) TestGetResourcePoolsByOwner() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	nResourcePools := 2

	// todo move to setup once ^^^ issue resolves
	for i := 0; i < nResourcePools; i++ {
		resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d", _resPoolOwner, i)}
		resourcePoolConfig := createResourcePoolConfig()
		resourcePoolConfig.Name = resourcePoolID.Value
		err := resourcePoolStore.CreateResourcePool(context.Background(), resourcePoolID, resourcePoolConfig, _resPoolOwner)
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
		resourcePools, actualErr := resourcePoolStore.GetResourcePoolsByOwner(context.Background(), tc.owner)
		if tc.expectedErr == nil {
			suite.Nil(actualErr, tc.msg)
			suite.Len(resourcePools, tc.nResourcePools, tc.msg)
		} else {
			suite.EqualError(actualErr, tc.expectedErr.Error(), tc.msg)
		}
	}
}

func (suite *CassandraStoreTestSuite) TestUpdateResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d",
		"UpdateRespool", 1)}
	resourcePoolConfig := createResourcePoolConfig()

	resourcePoolConfig.Name = resourcePoolID.Value
	err := resourcePoolStore.CreateResourcePool(context.Background(),
		resourcePoolID, resourcePoolConfig, "Update")
	suite.NoError(err)

	resourcePoolConfig.Description = "Updated description"
	err = resourcePoolStore.UpdateResourcePool(context.Background(),
		resourcePoolID, resourcePoolConfig)
	suite.NoError(err)

	resourcePools, err := resourcePoolStore.GetResourcePoolsByOwner(context.Background(),
		"Update")
	suite.NoError(err)
	suite.Equal("Updated description", resourcePools[resourcePoolID.Value].Description)
}

func (suite *CassandraStoreTestSuite) TestDeleteResourcePool() {
	var resourcePoolStore storage.ResourcePoolStore
	resourcePoolStore = store
	resourcePoolID := &peloton.ResourcePoolID{Value: fmt.Sprintf("%s%d", "DeleteRespool", 1)}
	resourcePoolConfig := createResourcePoolConfig()
	resourcePoolConfig.Name = resourcePoolID.Value
	err := resourcePoolStore.CreateResourcePool(context.Background(), resourcePoolID, resourcePoolConfig, "Delete")
	suite.Nil(err)
	err = resourcePoolStore.DeleteResourcePool(context.Background(), resourcePoolID)
	suite.Nil(err)
}

func (suite *CassandraStoreTestSuite) TestJobRuntime() {
	var jobStore = store
	nTasks := 20

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: uuid.New()}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(nTasks)
	err := suite.createJob(context.Background(), &jobID, jobConfig, "uber")
	suite.NoError(err)

	runtime, err := jobStore.GetJobRuntime(context.Background(), &jobID)
	suite.NoError(err)
	suite.Equal(job.JobState_INITIALIZED, runtime.State)
	suite.Equal(1, len(runtime.TaskStats))
	suite.Equal(jobConfig.InstanceCount, runtime.TaskStats[task.TaskState_INITIALIZED.String()])

	// update job runtime
	runtime.State = job.JobState_RUNNING
	runtime.TaskStats[task.TaskState_PENDING.String()] = 5
	runtime.TaskStats[task.TaskState_PLACED.String()] = 5
	runtime.TaskStats[task.TaskState_RUNNING.String()] = 5
	runtime.TaskStats[task.TaskState_SUCCEEDED.String()] = 5

	err = jobStore.UpdateJobRuntime(context.Background(), &jobID, runtime)
	suite.NoError(err)

	runtime, err = jobStore.GetJobRuntime(context.Background(), &jobID)
	suite.NoError(err)
	suite.Equal(job.JobState_RUNNING, runtime.State)
	suite.Equal(5, len(runtime.TaskStats))

	jobIds, err := store.GetJobsByStates(context.Background(), []job.JobState{job.JobState_RUNNING})
	suite.NoError(err)
	idFound := false
	for _, id := range jobIds {
		if id == jobID {
			idFound = true
		}
	}
	suite.True(idFound)

	jobIds, err = store.GetJobsByStates(context.Background(), []job.JobState{120})
	suite.NoError(err)
	suite.Equal(0, len(jobIds))
}

func (suite *CassandraStoreTestSuite) TestJobConfig() {
	var jobStore = store
	oldInstanceCount := 20
	newInstanceCount := 50

	// CreateJob should create the default job runtime
	var jobID = peloton.JobID{Value: uuid.New()}
	jobConfig := createJobConfig()
	jobConfig.InstanceCount = uint32(oldInstanceCount)
	err := suite.createJob(context.Background(), &jobID, jobConfig, "uber")
	suite.NoError(err)

	jobRuntime, err := jobStore.GetJobRuntime(context.Background(), &jobID)
	suite.NoError(err)
	suite.Equal(jobConfig.InstanceCount, jobRuntime.TaskStats[task.TaskState_INITIALIZED.String()])

	jobConfig, err = jobStore.GetJobConfig(context.Background(), &jobID)
	suite.NoError(err)
	suite.Equal(uint32(oldInstanceCount), jobConfig.InstanceCount)

	// update instance count
	jobConfig.InstanceCount = uint32(newInstanceCount)
	err = jobStore.UpdateJobConfig(context.Background(), &jobID, jobConfig)
	suite.NoError(err)

	jobConfig, err = jobStore.GetJobConfig(context.Background(), &jobID)
	suite.NoError(err)
	suite.Equal(uint32(newInstanceCount), jobConfig.InstanceCount)
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
	err := volumeStore.CreatePersistentVolume(context.Background(), pv)
	suite.NoError(err)

	volumeID1 := &peloton.VolumeID{
		Value: "volume1",
	}
	rpv, err := volumeStore.GetPersistentVolume(context.Background(), volumeID1)
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
	volumeID2 := &peloton.VolumeID{
		Value: "volume2",
	}
	_, err = volumeStore.GetPersistentVolume(context.Background(), volumeID2)
	suite.Error(err)

	err = volumeStore.UpdatePersistentVolume(context.Background(), volumeID1, volume.VolumeState_CREATED)
	suite.NoError(err)

	// Verfy updated persistent volume info.
	rpv, err = volumeStore.GetPersistentVolume(context.Background(), volumeID1)
	suite.NoError(err)
	suite.Equal(rpv.Id.Value, "volume1")
	suite.Equal(rpv.State.String(), "CREATED")
	suite.Equal(rpv.State.String(), "CREATED")
	suite.Equal(rpv.JobId.Value, "job")
	suite.Equal(rpv.InstanceId, uint32(0))
	suite.Equal(rpv.Hostname, "host")
	suite.Equal(rpv.SizeMB, uint32(10))
	suite.Equal(rpv.ContainerPath, "testpath")

	err = volumeStore.DeletePersistentVolume(context.Background(), volumeID1)
	suite.NoError(err)

	// Verify volume has been deleted.
	_, err = volumeStore.GetPersistentVolume(context.Background(), volumeID1)
	suite.Error(err)
}

func (suite *CassandraStoreTestSuite) TestUpgrade() {
	id := &upgrade.WorkflowID{
		Value: uuid.New(),
	}
	suite.NoError(store.CreateUpgrade(context.Background(), id, &upgrade.UpgradeSpec{
		JobId: &peloton.JobID{
			Value: uuid.New(),
		},
	}))
	processing, progress, err := store.GetWorkflowProgress(context.Background(), id)
	suite.Empty(processing)
	suite.Equal(uint32(0), progress)
	suite.NoError(err)

	// Cannot process task 2.
	suite.EqualError(store.AddTaskToProcessing(context.Background(), id, 2),
		fmt.Sprintf("code:already-exists message:%v is not applied, item could exist already", id.Value))
	processing, progress, err = store.GetWorkflowProgress(context.Background(), id)
	suite.Empty(processing)
	suite.Equal(uint32(0), progress)
	suite.NoError(err)

	// Processing task 2 should succeed.
	suite.NoError(store.AddTaskToProcessing(context.Background(), id, 0))
	processing, progress, err = store.GetWorkflowProgress(context.Background(), id)
	suite.Equal([]uint32{0}, processing)
	suite.Equal(uint32(1), progress)
	suite.NoError(err)

	suite.NoError(store.RemoveTaskFromProcessing(context.Background(), id, 2))
	processing, progress, err = store.GetWorkflowProgress(context.Background(), id)
	suite.Equal([]uint32{0}, processing)
	suite.Equal(uint32(1), progress)
	suite.NoError(err)

	suite.NoError(store.RemoveTaskFromProcessing(context.Background(), id, 0))
	processing, progress, err = store.GetWorkflowProgress(context.Background(), id)
	suite.Empty(processing)
	suite.Equal(uint32(1), progress)
	suite.NoError(err)
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
		DefaultConfig: &task.TaskConfig{},
	}
	return &jobConfig
}

func createTaskInfo(
	jobConfig *job.JobConfig, jobID *peloton.JobID, i uint32) *task.TaskInfo {

	var tID = fmt.Sprintf("%s-%d", jobID.GetValue(), i)
	var taskInfo = task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			MesosTaskId: &mesos.TaskID{Value: &tID},
			State:       task.TaskState_INITIALIZED,
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

func (suite *CassandraStoreTestSuite) TestGetTaskRuntime() {
	jobID := &peloton.JobID{Value: uuid.NewRandom().String()}

	suite.NoError(store.createTaskConfig(context.Background(), jobID, 0, &task.TaskConfig{}, 0))
	suite.NoError(store.CreateTaskRuntime(context.Background(), jobID, 0, &task.RuntimeInfo{}, ""))

	info, err := store.getTask(context.Background(), jobID.GetValue(), 0)
	suite.NoError(err)

	runtime, err := store.GetTaskRuntime(context.Background(), jobID, 0)
	suite.NoError(err)

	suite.Equal(info.Runtime, runtime)
}
