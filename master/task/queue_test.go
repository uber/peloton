package task

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/master/config"
	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/task"
)

var masterPort = 47960

type QueueTestSuite struct {
	suite.Suite
	store      *mysql.JobStore
	db         *sqlx.DB
	dispatcher yarpc.Dispatcher
}

func (suite *QueueTestSuite) SetupTest() {
	conf := mysql.LoadConfigWithDB()
	suite.db = conf.Conn
	suite.store = mysql.NewJobStore(*conf, tally.NoopScope)

	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(masterPort)),
	}
	url := "http://localhost:" + strconv.Itoa(masterPort) + config.FrameworkURLPath
	outbounds := transport.Outbounds{
		Unary: http.NewOutbound(url),
	}
	suite.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:     "peloton-master",
		Inbounds: inbounds,
		Outbounds: yarpc.Outbounds{
			"peloton-master": outbounds,
		},
	})
	suite.dispatcher.Start()
}

func (suite *QueueTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonTaskQueue(t *testing.T) {
	suite.Run(t, new(QueueTestSuite))
}

func (suite *QueueTestSuite) TestRefillTaskQueue() {
	mtx := metrics.New(tally.NoopScope)
	tq := InitTaskQueue(suite.dispatcher, &mtx, suite.store, suite.store)

	// Create jobs. each with different number of tasks
	jobs := [4]job.JobID{}
	for i := 0; i < 4; i++ {
		jobs[i] = job.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}
	suite.createJob(&jobs[0], 10, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(&jobs[1], 7, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(&jobs[2], 2, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(&jobs[3], 2, 10, task.RuntimeInfo_INITIALIZED)

	tq.LoadFromDB()

	// 1. All jobs should have 10 tasks in DB
	tasks, err := suite.store.GetTasksForJob(&jobs[0])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&jobs[1])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&jobs[2])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&jobs[3])
	suite.NoError(err)
	suite.Equal(len(tasks), 10)

	// 2. check the queue content
	contentSummary := getQueueContent(tq)
	suite.Equal(len(contentSummary["TestJob_1"]), 3)
	suite.Equal(len(contentSummary["TestJob_2"]), 8)
	suite.Equal(len(contentSummary["TestJob_3"]), 10)
	suite.Equal(len(contentSummary), 3)
}

func (suite *QueueTestSuite) createJob(
	jobID *job.JobID,
	numTasks uint32,
	instanceCount uint32,
	taskState task.RuntimeInfo_TaskState) {

	sla := job.SlaConfig{
		Preemptible: false,
	}
	var jobConfig = job.JobConfig{
		Name:          jobID.Value,
		Sla:           &sla,
		InstanceCount: instanceCount,
	}
	var err = suite.store.CreateJob(jobID, &jobConfig, "uber")
	suite.NoError(err)
	for i := uint32(0); i < numTasks; i++ {
		var taskID = fmt.Sprintf("%s-%d", jobID.Value, i)
		var taskInfo = task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				TaskId: &mesos.TaskID{Value: &taskID},
				State:  taskState,
			},
			Config:     jobConfig.GetDefaultConfig(),
			InstanceId: i,
			JobId:      jobID,
		}
		err = suite.store.CreateTask(jobID, i, &taskInfo, "test")
		suite.NoError(err)
	}
}

func getQueueContent(q *Queue) map[string]map[string]bool {
	var result = make(map[string]map[string]bool)
	for {
		task := q.tqValue.Load().(util.TaskQueue).GetTask(1 * time.Millisecond)
		if task != nil {
			jobID := task.JobId.Value
			taskID := task.Runtime.TaskId.Value
			_, ok := result[jobID]
			if !ok {
				result[jobID] = make(map[string]bool)
			}
			result[jobID][*taskID] = true
		} else {
			break
		}
	}
	return result
}
