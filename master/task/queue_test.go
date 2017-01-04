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
	"peloton/job"
	"peloton/task"
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
	suite.store = mysql.NewJobStore(conf.Conn, tally.NoopScope)

	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(masterPort)),
	}
	outbounds := transport.Outbounds{
		Unary: http.NewOutbound("http://localhost:" + strconv.Itoa(masterPort) + config.FrameworkURLPath),
	}
	yOutbounds := yarpc.Outbounds{
		"peloton-master": outbounds,
	}
	suite.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-master",
		Inbounds:  inbounds,
		Outbounds: yOutbounds,
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
	tq := InitTaskQueue(suite.dispatcher, &mtx)

	// Create jobs. each with different
	suite.createJob(0, 10, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(1, 7, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(2, 2, 10, task.RuntimeInfo_SUCCEEDED)
	suite.createJob(3, 2, 10, task.RuntimeInfo_INITIALIZED)

	tq.LoadFromDB(suite.store, suite.store)

	// 1. All jobs should have 10 tasks in DB
	tasks, err := suite.store.GetTasksForJob(&job.JobID{Value: "TestJob_0"})
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&job.JobID{Value: "TestJob_1"})
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&job.JobID{Value: "TestJob_2"})
	suite.NoError(err)
	suite.Equal(len(tasks), 10)
	tasks, err = suite.store.GetTasksForJob(&job.JobID{Value: "TestJob_3"})
	suite.NoError(err)
	suite.Equal(len(tasks), 10)

	// 2. check the queue content
	contentSummary := getQueueContent(tq)
	suite.Equal(len(contentSummary["TestJob_1"]), 3)
	suite.Equal(len(contentSummary["TestJob_2"]), 8)
	suite.Equal(len(contentSummary["TestJob_3"]), 10)
	suite.Equal(len(contentSummary), 3)
}

func (suite *QueueTestSuite) createJob(i int, tasks int, totalTasks int, taskState task.RuntimeInfo_TaskState) {
	var jobID = job.JobID{Value: fmt.Sprintf("TestJob_%v", i)}
	var sla = job.SlaConfig{
		Preemptible: false,
	}
	var jobConfig = job.JobConfig{
		Name:          fmt.Sprintf("TestJob_%v", i),
		OwningTeam:    "team6",
		LdapGroups:    []string{"money", "team6", "otto"},
		Sla:           &sla,
		InstanceCount: uint32(totalTasks),
	}
	var err = suite.store.CreateJob(&jobID, &jobConfig, "uber")
	suite.NoError(err)
	for j := 0; j < tasks; j++ {
		var tID = fmt.Sprintf("%s-%d", jobID.Value, j)
		var taskInfo = task.TaskInfo{
			Runtime: &task.RuntimeInfo{
				TaskId: &mesos.TaskID{Value: &tID},
				State:  taskState,
			},
			JobConfig:  &jobConfig,
			InstanceId: uint32(j),
			JobId:      &jobID,
		}
		err = suite.store.CreateTask(&jobID, j, &taskInfo, "test")
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
