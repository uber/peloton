package task

import (
	//"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	mesos "mesos/v1"
	"peloton/job"
	"peloton/task"
	"strconv"
	"testing"
	"time"
)

var masterPort = 47960

type QueueTestSuite struct {
	suite.Suite
	store      *mysql.MysqlJobStore
	db         *sqlx.DB
	dispatcher yarpc.Dispatcher
}

func (suite *QueueTestSuite) SetupTest() {
	conf := mysql.LoadConfigWithDB()
	suite.db = conf.Conn
	suite.store = mysql.NewMysqlJobStore(conf.Conn)

	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(masterPort)),
	}
	outbounds := transport.Outbounds{
		"peloton-master": http.NewOutbound("http://localhost:" + strconv.Itoa(masterPort)),
	}
	suite.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-master",
		Inbounds:  inbounds,
		Outbounds: outbounds,
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
	tq := TaskQueue{
		tQueue: util.NewMemLocalTaskQueue("sourceTaskQueue"),
	}
	json.Register(suite.dispatcher, json.Procedure("TaskQueue.Enqueue", tq.Enqueue))
	json.Register(suite.dispatcher, json.Procedure("TaskQueue.Dequeue", tq.Dequeue))

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
	contentSummary := getQueueContent(&tq)
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

func getQueueContent(q *TaskQueue) map[string]map[string]bool {
	var result = make(map[string]map[string]bool)
	for {
		task := q.tQueue.GetTask(1 * time.Millisecond)
		if task != nil {
			jobId := task.JobId.Value
			taskId := task.Runtime.TaskId.Value
			_, ok := result[jobId]
			if !ok {
				result[jobId] = make(map[string]bool)
			}
			result[jobId][*taskId] = true
		} else {
			break
		}
	}
	return result
}
