// +build !unit

// FIXME: Use a store mock and remove above build tag!

package taskqueue

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	mesos "mesos/v1"
	"peloton/api/job"
	"peloton/api/peloton"
	"peloton/api/task"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/queue"
	"code.uber.internal/infra/peloton/storage/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

var masterPort = 47960

type ServiceHandlerTestSuite struct {
	suite.Suite
	store      *mysql.JobStore
	db         *sqlx.DB
	dispatcher yarpc.Dispatcher
}

func (suite *ServiceHandlerTestSuite) SetupTest() {
	conf := mysql.LoadConfigWithDB()
	suite.db = conf.Conn
	suite.store = mysql.NewJobStore(*conf, tally.NoopScope)

	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(masterPort)),
	}
	url := "http://localhost:" + strconv.Itoa(masterPort) + common.PelotonEndpointPath
	outbounds := transport.Outbounds{
		Unary: http.NewOutbound(url),
	}
	suite.dispatcher = yarpc.NewDispatcher(yarpc.Config{
		Name:     "peloton-resmgr",
		Inbounds: inbounds,
		Outbounds: yarpc.Outbounds{
			"peloton-resmgr": outbounds,
		},
	})
	suite.dispatcher.Start()
}

func (suite *ServiceHandlerTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonTaskQueue(t *testing.T) {
	suite.Run(t, new(ServiceHandlerTestSuite))
}

func (suite *ServiceHandlerTestSuite) TestRefillTaskQueue() {
	InitServiceHandler(suite.dispatcher, tally.NoopScope, suite.store, suite.store)

	// Create jobs. each with different number of tasks
	jobs := [4]peloton.JobID{}
	for i := 0; i < 4; i++ {
		jobs[i] = peloton.JobID{Value: fmt.Sprintf("TestJob_%d", i)}
	}
	suite.createJob(&jobs[0], 10, 10, task.TaskState_SUCCEEDED)
	suite.createJob(&jobs[1], 7, 10, task.TaskState_SUCCEEDED)
	suite.createJob(&jobs[2], 2, 10, task.TaskState_SUCCEEDED)
	suite.createJob(&jobs[3], 2, 10, task.TaskState_INITIALIZED)

	handler.LoadFromDB()

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
	contentSummary := getQueueContent(handler)
	suite.Equal(len(contentSummary["TestJob_1"]), 3)
	suite.Equal(len(contentSummary["TestJob_2"]), 8)
	suite.Equal(len(contentSummary["TestJob_3"]), 10)
	suite.Equal(len(contentSummary), 3)
}

func (suite *ServiceHandlerTestSuite) createJob(
	jobID *peloton.JobID,
	numTasks uint32,
	instanceCount uint32,
	taskState task.TaskState) {

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

func getQueueContent(h *serviceHandler) map[string]map[string]bool {
	var result = make(map[string]map[string]bool)
	for {
		item, err := h.tqValue.Load().(queue.Queue).Dequeue(10 * time.Millisecond)
		if err != nil {
			fmt.Printf("Failed to dequeue item: %v", err)
			break
		}
		task := item.(*task.TaskInfo)
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
