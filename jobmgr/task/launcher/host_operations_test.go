package launcher

import (
	"fmt"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/util"
)

type HostOperationTestSuite struct {
	suite.Suite

	launchOperation   *hostsvc.OfferOperation
	reserveOperation  *hostsvc.OfferOperation
	createOperation   *hostsvc.OfferOperation
	reservedResources []*mesos.Resource
}

func (suite *HostOperationTestSuite) SetupTest() {
}

func (suite *HostOperationTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestOperationTestSuite(t *testing.T) {
	suite.Run(t, new(HostOperationTestSuite))
}

func (suite *HostOperationTestSuite) TestGetHostOperations() {
	operationTypes := []hostsvc.OfferOperation_Type{
		hostsvc.OfferOperation_RESERVE,
		hostsvc.OfferOperation_CREATE,
		hostsvc.OfferOperation_LAUNCH,
	}
	testTask := createStatefulTask(0)
	launchableTasks := []*hostsvc.LaunchableTask{createLaunchableTasks([]*task.TaskInfo{testTask})[0]}
	hostOffer := &hostsvc.HostOffer{
		Hostname: fmt.Sprintf("hostname-%d", "host0"),
		AgentId: &mesos.AgentID{
			Value: util.PtrPrintf(fmt.Sprintf("agent-%d", "host0")),
		},
	}
	placement := createPlacements(testTask, hostOffer)
	operationsFactory := NewHostOperationsFactory(launchableTasks, placement)

	hostOperations, err := operationsFactory.GetHostOperations(operationTypes)

	suite.NoError(err)
	suite.Equal(3, len(hostOperations))
	reserveOp := hostOperations[0]
	createOp := hostOperations[1]
	launchOp := hostOperations[2]
	suite.Equal(
		hostsvc.OfferOperation_RESERVE,
		reserveOp.GetType())
	suite.Equal(
		hostsvc.OfferOperation_CREATE,
		createOp.GetType())
	suite.Equal(
		hostsvc.OfferOperation_LAUNCH,
		launchOp.GetType())
	reserve := reserveOp.GetReserve()
	suite.Equal(4, len(reserve.GetResources()))
	launch := launchOp.GetLaunch()
	suite.NotNil(launch)
	suite.Equal(1, len(launch.GetTasks()))
	suite.Equal(
		fmt.Sprintf(taskIDFmt, 0),
		launch.GetTasks()[0].GetTaskId().GetValue())
}

func (suite *HostOperationTestSuite) TestGetHostOperationsLaunchOnly() {
	operationTypes := []hostsvc.OfferOperation_Type{
		hostsvc.OfferOperation_LAUNCH,
	}
	testTask := createStatefulTask(0)
	launchableTasks := []*hostsvc.LaunchableTask{createLaunchableTasks([]*task.TaskInfo{testTask})[0]}
	hostOffer := &hostsvc.HostOffer{
		Hostname: fmt.Sprintf("hostname-%d", "host0"),
		AgentId: &mesos.AgentID{
			Value: util.PtrPrintf(fmt.Sprintf("agent-%d", "host0")),
		},
	}
	placement := createPlacements(testTask, hostOffer)
	operationsFactory := NewHostOperationsFactory(launchableTasks, placement)

	hostOperations, err := operationsFactory.GetHostOperations(operationTypes)

	suite.NoError(err)
	suite.Equal(1, len(hostOperations))
	launchOp := hostOperations[0]
	suite.Equal(
		hostsvc.OfferOperation_LAUNCH,
		launchOp.GetType())
	launch := launchOp.GetLaunch()
	suite.NotNil(launch)
	suite.Equal(1, len(launch.GetTasks()))
	suite.Equal(
		fmt.Sprintf(taskIDFmt, 0),
		launch.GetTasks()[0].GetTaskId().GetValue())
}

func (suite *HostOperationTestSuite) TestGetHostOperationsReserveNoPorts() {
	operationTypes := []hostsvc.OfferOperation_Type{
		hostsvc.OfferOperation_RESERVE,
		hostsvc.OfferOperation_CREATE,
		hostsvc.OfferOperation_LAUNCH,
	}
	testTask := createStatefulTask(0)
	launchableTasks := []*hostsvc.LaunchableTask{createLaunchableTasks([]*task.TaskInfo{testTask})[0]}
	hostOffer := &hostsvc.HostOffer{
		Hostname: fmt.Sprintf("hostname-%d", "host0"),
		AgentId: &mesos.AgentID{
			Value: util.PtrPrintf(fmt.Sprintf("agent-%d", "host0")),
		},
	}
	placement := createPlacements(testTask, hostOffer)
	placement.Ports = []uint32{}
	operationsFactory := NewHostOperationsFactory(launchableTasks, placement)

	hostOperations, err := operationsFactory.GetHostOperations(operationTypes)

	suite.NoError(err)
	suite.Equal(3, len(hostOperations))
	reserveOp := hostOperations[0]
	createOp := hostOperations[1]
	launchOp := hostOperations[2]
	suite.Equal(
		hostsvc.OfferOperation_RESERVE,
		reserveOp.GetType())
	suite.Equal(
		hostsvc.OfferOperation_CREATE,
		createOp.GetType())
	suite.Equal(
		hostsvc.OfferOperation_LAUNCH,
		launchOp.GetType())
	reserve := reserveOp.GetReserve()
	suite.Equal(3, len(reserve.GetResources()))
	for _, res := range reserve.GetResources() {
		suite.NotEqual(res.GetName(), "ports")
	}
	launch := launchOp.GetLaunch()
	suite.NotNil(launch)
	suite.Equal(1, len(launch.GetTasks()))
	suite.Equal(
		fmt.Sprintf(taskIDFmt, 0),
		launch.GetTasks()[0].GetTaskId().GetValue())
}

func createStatefulTask(instanceID int) *task.TaskInfo {
	testTask := createTestTask(instanceID)
	testTask.GetConfig().Volume = &task.PersistentVolumeConfig{
		ContainerPath: "testpath",
		SizeMB:        10,
	}
	return testTask
}
