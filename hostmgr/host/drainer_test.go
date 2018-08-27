package host

import (
	"fmt"
	"testing"
	"time"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	mesos_maintenance "code.uber.internal/infra/peloton/.gen/mesos/v1/maintenance"
	mesos_master "code.uber.internal/infra/peloton/.gen/mesos/v1/master"

	"code.uber.internal/infra/peloton/common/lifecycle"
	mq_mocks "code.uber.internal/infra/peloton/hostmgr/queue/mocks"
	mpb_mocks "code.uber.internal/infra/peloton/yarpc/encoding/mpb/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

const (
	drainerPeriod = 100 * time.Millisecond
)

var (
	hostnames = []string{"testhost"}
)

type DrainerTestSuite struct {
	suite.Suite
	drainer                  *drainer
	mockCtrl                 *gomock.Controller
	mockMasterOperatorClient *mpb_mocks.MockMasterOperatorClient
	mockMaintenanceQueue     *mq_mocks.MockMaintenanceQueue
}

func (suite *DrainerTestSuite) SetupTest() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockMasterOperatorClient = mpb_mocks.NewMockMasterOperatorClient(suite.mockCtrl)
	suite.mockMaintenanceQueue = mq_mocks.NewMockMaintenanceQueue(suite.mockCtrl)
	suite.drainer = &drainer{
		drainerPeriod:        drainerPeriod,
		masterOperatorClient: suite.mockMasterOperatorClient,
		maintenanceQueue:     suite.mockMaintenanceQueue,
		lifecycle:            lifecycle.NewLifeCycle(),
	}
}

func (suite *DrainerTestSuite) TearDownTest() {
	suite.mockCtrl.Finish()
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(DrainerTestSuite))
}

//TestNewDrainer test creation of new host drainer
func (suite *DrainerTestSuite) TestNewDrainer() {
	drainer := NewDrainer(drainerPeriod,
		suite.mockMasterOperatorClient,
		suite.mockMaintenanceQueue)
	suite.NotNil(drainer)
}

// TestStart tests starting the host drainer
func (suite *DrainerTestSuite) TestStart() {
	response := mesos_master.Response_GetMaintenanceStatus{
		Status: &mesos_maintenance.ClusterStatus{
			DrainingMachines: []*mesos_maintenance.ClusterStatus_DrainingMachine{},
		},
	}
	for i := 0; i < len(hostnames); i++ {
		response.Status.DrainingMachines = append(
			response.Status.DrainingMachines, &mesos_maintenance.ClusterStatus_DrainingMachine{
				Id: &mesos.MachineID{
					Hostname: &hostnames[i],
				},
			})
	}
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)
	suite.mockMaintenanceQueue.EXPECT().
		Enqueue(hostnames).
		Return(nil).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	// Starting drainer again should be no-op
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()

	// Test GetMaintenanceStatus error
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(nil, fmt.Errorf("Fake GetMaintenanceStatus error")).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()

	// Test Enqueue error
	suite.mockMasterOperatorClient.EXPECT().
		GetMaintenanceStatus().
		Return(&response, nil).
		MinTimes(1).
		MaxTimes(2)
	suite.mockMaintenanceQueue.EXPECT().
		Enqueue(hostnames).
		Return(fmt.Errorf("Fake Enqueue error")).
		MinTimes(1).
		MaxTimes(2)
	suite.drainer.Start()
	time.Sleep(2 * drainerPeriod)
	suite.drainer.Stop()
}

// TestStop tests stopping the host drainer
func (suite *DrainerTestSuite) TestStop() {
	suite.drainer.Stop()
	<-suite.drainer.lifecycle.StopCh()
}
