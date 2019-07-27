// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package host

import (
	"fmt"
	"testing"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	host_mocks "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc/mocks"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	res_mocks "github.com/uber/peloton/pkg/resmgr/respool/mocks"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/eventstream"
	"github.com/uber/peloton/pkg/common/lifecycle"
	preemption_mocks "github.com/uber/peloton/pkg/resmgr/preemption/mocks"
	rm_task "github.com/uber/peloton/pkg/resmgr/task"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

const (
	hostname      = "testHostname"
	drainerPeriod = 1 * time.Second
)

type DrainerTestSuite struct {
	suite.Suite
	mockCtrl           *gomock.Controller
	tracker            rm_task.Tracker
	drainer            Drainer
	preemptor          *preemption_mocks.MockQueue
	mockHostmgr        *host_mocks.MockInternalHostServiceYARPCClient
	eventStreamHandler *eventstream.Handler
	hostnames          []string
}

func (suite *DrainerTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	rm_task.InitTaskTracker(tally.NoopScope, &rm_task.Config{})
	suite.tracker = rm_task.GetTracker()
}

func (suite *DrainerTestSuite) SetupTest() {
	suite.mockHostmgr = host_mocks.NewMockInternalHostServiceYARPCClient(suite.mockCtrl)

	suite.eventStreamHandler = eventstream.NewEventStreamHandler(
		1000,
		[]string{
			common.PelotonJobManager,
			common.PelotonResourceManager,
		},
		nil,
		tally.Scope(tally.NoopScope))

	suite.preemptor = preemption_mocks.NewMockQueue(suite.mockCtrl)

	suite.drainer = Drainer{
		drainerPeriod:   drainerPeriod,
		hostMgrClient:   suite.mockHostmgr,
		preemptionQueue: suite.preemptor,
		rmTracker:       suite.tracker,
		lifecycle:       lifecycle.NewLifeCycle(),
	}

	jobID := uuid.New()
	taskID := fmt.Sprintf("%s-%d", jobID, 0)
	mesosTaskID := fmt.Sprintf("%s-%d-1", jobID, 0)
	t := &resmgr.Task{
		Name:     taskID,
		JobId:    &peloton.JobID{Value: jobID},
		Id:       &peloton.TaskID{Value: taskID},
		TaskId:   &mesos_v1.TaskID{Value: &mesosTaskID},
		Hostname: hostname,
	}

	suite.addTaskToTracker(t)
	suite.hostnames = []string{hostname}
}

func (suite *DrainerTestSuite) addTaskToTracker(t *resmgr.Task) {
	mockRespool := res_mocks.NewMockResPool(suite.mockCtrl)
	mockRespool.EXPECT().GetPath().Return("mockRespoolPath")
	suite.tracker.AddTask(
		t, suite.eventStreamHandler,
		mockRespool,
		&rm_task.Config{})
}

func TestDrainer(t *testing.T) {
	suite.Run(t, new(DrainerTestSuite))
}

func (suite *DrainerTestSuite) TearDownTest() {
	suite.tracker.Clear()
}

func (suite *DrainerTestSuite) TestDrainer_Init() {
	r := NewDrainer(
		tally.NoopScope,
		suite.mockHostmgr,
		drainerPeriod,
		suite.tracker,
		suite.preemptor)
	suite.NotNil(r)
}

func (suite *DrainerTestSuite) TestDrainer_StartStop() {
	defer func() {
		suite.drainer.Stop()
		_, ok := <-suite.drainer.lifecycle.StopCh()
		suite.False(ok)

		// Stopping drainer again should be no-op
		err := suite.drainer.Stop()
		suite.NoError(err)
	}()
	err := suite.drainer.Start()
	suite.NoError(err)
	suite.NotNil(suite.drainer.lifecycle.StopCh())

	// Starting drainer again should be no-op
	err = suite.drainer.Start()
	suite.NoError(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_EnqueueError() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: suite.hostnames,
		}, nil)
	suite.preemptor.EXPECT().
		EnqueueTasks(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("fake Enqueue error"))
	err := suite.drainer.performDrainCycle()
	suite.Error(err)
}

func (suite *DrainerTestSuite) TestDrainCycle() {
	suite.tracker.Clear()

	suite.preemptor.EXPECT().
		EnqueueTasks(gomock.Any(), gomock.Any()).
		Return(nil).Times(2)

	// simulate 2 cycles
	for i := 0; i < 2; i++ {
		// add tasks to tracker
		hostname := fmt.Sprintf("hostname-%d", i)
		jobID := uuid.New()
		taskID := fmt.Sprintf("%s-%d", jobID, i)
		suite.addTaskToTracker(&resmgr.Task{
			Name:     taskID,
			JobId:    &peloton.JobID{Value: jobID},
			Id:       &peloton.TaskID{Value: taskID},
			TaskId:   &mesos_v1.TaskID{Value: &[]string{fmt.Sprintf("%s-%d-1", jobID, i)}[0]},
			Hostname: hostname,
		})
		hosts := []string{hostname}

		suite.mockHostmgr.EXPECT().
			GetDrainingHosts(gomock.Any(), gomock.Any()).
			Return(&hostsvc.GetDrainingHostsResponse{
				Hostnames: hosts,
			}, nil).Times(1)

		err := suite.drainer.performDrainCycle()
		suite.NoError(err)
	}
}
func (suite *DrainerTestSuite) TestDrainCycle_NoHostsToDrain() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{}, nil)
	err := suite.drainer.performDrainCycle()
	suite.NoError(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_GetDrainingHostsError() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(nil, fmt.Errorf("fake GetDrainingHosts error"))
	err := suite.drainer.performDrainCycle()
	suite.Error(err)
}

func (suite *DrainerTestSuite) TestDrainCycle_MarkHostsDrained() {
	suite.mockHostmgr.EXPECT().
		GetDrainingHosts(gomock.Any(), gomock.Any()).
		Return(&hostsvc.GetDrainingHostsResponse{
			Hostnames: []string{"dummyhost"},
		}, nil)
	suite.mockHostmgr.EXPECT().
		MarkHostDrained(gomock.Any(), gomock.Any()).
		Return(&hostsvc.MarkHostDrainedResponse{}, nil)
	err := suite.drainer.performDrainCycle()
	suite.NoError(err)
}
