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

package mesos

import (
	"context"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"
	hostmgrmesosmocks "github.com/uber/peloton/pkg/hostmgr/mesos/mocks"
	mpbmocks "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/models"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type MesosManagerTestSuite struct {
	suite.Suite

	ctrl            *gomock.Controller
	podEventCh      chan *scalar.PodEvent
	hostEventCh     chan *scalar.HostEvent
	provider        *hostmgrmesosmocks.MockFrameworkInfoProvider
	schedulerClient *mpbmocks.MockSchedulerClient
	operatorClient  *mpbmocks.MockMasterOperatorClient
	mesosManager    *MesosManager
}

func (suite *MesosManagerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.provider = hostmgrmesosmocks.NewMockFrameworkInfoProvider(suite.ctrl)
	suite.schedulerClient = mpbmocks.NewMockSchedulerClient(suite.ctrl)
	suite.operatorClient = mpbmocks.NewMockMasterOperatorClient(suite.ctrl)
	suite.podEventCh = make(chan *scalar.PodEvent, 1000)
	suite.hostEventCh = make(chan *scalar.HostEvent, 1000)
	d := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonHostManager,
	})
	suite.mesosManager = NewMesosManager(
		d,
		suite.provider,
		suite.schedulerClient,
		suite.operatorClient,
		10*time.Second,
		60*time.Second,
		tally.NoopScope,
		suite.podEventCh,
		suite.hostEventCh,
	)
}

func (suite *MesosManagerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestMesosManagerTestSuite(t *testing.T) {
	suite.Run(t, new(MesosManagerTestSuite))
}

func (suite *MesosManagerTestSuite) TestMesosManagerStartStop() {
	// Agents method should be called at least once upon start
	suite.operatorClient.
		EXPECT().
		Agents().
		Return(nil, nil).
		MinTimes(1)

	suite.mesosManager.Start()
	suite.mesosManager.Stop()
}

func (suite *MesosManagerTestSuite) TestMesosManagerLaunchPodNoOffer() {
	testPodName := "test_pod"
	testHostName := "test_host"

	testPodSpec := newTestPelotonPodSpec(testPodName)
	_, err := suite.mesosManager.LaunchPods(
		context.Background(),
		[]*models.LaunchablePod{
			{PodId: &peloton.PodID{Value: testPodName}, Spec: testPodSpec},
		},
		testHostName,
	)
	suite.Error(err)
}

func (suite *MesosManagerTestSuite) TestMesosManagerLaunchPodSuccess() {
	testPodName := "bca875f5-322a-4439-b0c9-63e3cf9f982e-1-1"
	testHostName := "test_host"
	streamID := "streamID"
	frameID := "frameID"
	uuid1 := uuid.New()
	testPodSpec := newTestPelotonPodSpec(testPodName)

	// add enough resources in offer pool
	suite.mesosManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &testHostName,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
			},
		},
	})

	suite.provider.
		EXPECT().
		GetFrameworkID(gomock.Any()).
		Return(&mesos.FrameworkID{
			Value: &frameID,
		})
	suite.provider.
		EXPECT().
		GetMesosStreamID(gomock.Any()).
		Return(streamID)
	suite.schedulerClient.
		EXPECT().
		Call(streamID, gomock.Any()).
		Do(func(mesosStreamID string, call *sched.Call) {
			suite.Equal(call.GetType(), sched.Call_ACCEPT)
		}).
		Return(nil)

	launched, err := suite.mesosManager.LaunchPods(
		context.Background(),
		[]*models.LaunchablePod{
			{PodId: &peloton.PodID{Value: testPodName}, Spec: testPodSpec},
		},
		testHostName,
	)
	suite.NoError(err)
	suite.Equal(1, len(launched))
}

func (suite *MesosManagerTestSuite) TestMesosManagerKillPodSuccess() {
	podID := "test_pod"
	streamID := "streamID"
	frameID := "frameID"

	suite.provider.
		EXPECT().
		GetFrameworkID(gomock.Any()).
		Return(&mesos.FrameworkID{
			Value: &frameID,
		})
	suite.provider.
		EXPECT().
		GetMesosStreamID(gomock.Any()).
		Return(streamID)
	suite.schedulerClient.
		EXPECT().
		Call(streamID, gomock.Any()).
		Do(func(mesosStreamID string, call *sched.Call) {
			suite.Equal(call.GetType(), sched.Call_KILL)
		}).
		Return(nil)

	suite.NoError(suite.mesosManager.KillPod(context.Background(), podID))
}

func (suite *MesosManagerTestSuite) TestAckPodEvents() {
	expectedPodEvent := &scalar.PodEvent{
		Event:     &pbpod.PodEvent{},
		EventType: 0,
		EventID:   uuid.New(),
	}

	suite.mesosManager.AckPodEvent(expectedPodEvent)

	pe := <-suite.mesosManager.ackChannel
	suite.Equal(pe.EventID, expectedPodEvent.EventID)
}

func (suite *MesosManagerTestSuite) TestMesosManagerKillPodFail() {
	podID := "test_pod"
	streamID := "streamID"
	frameID := "frameID"

	suite.provider.
		EXPECT().
		GetFrameworkID(gomock.Any()).
		Return(&mesos.FrameworkID{
			Value: &frameID,
		})
	suite.provider.
		EXPECT().
		GetMesosStreamID(gomock.Any()).
		Return(streamID)
	suite.schedulerClient.
		EXPECT().
		Call(streamID, gomock.Any()).
		Do(func(mesosStreamID string, call *sched.Call) {
			suite.Equal(call.GetType(), sched.Call_KILL)
		}).
		Return(errors.New("test error"))

	suite.Error(suite.mesosManager.KillPod(context.Background(), podID))
}

func (suite *MesosManagerTestSuite) TestMesosManagerReoncileHosts() {
	suite.mesosManager.ReconcileHosts()
}

// TestNewMesosManagerOffersSingleOffer tests
// adding offers for the same host
func (suite *MesosManagerTestSuite) TestNewMesosManagerOffersSameHost() {
	host := "hostname1"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	suite.mesosManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
				{
					Resources: []*mesos.Resource{
						util.NewMesosResourceBuilder().
							WithName(common.MesosCPU).
							WithValue(2.0).
							Build(),
						util.NewMesosResourceBuilder().
							WithName(common.MesosMem).
							WithValue(300.0).
							Build(),
					},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid2},
				},
			},
		},
	})

	he := <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   3.0,
		MemMb: 400.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)
}

// TestNewMesosManagerOffersSingleOffer tests
// adding offers for multiple hosts
func (suite *MesosManagerTestSuite) TestNewMesosManagerOffersMultipleHost() {
	host1 := "hostname1"
	host2 := "hostname2"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	suite.mesosManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host1,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
				{
					Resources: []*mesos.Resource{
						util.NewMesosResourceBuilder().
							WithName(common.MesosCPU).
							WithValue(2.0).
							Build(),
						util.NewMesosResourceBuilder().
							WithName(common.MesosMem).
							WithValue(300.0).
							Build(),
					},
					Hostname: &host2,
					Id:       &mesos.OfferID{Value: &uuid2},
				},
			},
		},
	})

	he1 := <-suite.hostEventCh
	he2 := <-suite.hostEventCh

	var host1Event *scalar.HostEvent
	var host2Event *scalar.HostEvent

	if he1.GetHostInfo().GetHostName() == host1 {
		host1Event = he1
	} else if he2.GetHostInfo().GetHostName() == host1 {
		host1Event = he2
	} else {
		suite.Fail("no event from host 1 received")
	}

	if he1.GetHostInfo().GetHostName() == host2 {
		host2Event = he1
	} else if he2.GetHostInfo().GetHostName() == host2 {
		host2Event = he2
	} else {
		suite.Fail("no event from host 2 received")
	}

	suite.Equal(host1Event.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(host1Event.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   1.0,
		MemMb: 100.0,
	})
	suite.Equal(host1Event.GetHostInfo().GetHostName(), host1)

	suite.Equal(host2Event.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(host2Event.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   2.0,
		MemMb: 300.0,
	})
	suite.Equal(host2Event.GetHostInfo().GetHostName(), host2)
}

// TestNewMesosManagerRescindOffer tests normal cases of rescinding offers
func (suite *MesosManagerTestSuite) TestNewMesosManagerRescindOffer() {
	// First, add offers to the host
	host := "hostname1"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	suite.mesosManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
				{
					Resources: []*mesos.Resource{
						util.NewMesosResourceBuilder().
							WithName(common.MesosCPU).
							WithValue(2.0).
							Build(),
						util.NewMesosResourceBuilder().
							WithName(common.MesosMem).
							WithValue(300.0).
							Build(),
					},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid2},
				},
			},
		},
	})

	he := <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   3.0,
		MemMb: 400.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)

	// Second, rescind the first offer
	suite.mesosManager.Rescind(context.Background(), &sched.Event{
		Rescind: &sched.Event_Rescind{
			OfferId: &mesos.OfferID{Value: &uuid1},
		},
	})
	he = <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   2.0,
		MemMb: 300.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)

	// Finally, rescind the second offer
	suite.mesosManager.Rescind(context.Background(), &sched.Event{
		Rescind: &sched.Event_Rescind{
			OfferId: &mesos.OfferID{Value: &uuid2},
		},
	})
	he = <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   0.0,
		MemMb: 0.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)
}

// TestNewMesosManagerRescindOffer tests rescinding nonexistent offers
func (suite *MesosManagerTestSuite) TestNewMesosManagerRescindNonexistentOffer() {
	// First, add offers to the host
	host := "hostname1"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	suite.mesosManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
			},
		},
	})

	he := <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   1.0,
		MemMb: 100.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)

	// Second, rescind the first offer
	suite.mesosManager.Rescind(context.Background(), &sched.Event{
		Rescind: &sched.Event_Rescind{
			OfferId: &mesos.OfferID{Value: &uuid1},
		},
	})
	he = <-suite.hostEventCh
	suite.Equal(he.GetEventType(), scalar.UpdateHostAvailableRes)
	suite.Equal(he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   0.0,
		MemMb: 0.0,
	})
	suite.Equal(he.GetHostInfo().GetHostName(), host)

	// Third, rescind the first offer again.
	// Should be a noop without event sent
	suite.mesosManager.Rescind(context.Background(), &sched.Event{
		Rescind: &sched.Event_Rescind{
			OfferId: &mesos.OfferID{Value: &uuid1},
		},
	})
	select {
	case <-suite.hostEventCh:
		suite.Fail("no event should be sent for " +
			"rescinding offer already rescinded")
	default:
	}

	// Fourth, rescind the offer never seen.
	// Should be a noop without event sent
	suite.mesosManager.Rescind(context.Background(), &sched.Event{
		Rescind: &sched.Event_Rescind{
			OfferId: &mesos.OfferID{Value: &uuid2},
		},
	})
	select {
	case <-suite.hostEventCh:
		suite.Fail("no event should be sent for " +
			"rescinding offer never seen")
	default:
	}
}

// TestNewMesosManagerStatusUpdates tests receiving task status update events.
func (suite *MesosManagerTestSuite) TestNewMesosManagerStatusUpdates() {
	host1 := "hostname1"
	uuid1 := uuid.New()
	state := mesos.TaskState_TASK_STARTING
	eventID := []byte{201, 117, 104, 168, 54, 76, 69, 143, 185, 116, 159, 95, 198, 94, 162, 38}

	status := &mesos.TaskStatus{
		TaskId: &mesos.TaskID{
			Value: &uuid1,
		},
		State: &state,
		AgentId: &mesos.AgentID{
			Value: &host1,
		},
		Uuid: eventID,
	}

	suite.mesosManager.Update(context.Background(), &sched.Event{
		Update: &sched.Event_Update{
			Status: status,
		},
	})

	pe := <-suite.podEventCh

	suite.Equal(pe.EventType, scalar.UpdatePod)
	suite.Equal(pe.EventID, string(eventID))
	suite.Equal(pe.Event.GetHostname(), host1)
	suite.Equal(pe.Event.GetAgentId(), host1)
	suite.Equal(pe.Event.GetPodId().GetValue(), uuid1)
	suite.Equal(
		pe.Event.GetActualState(),
		pbpod.PodState_POD_STATE_STARTING.String(),
	)
}

func newTestPelotonPodSpec(podName string) *pbpod.PodSpec {
	return &pbpod.PodSpec{
		Containers: []*pbpod.ContainerSpec{
			{
				Name: podName,
				Resource: &pbpod.ResourceSpec{
					CpuLimit:   1.0,
					MemLimitMb: 100.0,
				},
				Ports: []*pbpod.PortSpec{
					{
						Name:  "http",
						Value: 8080,
					},
				},
				Image: "test_image",
			},
		},
	}
}
