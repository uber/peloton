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

package operation

import (
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common/util"
)

const (
	_perHostCPU         = 10.0
	_perHostMem         = 20.0
	_perHostReserveMem  = 11.0
	_perHostDisk        = 30.0
	_perHostReserveDisk = 12.0

	_testJobID  = "bca875f5-322a-4439-b0c9-63e3cf9f982e"
	_taskIDFmt  = _testJobID + "-%d-abcdef12-abcd-1234-5678-1234567890ab"
	_defaultCmd = "/bin/sh"
)

var (
	_testAgent    = "agent"
	_testKey      = "testKey"
	_testValue    = "testValue"
	_testOfferID  = "testOffer"
	_testVolumeID = "testVolume"
)

type OperationTestSuite struct {
	suite.Suite

	launchOperation   *hostsvc.OfferOperation
	reserveOperation  *hostsvc.OfferOperation
	createOperation   *hostsvc.OfferOperation
	reservedResources []*mesos.Resource
}

func (suite *OperationTestSuite) SetupTest() {
	suite.reserveOperation = &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_RESERVE,
		Reserve: &hostsvc.OfferOperation_Reserve{
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName("cpus").
					WithValue(_perHostCPU).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("mem").
					WithValue(_perHostReserveMem).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(_perHostReserveDisk).
					Build(),
			},
		},
		ReservationLabels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	suite.createOperation = &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_CREATE,
		Create: &hostsvc.OfferOperation_Create{
			Volume: &hostsvc.Volume{
				Resource: util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(1.0).
					Build(),
				ContainerPath: "test",
				Id: &peloton.VolumeID{
					Value: "volumeid",
				},
			},
		},
		ReservationLabels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	suite.launchOperation = &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_LAUNCH,
		Launch: &hostsvc.OfferOperation_Launch{
			Tasks: generateLaunchableTasks(1),
		},
		ReservationLabels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	reservation := &mesos.Resource_ReservationInfo{
		Labels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}
	diskInfo := &mesos.Resource_DiskInfo{
		Persistence: &mesos.Resource_DiskInfo_Persistence{
			Id: &_testVolumeID,
		},
	}
	suite.reservedResources = []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName("cpus").
			WithValue(_perHostCPU).
			WithRole(pelotonRole).
			WithReservation(reservation).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("mem").
			WithValue(_perHostMem).
			WithReservation(reservation).
			WithRole(pelotonRole).
			Build(),
		util.NewMesosResourceBuilder().
			WithName("disk").
			WithValue(_perHostDisk).
			WithRole(pelotonRole).
			WithReservation(reservation).
			WithDisk(diskInfo).
			Build(),
	}
}

func (suite *OperationTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestOperationTestSuite(t *testing.T) {
	suite.Run(t, new(OperationTestSuite))
}

func (suite *OperationTestSuite) TestGetOfferOperations() {
	operations := []*hostsvc.OfferOperation{
		suite.reserveOperation,
		suite.createOperation,
		suite.launchOperation,
	}
	operationsFactory := NewOfferOperationsFactory(
		operations,
		suite.reservedResources,
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	offerOperations, err := operationsFactory.GetOfferOperations()

	suite.NoError(err)
	suite.Equal(3, len(offerOperations))
	reserveOp := offerOperations[0]
	createOp := offerOperations[1]
	launchOp := offerOperations[2]
	suite.Equal(
		mesos.Offer_Operation_RESERVE,
		reserveOp.GetType())
	suite.Equal(
		mesos.Offer_Operation_CREATE,
		createOp.GetType())
	suite.Equal(
		mesos.Offer_Operation_LAUNCH,
		launchOp.GetType())
	launch := launchOp.GetLaunch()
	suite.NotNil(launch)
	suite.Equal(1, len(launch.GetTaskInfos()))
	suite.Equal(
		fmt.Sprintf(_taskIDFmt, 0),
		launch.GetTaskInfos()[0].GetTaskId().GetValue())
}

func (suite *OperationTestSuite) TestOfferOperationsReserveNotEnoughResources() {
	reserveOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_RESERVE,
		Reserve: &hostsvc.OfferOperation_Reserve{
			Resources: []*mesos.Resource{
				util.NewMesosResourceBuilder().
					WithName("cpus").
					WithValue(_perHostCPU).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("mem").
					WithValue(21.0).
					Build(),
				util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(12.0).
					Build(),
			},
		},
		ReservationLabels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}

	operations := []*hostsvc.OfferOperation{
		reserveOperation,
		suite.createOperation,
		suite.launchOperation,
	}
	operationsFactory := NewOfferOperationsFactory(
		operations,
		suite.reservedResources,
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	offerOperations, err := operationsFactory.GetOfferOperations()

	suite.Error(err)
	suite.Equal(0, len(offerOperations))
}

func (suite *OperationTestSuite) TestOfferOperationsLaunchNotEnoughResources() {
	launchOperation := &hostsvc.OfferOperation{
		Type: hostsvc.OfferOperation_LAUNCH,
		Launch: &hostsvc.OfferOperation_Launch{
			Tasks: generateLaunchableTasks(2),
		},
		ReservationLabels: &mesos.Labels{
			Labels: []*mesos.Label{
				{
					Key:   &_testKey,
					Value: &_testValue,
				},
			},
		},
	}

	operations := []*hostsvc.OfferOperation{
		suite.reserveOperation,
		suite.createOperation,
		launchOperation,
	}
	operationsFactory := NewOfferOperationsFactory(
		operations,
		suite.reservedResources,
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	offerOperations, err := operationsFactory.GetOfferOperations()

	suite.Error(err)
	suite.Equal(0, len(offerOperations))
}

func (suite *OperationTestSuite) TestGetOfferDestroyOperation() {
	operations := []*hostsvc.OfferOperation{
		{
			Type: hostsvc.OfferOperation_DESTROY,
			Destroy: &hostsvc.OfferOperation_Destroy{
				VolumeID: _testVolumeID,
			},
		},
	}
	testOffer := suite.createReservedMesosOffer(suite.reservedResources)
	operationsFactory := NewOfferOperationsFactory(
		operations,
		testOffer.GetResources(),
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	offerOperations, err := operationsFactory.GetOfferOperations()

	suite.NoError(err)
	suite.Equal(1, len(offerOperations))
	destroyOp := offerOperations[0]
	suite.Equal(
		mesos.Offer_Operation_DESTROY,
		destroyOp.GetType())
	suite.Equal(
		suite.reservedResources[2],
		destroyOp.GetDestroy().GetVolumes()[0])
}

func (suite *OperationTestSuite) TestGetOfferUnreserveOperation() {
	operations := []*hostsvc.OfferOperation{
		{
			Type: hostsvc.OfferOperation_UNRESERVE,
			Unreserve: &hostsvc.OfferOperation_Unreserve{
				Label: suite.reservedResources[0].GetReservation().GetLabels().String(),
			},
		},
	}
	testOffer := suite.createReservedMesosOffer(suite.reservedResources[:2])
	operationsFactory := NewOfferOperationsFactory(
		operations,
		testOffer.GetResources(),
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	offerOperations, err := operationsFactory.GetOfferOperations()

	suite.NoError(err)
	suite.Equal(1, len(offerOperations))
	unreserveOp := offerOperations[0]
	suite.Equal(
		mesos.Offer_Operation_UNRESERVE,
		unreserveOp.GetType())
	suite.Equal(
		suite.reservedResources[:2],
		unreserveOp.GetUnreserve().GetResources())
}

func (suite *OperationTestSuite) TestGetOfferInvalidUnreserveOperation() {
	operations := []*hostsvc.OfferOperation{
		{
			Type: hostsvc.OfferOperation_UNRESERVE,
		},
	}
	operationsFactory := NewOfferOperationsFactory(
		operations,
		suite.reservedResources,
		"hostname-0",
		&mesos.AgentID{
			Value: util.PtrPrintf("agent-0"),
		},
	)

	_, err := operationsFactory.GetOfferOperations()
	suite.Error(err)
}

func (suite *OperationTestSuite) createReservedMesosOffer(res []*mesos.Resource) *mesos.Offer {
	return &mesos.Offer{
		Id: &mesos.OfferID{
			Value: &_testOfferID,
		},
		AgentId: &mesos.AgentID{
			Value: &_testAgent,
		},
		Hostname:  &_testAgent,
		Resources: res,
	}
}

func generateLaunchableTasks(numTasks int) []*hostsvc.LaunchableTask {
	var tasks []*hostsvc.LaunchableTask
	for i := 0; i < numTasks; i++ {
		tid := fmt.Sprintf(_taskIDFmt, i)
		tmpCmd := _defaultCmd
		tasks = append(tasks, &hostsvc.LaunchableTask{
			TaskId: &mesos.TaskID{Value: &tid},
			Config: &task.TaskConfig{
				Name: fmt.Sprintf("name-%d", i),
				Resource: &task.ResourceConfig{
					CpuLimit:    _perHostCPU,
					MemLimitMb:  _perHostMem,
					DiskLimitMb: _perHostDisk,
				},
				Command: &mesos.CommandInfo{
					Value: &tmpCmd,
				},
			},
			Volume: &hostsvc.Volume{
				Resource: util.NewMesosResourceBuilder().
					WithName("disk").
					WithValue(1.0).
					Build(),
				ContainerPath: "test",
				Id: &peloton.VolumeID{
					Value: "volumeid",
				},
			},
		})
	}
	return tasks
}
