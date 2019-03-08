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

package volumesvc

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/volume"
	volume_svc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"

	"github.com/uber/peloton/pkg/storage"
	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"
)

const (
	_testVolumeID = "volumeID1"
	_testJobID    = "testJob"
)

type VolumeHandlerTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	testScope   tally.TestScope
	jobStore    *storage_mocks.MockJobStore
	taskStore   *storage_mocks.MockTaskStore
	volumeStore *storage_mocks.MockPersistentVolumeStore
	handler     *serviceHandler
}

func (suite *VolumeHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.volumeStore = storage_mocks.NewMockPersistentVolumeStore(suite.ctrl)
	suite.taskStore = storage_mocks.NewMockTaskStore(suite.ctrl)
	suite.jobStore = storage_mocks.NewMockJobStore(suite.ctrl)

	suite.handler = &serviceHandler{
		metrics:     NewMetrics(suite.testScope),
		taskStore:   suite.taskStore,
		volumeStore: suite.volumeStore,
	}
}

func (suite *VolumeHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
	log.Debug("tearing down")
}

func TestVolumeHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeHandlerTestSuite))
}

func (suite *VolumeHandlerTestSuite) TestGetVolume() {
	testPelotonVolumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	volumeInfo := &volume.PersistentVolumeInfo{}
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeID).
		AnyTimes().
		Return(volumeInfo, nil)

	resp, err := suite.handler.GetVolume(
		context.Background(),
		&volume_svc.GetVolumeRequest{
			Id: testPelotonVolumeID,
		},
	)
	suite.NoError(err)
	suite.Equal(volumeInfo, resp.GetResult())
}

func (suite *VolumeHandlerTestSuite) TestGetVolumeNotFound() {
	testPelotonVolumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeID).
		AnyTimes().
		Return(nil, &storage.VolumeNotFoundError{})

	_, err := suite.handler.GetVolume(
		context.Background(),
		&volume_svc.GetVolumeRequest{
			Id: testPelotonVolumeID,
		},
	)
	suite.Equal(err, errVolumeNotFound)
}

func (suite *VolumeHandlerTestSuite) TestListVolumes() {
	testPelotonVolumeID1 := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	testPelotonVolumeID2 := &peloton.VolumeID{
		Value: "testVolume2",
	}
	testPelotonVolumeIDNotExist := &peloton.VolumeID{
		Value: "randomeVolume",
	}
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[0] = &task.TaskInfo{}
	taskInfos[1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID: testPelotonVolumeID1,
		},
	}
	taskInfos[2] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID: testPelotonVolumeID2,
		},
	}
	taskInfos[3] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID: testPelotonVolumeIDNotExist,
		},
	}

	volumeInfo1 := &volume.PersistentVolumeInfo{
		Id: testPelotonVolumeID1,
	}
	volumeInfo2 := &volume.PersistentVolumeInfo{
		Id: testPelotonVolumeID2,
	}
	suite.taskStore.EXPECT().
		GetTasksForJob(context.Background(), testJobID).
		Return(taskInfos, nil)
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeID1).
		Return(volumeInfo1, nil)
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeID2).
		Return(volumeInfo2, nil)
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeIDNotExist).
		Return(nil, &storage.VolumeNotFoundError{})

	resp, err := suite.handler.ListVolumes(
		context.Background(),
		&volume_svc.ListVolumesRequest{
			JobId: testJobID,
		},
	)
	suite.NoError(err)
	suite.Equal(2, len(resp.GetVolumes()))
}

func (suite *VolumeHandlerTestSuite) TestListVolumesDBError() {
	testPelotonVolumeID1 := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[0] = &task.TaskInfo{}
	taskInfos[1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID: testPelotonVolumeID1,
		},
	}
	suite.taskStore.EXPECT().
		GetTasksForJob(context.Background(), testJobID).
		Return(taskInfos, nil)
	suite.volumeStore.EXPECT().
		GetPersistentVolume(context.Background(), testPelotonVolumeID1).
		Return(nil, errVolumeNotFound)

	resp, err := suite.handler.ListVolumes(
		context.Background(),
		&volume_svc.ListVolumesRequest{
			JobId: testJobID,
		},
	)
	suite.Error(err)
	suite.Nil(resp.GetVolumes())
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolume() {
	testPelotonVolumeID1 := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID:  testPelotonVolumeID1,
			State:     task.TaskState_KILLED,
			GoalState: task.TaskState_KILLED,
		},
	}

	volumeInfo1 := &volume.PersistentVolumeInfo{
		Id:         testPelotonVolumeID1,
		JobId:      testJobID,
		InstanceId: 1,
	}

	gomock.InOrder(
		suite.volumeStore.EXPECT().
			GetPersistentVolume(context.Background(), testPelotonVolumeID1).
			Return(volumeInfo1, nil),
		suite.taskStore.EXPECT().
			GetTaskRuntime(context.Background(), testJobID, uint32(1)).
			Return(taskInfos[1].Runtime, nil),
		suite.volumeStore.EXPECT().
			UpdatePersistentVolume(context.Background(), volumeInfo1).
			Return(nil),
	)

	_, err := suite.handler.DeleteVolume(
		context.Background(),
		&volume_svc.DeleteVolumeRequest{
			Id: testPelotonVolumeID1,
		},
	)
	suite.NoError(err)
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolumeInUse() {
	testPelotonVolumeID1 := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID:  testPelotonVolumeID1,
			State:     task.TaskState_RUNNING,
			GoalState: task.TaskState_KILLED,
		},
	}

	volumeInfo1 := &volume.PersistentVolumeInfo{
		Id:         testPelotonVolumeID1,
		JobId:      testJobID,
		InstanceId: 1,
	}

	gomock.InOrder(
		suite.volumeStore.EXPECT().
			GetPersistentVolume(context.Background(), testPelotonVolumeID1).
			Return(volumeInfo1, nil),
		suite.taskStore.EXPECT().
			GetTaskRuntime(context.Background(), testJobID, uint32(1)).
			Return(taskInfos[1].Runtime, nil),
	)

	_, err := suite.handler.DeleteVolume(
		context.Background(),
		&volume_svc.DeleteVolumeRequest{
			Id: testPelotonVolumeID1,
		},
	)
	suite.Error(err)
}

func (suite *VolumeHandlerTestSuite) TestInitServiceHandler() {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "test-service",
	})

	InitServiceHandler(
		dispatcher,
		suite.testScope,
		suite.jobStore,
		suite.taskStore,
		suite.volumeStore,
	)
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolumeNoVolumeID() {
	_, err := suite.handler.DeleteVolume(context.Background(),
		&volume_svc.DeleteVolumeRequest{})
	suite.Error(err)
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolumeGetVolumeFailure() {
	testPelotonVolumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	suite.volumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), testPelotonVolumeID).
		Return(nil, fmt.Errorf("test error"))
	_, err := suite.handler.DeleteVolume(context.Background(),
		&volume_svc.DeleteVolumeRequest{Id: testPelotonVolumeID})
	suite.Error(err)
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolumeGetTaskRuntimeFailure() {
	testPelotonVolumeID := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	jobID := &peloton.JobID{Value: _testJobID}
	suite.volumeStore.EXPECT().
		GetPersistentVolume(gomock.Any(), testPelotonVolumeID).
		Return(&volume.PersistentVolumeInfo{
			JobId:      jobID,
			InstanceId: 0,
		}, nil)
	suite.taskStore.EXPECT().
		GetTaskRuntime(gomock.Any(), jobID, uint32(0)).
		Return(nil, fmt.Errorf("test error"))
	_, err := suite.handler.DeleteVolume(context.Background(),
		&volume_svc.DeleteVolumeRequest{Id: testPelotonVolumeID})
	suite.Error(err)
}

func (suite *VolumeHandlerTestSuite) TestDeleteVolumeUpdateVolumeFailure() {
	testPelotonVolumeID1 := &peloton.VolumeID{
		Value: _testVolumeID,
	}
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}
	taskInfos := make(map[uint32]*task.TaskInfo)
	taskInfos[1] = &task.TaskInfo{
		Runtime: &task.RuntimeInfo{
			VolumeID:  testPelotonVolumeID1,
			State:     task.TaskState_KILLED,
			GoalState: task.TaskState_KILLED,
		},
	}

	volumeInfo1 := &volume.PersistentVolumeInfo{
		Id:         testPelotonVolumeID1,
		JobId:      testJobID,
		InstanceId: 1,
	}

	gomock.InOrder(
		suite.volumeStore.EXPECT().
			GetPersistentVolume(context.Background(), testPelotonVolumeID1).
			Return(volumeInfo1, nil),
		suite.taskStore.EXPECT().
			GetTaskRuntime(context.Background(), testJobID, uint32(1)).
			Return(taskInfos[1].Runtime, nil),
		suite.volumeStore.EXPECT().
			UpdatePersistentVolume(context.Background(), volumeInfo1).
			Return(fmt.Errorf("test err")),
	)

	_, err := suite.handler.DeleteVolume(
		context.Background(),
		&volume_svc.DeleteVolumeRequest{
			Id: testPelotonVolumeID1,
		},
	)
	suite.Error(err)
}

func (suite *VolumeHandlerTestSuite) TestListVolumesGetTasksFailure() {
	testJobID := &peloton.JobID{
		Value: _testJobID,
	}

	suite.taskStore.EXPECT().
		GetTasksForJob(context.Background(), testJobID).
		Return(nil, fmt.Errorf("test error"))

	_, err := suite.handler.ListVolumes(
		context.Background(),
		&volume_svc.ListVolumesRequest{
			JobId: testJobID,
		},
	)
	suite.Error(err)
}
