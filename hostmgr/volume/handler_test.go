package volume

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/volume/svc"

	"code.uber.internal/infra/peloton/storage"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

const (
	_testVolumeID = "volumeID1"
	_testJobID    = "testJob"
)

type HostmgVolumeHandlerTestSuite struct {
	suite.Suite

	ctrl        *gomock.Controller
	testScope   tally.TestScope
	taskStore   *storage_mocks.MockTaskStore
	volumeStore *storage_mocks.MockPersistentVolumeStore
	handler     *serviceHandler
}

func (suite *HostmgVolumeHandlerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.volumeStore = storage_mocks.NewMockPersistentVolumeStore(suite.ctrl)
	suite.taskStore = storage_mocks.NewMockTaskStore(suite.ctrl)

	suite.handler = &serviceHandler{
		metrics:     NewMetrics(suite.testScope),
		taskStore:   suite.taskStore,
		volumeStore: suite.volumeStore,
	}
}

func (suite *HostmgVolumeHandlerTestSuite) TearDownTest() {
	log.Debug("tearing down")
}

func TestHostmgVolumeHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(HostmgVolumeHandlerTestSuite))
}

func (suite *HostmgVolumeHandlerTestSuite) TestGetVolume() {
	defer suite.ctrl.Finish()

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

func (suite *HostmgVolumeHandlerTestSuite) TestGetVolumeNotFound() {
	defer suite.ctrl.Finish()

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

func (suite *HostmgVolumeHandlerTestSuite) TestListVolumes() {
	defer suite.ctrl.Finish()

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

func (suite *HostmgVolumeHandlerTestSuite) TestListVolumesDBError() {
	defer suite.ctrl.Finish()

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

func (suite *HostmgVolumeHandlerTestSuite) TestDeleteVolume() {
	defer suite.ctrl.Finish()

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

func (suite *HostmgVolumeHandlerTestSuite) TestDeleteVolumeInUse() {
	defer suite.ctrl.Finish()

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
