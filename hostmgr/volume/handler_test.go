package volume

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/volume"
	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/volume/svc"

	"code.uber.internal/infra/peloton/storage"
	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
)

const (
	_testVolumeID = "volumeID1"
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
