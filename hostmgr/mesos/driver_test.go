package mesos

import (
	"errors"
	"net/url"
	"reflect"
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	mesos "code.uber.internal/infra/peloton/.gen/mesos/v1"
	sched "code.uber.internal/infra/peloton/.gen/mesos/v1/scheduler"

	storage_mocks "code.uber.internal/infra/peloton/storage/mocks"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
)

const (
	_encoding      = mpb.ContentTypeJSON
	_zkPath        = "zkpath"
	_frameworkID   = "framework-id"
	_frameworkName = "framework-name"
	_streamID      = "stream_id"
	_hostPort      = "test-host:1234"
)

type schedulerDriverTestSuite struct {
	suite.Suite

	ctrl   *gomock.Controller
	store  *storage_mocks.MockFrameworkInfoStore
	driver *schedulerDriver
}

func (suite *schedulerDriverTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.store = storage_mocks.NewMockFrameworkInfoStore(suite.ctrl)
	suite.driver = InitSchedulerDriver(
		&Config{
			Framework: &FrameworkConfig{
				Name:         _frameworkName,
				GPUSupported: true,
				Checkpoint:   true,
			},
			ZkPath:   _zkPath,
			Encoding: _encoding,
		},
		suite.store,
	).(*schedulerDriver)
}

func (suite *schedulerDriverTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.ctrl.Finish()
}

func (suite *schedulerDriverTestSuite) TestGetInstance() {
	suite.Equal(suite.driver, GetSchedulerDriver())
}

func (suite *schedulerDriverTestSuite) TestGetFrameworkID() {
	value := _frameworkID
	frameworkID := &mesos.FrameworkID{
		Value: &value,
	}

	// Before first call, cache is nil.
	suite.Nil(suite.driver.frameworkID)

	suite.store.EXPECT().
		GetFrameworkID(gomock.Eq(_frameworkName)).
		Return(value, nil)

	suite.Equal(frameworkID, suite.driver.GetFrameworkID())
	suite.Equal(frameworkID, suite.driver.frameworkID)
	suite.Equal(frameworkID, suite.driver.GetFrameworkID())
}

func (suite *schedulerDriverTestSuite) TestGetFrameworkIDError() {
	err := errors.New("test")

	suite.store.EXPECT().
		GetFrameworkID(gomock.Eq(_frameworkName)).
		Return("", err)

	suite.Nil(suite.driver.GetFrameworkID())

	suite.store.EXPECT().
		GetFrameworkID(gomock.Eq(_frameworkName)).
		Return("", nil)

	suite.Nil(suite.driver.GetFrameworkID())
}

func (suite *schedulerDriverTestSuite) TestGetStreamID() {
	// Before first call, cache is empty.
	suite.Empty(suite.driver.mesosStreamID)

	suite.store.EXPECT().
		GetMesosStreamID(_frameworkName).
		Return(_streamID, nil).
		Times(2)

	suite.Equal(_streamID, suite.driver.GetMesosStreamID())
	suite.Equal(_streamID, suite.driver.mesosStreamID)
	suite.Equal(_streamID, suite.driver.GetMesosStreamID())
}

func (suite *schedulerDriverTestSuite) TestGetStreamIDError() {
	err := errors.New("error stream id")
	// Before first call, cache is empty.
	suite.Empty(suite.driver.mesosStreamID)

	suite.store.EXPECT().
		GetMesosStreamID(_frameworkName).
		Return("", err)

	suite.Empty(suite.driver.GetMesosStreamID())
	suite.Empty(suite.driver.mesosStreamID)
}

func (suite *schedulerDriverTestSuite) TestStaticMethods() {
	suite.Equal(ServiceName, suite.driver.Name())

	suite.Equal(
		url.URL{
			Scheme: serviceSchema,
			Path:   servicePath,
		},
		suite.driver.Endpoint())

	suite.Equal(reflect.TypeOf(sched.Event{}), suite.driver.EventDataType())
	suite.Equal(_encoding, suite.driver.GetContentEncoding())
}

func (suite *schedulerDriverTestSuite) TestPostSubscribe() {

	suite.store.EXPECT().
		SetMesosStreamID(_frameworkName, _streamID).
		Return(nil)

	suite.driver.PostSubscribe(_streamID)

	err := errors.New("error saving stream id")
	suite.store.EXPECT().
		SetMesosStreamID(_frameworkName, _streamID).
		Return(err)

	// TODO: Do something here.
	suite.driver.PostSubscribe(_streamID)
}

func (suite *schedulerDriverTestSuite) TestPrepareSubscribeRequest() {
	req, err := suite.driver.PrepareSubscribeRequest("")
	suite.Error(err)
	suite.Nil(req)

	value := _frameworkID

	suite.store.EXPECT().
		GetFrameworkID(gomock.Eq(_frameworkName)).
		Return(value, nil)

	req, err = suite.driver.PrepareSubscribeRequest(_hostPort)
	suite.NoError(err)
	suite.Equal("POST", req.Method)
	suite.Equal("http://test-host:1234/api/v1/scheduler", req.URL.String())
	suite.Contains(req.Header["Content-Type"], "application/json")
	p := make([]byte, 1000)
	n, err := req.Body.Read(p)
	suite.NotEmpty(n)
	suite.NoError(err)
	call := reflect.New(reflect.TypeOf(sched.Call{}))
	suite.NoError(mpb.UnmarshalPbMessage(p, call, _encoding))
}

func TestSchedulerDriverTestSuite(t *testing.T) {
	suite.Run(t, new(schedulerDriverTestSuite))
}
