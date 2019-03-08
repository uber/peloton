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
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"

	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	storage_mocks "github.com/uber/peloton/pkg/storage/mocks"
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
				Name:                        _frameworkName,
				GPUSupported:                true,
				TaskKillingStateSupported:   false,
				PartitionAwareSupported:     false,
				RevocableResourcesSupported: false,
			},
			ZkPath:   _zkPath,
			Encoding: _encoding,
		},
		suite.store,
		http.Header{},
	).(*schedulerDriver)
}

func (suite *schedulerDriverTestSuite) TearDownTest() {
	log.Debug("tearing down")
	suite.ctrl.Finish()
}

func (suite *schedulerDriverTestSuite) TestGetAuthHeader() {
	testSecret := "test-secret"
	content := []byte(testSecret)
	tmpfile, err := ioutil.TempFile("", "secret")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name()) // clean up

	if _, err := tmpfile.Write(content); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	// No secret file name.
	config := Config{
		Framework: &FrameworkConfig{
			Principal: "test-principal",
		},
	}

	header, err := GetAuthHeader(&config, "")
	suite.NoError(err)
	suite.Empty(header.Get("Authorization"))

	header, err = GetAuthHeader(&config, tmpfile.Name())
	suite.NoError(err)
	encoded := "Basic dGVzdC1wcmluY2lwYWw6dGVzdC1zZWNyZXQ="
	suite.Equal(encoded, header.Get("Authorization"))
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
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return(value, nil)

	suite.Equal(frameworkID, suite.driver.GetFrameworkID(context.Background()))
	suite.Equal(frameworkID, suite.driver.frameworkID)
	suite.Equal(frameworkID, suite.driver.GetFrameworkID(context.Background()))
}

func (suite *schedulerDriverTestSuite) TestGetFrameworkIDError() {
	err := errors.New("test")

	suite.store.EXPECT().
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return("", err)

	suite.Nil(suite.driver.GetFrameworkID(context.Background()))

	suite.store.EXPECT().
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return("", nil)

	suite.Nil(suite.driver.GetFrameworkID(context.Background()))
}

func (suite *schedulerDriverTestSuite) TestGetStreamID() {
	// Before first call, cache is empty.
	suite.Empty(suite.driver.mesosStreamID)

	suite.store.EXPECT().
		GetMesosStreamID(context.Background(), _frameworkName).
		Return(_streamID, nil).
		Times(2)

	suite.Equal(_streamID, suite.driver.GetMesosStreamID(context.Background()))
	suite.Equal(_streamID, suite.driver.mesosStreamID)
	suite.Equal(_streamID, suite.driver.GetMesosStreamID(context.Background()))
}

func (suite *schedulerDriverTestSuite) TestGetStreamIDError() {
	err := errors.New("error stream id")
	// Before first call, cache is empty.
	suite.Empty(suite.driver.mesosStreamID)

	suite.store.EXPECT().
		GetMesosStreamID(context.Background(), _frameworkName).
		Return("", err)

	suite.Empty(suite.driver.GetMesosStreamID(context.Background()))
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
		SetMesosStreamID(context.Background(), _frameworkName, _streamID).
		Return(nil)

	suite.driver.PostSubscribe(context.Background(), _streamID)

	err := errors.New("error saving stream id")
	suite.store.EXPECT().
		SetMesosStreamID(context.Background(), _frameworkName, _streamID).
		Return(err)

	// TODO: Do something here.
	suite.driver.PostSubscribe(context.Background(), _streamID)
}

func (suite *schedulerDriverTestSuite) TestFrameworkInfoCapability() {
	value := _frameworkID
	suite.store.EXPECT().
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return(value, nil)

	subscribe, err := suite.driver.prepareSubscribe(context.Background())
	suite.Nil(err)
	// Only GPU_RESOURCES framework capability is enabled.
	suite.Equal(len(subscribe.Subscribe.FrameworkInfo.Capabilities), 1)

	// Enable TASK_KILLING_STATE, PARTITION_AWARE & REVOCABLE_RESOURCES also.
	suite.driver.cfg.TaskKillingStateSupported = true
	suite.driver.cfg.PartitionAwareSupported = true
	suite.driver.cfg.RevocableResourcesSupported = true
	subscribe, err = suite.driver.prepareSubscribe(context.Background())
	suite.Nil(err)
	// GPU_RESOURCES, TASK_KILLING_STATE, REVOCABLE_RESOURCES framework capability is enabled too.
	suite.Equal(len(subscribe.Subscribe.FrameworkInfo.Capabilities), 4)
}

func (suite *schedulerDriverTestSuite) TestPrepareLoadedFrameworkID() {
	req, err := suite.driver.PrepareSubscribeRequest(context.Background(), "")
	suite.Error(err)
	suite.Nil(req)

	value := _frameworkID

	suite.store.EXPECT().
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return(value, nil)

	req, err = suite.driver.PrepareSubscribeRequest(context.Background(), _hostPort)
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

func (suite *schedulerDriverTestSuite) TestPrepareFirstTimeRegister() {
	req, err := suite.driver.PrepareSubscribeRequest(context.Background(), "")
	suite.Error(err)
	suite.Nil(req)

	suite.store.EXPECT().
		GetFrameworkID(context.Background(), gomock.Eq(_frameworkName)).
		Return("", nil)

	req, err = suite.driver.PrepareSubscribeRequest(context.Background(), _hostPort)
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
	pc := call.Interface().(*sched.Call)
	suite.Equal(pelotonFrameworkID, pc.GetFrameworkId().GetValue())
}

func TestSchedulerDriverTestSuite(t *testing.T) {
	suite.Run(t, new(schedulerDriverTestSuite))
}
