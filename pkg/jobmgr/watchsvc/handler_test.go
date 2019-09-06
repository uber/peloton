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

package watchsvc_test

import (
	"context"
	"errors"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	watchsvcmocks "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc/mocks"

	watchmocks "github.com/uber/peloton/pkg/jobmgr/watchsvc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"

	. "github.com/uber/peloton/pkg/jobmgr/watchsvc"
)

type WatchServiceHandlerTestSuite struct {
	suite.Suite

	handler *ServiceHandler

	ctx         context.Context
	ctrl        *gomock.Controller
	testScope   tally.TestScope
	processor   *watchmocks.MockWatchProcessor
	watchServer *watchsvcmocks.MockWatchServiceServiceWatchYARPCServer
}

func (suite *WatchServiceHandlerTestSuite) SetupTest() {
	suite.ctx = context.Background()
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.processor = watchmocks.NewMockWatchProcessor(suite.ctrl)
	suite.watchServer = watchsvcmocks.NewMockWatchServiceServiceWatchYARPCServer(suite.ctrl)

	suite.handler = NewServiceHandler(
		NewMetrics(suite.testScope),
		suite.processor,
	)
}

func (suite *WatchServiceHandlerTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestInitV1AlphaWatchServiceHandler tests InitV1AlphaWatchServiceHandler
// correctly initializes the service handler and watch processor
func (suite *WatchServiceHandlerTestSuite) TestInitV1AlphaWatchServiceHandler() {
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "test-service",
	})

	processor := InitV1AlphaWatchServiceHandler(
		dispatcher,
		suite.testScope,
		Config{},
	)
	suite.NotNil(processor)
}

// TestWatch_InvalidRequest checks Watch will return invalid-argument
// error when we don't pass any filter to the WatchRequest.
func (suite *WatchServiceHandlerTestSuite) TestWatch_InvalidRequest() {
	req := &watchsvc.WatchRequest{}

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsInvalidArgument(err))
}

// TestTaskWatch sets up a watch client, and verifies the responses
// are streamed back correctly based on the input, finally the
// test cancels the watch stream.
func (suite *WatchServiceHandlerTestSuite) TestTaskWatch() {
	watchID := NewWatchID(ClientTypeTask)
	taskClient := &TaskClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *pod.PodSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewTaskClient(gomock.Any()).
		Return(watchID, taskClient, nil)
	suite.processor.EXPECT().StopTaskClient(watchID)

	pods := []*pod.PodSummary{
		{
			PodName: &peloton.PodName{Value: "pod-0"},
		},
		{
			PodName: &peloton.PodName{Value: "pod-1"},
		},
	}

	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)
	for _, p := range pods {
		suite.watchServer.EXPECT().
			Send(&watchsvc.WatchResponse{
				WatchId: watchID,
				Pods:    []*pod.PodSummary{p},
			}).
			Return(nil)
	}

	req := &watchsvc.WatchRequest{
		PodFilter: &watch.PodFilter{},
	}

	go func() {
		for _, p := range pods {
			taskClient.Input <- p
		}
		// cancelling task watch
		taskClient.Signal <- StopSignalCancel
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsCancelled(err))
}

// TestTaskWatch_Overflow sets up a watch client, and verifies the responses
// are streamed back correctly based on the input, finally it simulates
// a buffer overflow signal and verifies the client received the aborted
// status code.
func (suite *WatchServiceHandlerTestSuite) TestTaskWatch_Overflow() {
	watchID := NewWatchID(ClientTypeTask)
	taskClient := &TaskClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *pod.PodSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewTaskClient(gomock.Any()).
		Return(watchID, taskClient, nil)
	suite.processor.EXPECT().StopTaskClient(watchID)

	pods := []*pod.PodSummary{
		{
			PodName: &peloton.PodName{Value: "pod-0"},
		},
		{
			PodName: &peloton.PodName{Value: "pod-1"},
		},
	}

	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)
	for _, p := range pods {
		suite.watchServer.EXPECT().
			Send(&watchsvc.WatchResponse{
				WatchId: watchID,
				Pods:    []*pod.PodSummary{p},
			}).
			Return(nil)
	}

	req := &watchsvc.WatchRequest{
		PodFilter: &watch.PodFilter{},
	}

	go func() {
		for _, p := range pods {
			taskClient.Input <- p
		}
		// simulate buffer overflow
		taskClient.Signal <- StopSignalOverflow
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsAborted(err))
}

// TestTaskWatch_MaxClientReached checks Watch will return resource-exhausted
// error when NewTaskClient reached max client.
func (suite *WatchServiceHandlerTestSuite) TestTaskWatch_MaxClientReached() {
	suite.processor.EXPECT().NewTaskClient(gomock.Any()).
		Return("", nil, yarpcerrors.ResourceExhaustedErrorf("max client reached"))

	req := &watchsvc.WatchRequest{
		PodFilter: &watch.PodFilter{},
	}

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsResourceExhausted(err))
}

// TestTaskWatch_InitSendError tests for error case of initial response.
func (suite *WatchServiceHandlerTestSuite) TestTaskWatch_InitSendError() {
	watchID := NewWatchID(ClientTypeTask)
	taskClient := &TaskClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *pod.PodSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewTaskClient(gomock.Any()).
		Return(watchID, taskClient, nil)
	suite.processor.EXPECT().StopTaskClient(watchID)

	sendErr := errors.New("message:transport is closing")

	// initial response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(sendErr)

	req := &watchsvc.WatchRequest{
		PodFilter: &watch.PodFilter{},
	}

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.Equal(sendErr, err)
}

// TestTaskWatch_InitSendError tests for error case of subsequent response
// after initial one.
func (suite *WatchServiceHandlerTestSuite) TestTaskWatch_SendError() {
	watchID := NewWatchID(ClientTypeTask)
	taskClient := &TaskClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *pod.PodSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewTaskClient(gomock.Any()).
		Return(watchID, taskClient, nil)
	suite.processor.EXPECT().StopTaskClient(watchID)

	p := &pod.PodSummary{
		PodName: &peloton.PodName{Value: "pod-0"},
	}

	// initial response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)

	sendErr := errors.New("message:transport is closing")

	// subsequent response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    []*pod.PodSummary{p},
		}).
		Return(sendErr)

	req := &watchsvc.WatchRequest{
		PodFilter: &watch.PodFilter{},
	}

	go func() {
		taskClient.Input <- p
		taskClient.Signal <- StopSignalCancel
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.Equal(sendErr, err)
}

// TestJobWatch sets up a watch client, and verifies the responses
// are streamed back correctly based on the input, finally the
// test cancels the watch stream.
func (suite *WatchServiceHandlerTestSuite) TestJobWatch() {
	watchID := NewWatchID(ClientTypeJob)
	jobClient := &JobClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *stateless.JobSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewJobClient(gomock.Any()).
		Return(watchID, jobClient, nil)
	suite.processor.EXPECT().StopJobClient(watchID)

	jobs := []*stateless.JobSummary{
		{
			JobId: &peloton.JobID{Value: "job-0"},
		},
		{
			JobId: &peloton.JobID{Value: "job-1"},
		},
	}

	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)
	for _, j := range jobs {
		suite.watchServer.EXPECT().
			Send(&watchsvc.WatchResponse{
				WatchId:       watchID,
				StatelessJobs: []*stateless.JobSummary{j},
			}).
			Return(nil)
	}

	req := &watchsvc.WatchRequest{
		StatelessJobFilter: &watch.StatelessJobFilter{},
	}

	go func() {
		for _, j := range jobs {
			jobClient.Input <- j
		}
		// cancelling task watch
		jobClient.Signal <- StopSignalCancel
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsCancelled(err))
}

// TestJobWatch_Overflow sets up a watch client, and verifies the responses
// are streamed back correctly based on the input, finally it simulates
// a buffer overflow signal and verifies the client received the aborted
// status code.
func (suite *WatchServiceHandlerTestSuite) TestJobWatch_Overflow() {
	watchID := NewWatchID(ClientTypeJob)
	jobClient := &JobClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *stateless.JobSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewJobClient(gomock.Any()).
		Return(watchID, jobClient, nil)
	suite.processor.EXPECT().StopJobClient(watchID)

	jobs := []*stateless.JobSummary{
		{
			JobId: &peloton.JobID{Value: "job-0"},
		},
		{
			JobId: &peloton.JobID{Value: "job-1"},
		},
	}

	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)
	for _, j := range jobs {
		suite.watchServer.EXPECT().
			Send(&watchsvc.WatchResponse{
				WatchId:       watchID,
				StatelessJobs: []*stateless.JobSummary{j},
			}).
			Return(nil)
	}

	req := &watchsvc.WatchRequest{
		StatelessJobFilter: &watch.StatelessJobFilter{},
	}

	go func() {
		for _, j := range jobs {
			jobClient.Input <- j
		}
		// cancelling task watch
		jobClient.Signal <- StopSignalOverflow
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsAborted(err))
}

// TestJobWatch_MaxClientReached checks Watch will return resource-exhausted
// error when NewJobClient reached max client.
func (suite *WatchServiceHandlerTestSuite) TestJobWatch_MaxClientReached() {
	suite.processor.EXPECT().NewJobClient(gomock.Any()).
		Return("", nil, yarpcerrors.ResourceExhaustedErrorf("max client reached"))

	req := &watchsvc.WatchRequest{
		StatelessJobFilter: &watch.StatelessJobFilter{},
	}

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.True(yarpcerrors.IsResourceExhausted(err))
}

// TestJobWatch_InitSendError tests for error case of initial response.
func (suite *WatchServiceHandlerTestSuite) TestJobWatch_InitSendError() {
	watchID := NewWatchID(ClientTypeJob)
	jobClient := &JobClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *stateless.JobSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewJobClient(gomock.Any()).
		Return(watchID, jobClient, nil)
	suite.processor.EXPECT().StopJobClient(watchID)

	sendErr := errors.New("message:transport is closing")

	// initial response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
		}).
		Return(sendErr)

	req := &watchsvc.WatchRequest{
		StatelessJobFilter: &watch.StatelessJobFilter{},
	}

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.Equal(sendErr, err)
}

// TestJobWatch_InitSendError tests for error case of subsequent response
// after initial one.
func (suite *WatchServiceHandlerTestSuite) TestJobWatch_SendError() {
	watchID := NewWatchID(ClientTypeJob)
	jobClient := &JobClient{
		// do not set buffer size for input to make sure the
		// tests sends all the events before sending stop
		// signal
		Input:  make(chan *stateless.JobSummary),
		Signal: make(chan StopSignal, 1),
	}

	suite.processor.EXPECT().NewJobClient(gomock.Any()).
		Return(watchID, jobClient, nil)
	suite.processor.EXPECT().StopJobClient(watchID)

	j := &stateless.JobSummary{
		JobId: &peloton.JobID{Value: "job-0"},
	}

	// initial response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId: watchID,
			Pods:    nil,
		}).
		Return(nil)

	sendErr := errors.New("message:transport is closing")

	// subsequent response
	suite.watchServer.EXPECT().
		Send(&watchsvc.WatchResponse{
			WatchId:       watchID,
			StatelessJobs: []*stateless.JobSummary{j},
		}).
		Return(sendErr)

	req := &watchsvc.WatchRequest{
		StatelessJobFilter: &watch.StatelessJobFilter{},
	}

	go func() {
		jobClient.Input <- j
		jobClient.Signal <- StopSignalCancel
	}()

	err := suite.handler.Watch(req, suite.watchServer)
	suite.Error(err)
	suite.Equal(sendErr, err)
}

// TestCancel tests Cancel request are proxied to watch processor correctly.
func (suite *WatchServiceHandlerTestSuite) TestCancel() {
	watchID := NewWatchID(ClientTypeTask)

	suite.processor.EXPECT().StopTaskClient(watchID).Return(nil)

	resp, err := suite.handler.Cancel(suite.ctx, &watchsvc.CancelRequest{
		WatchId: watchID,
	})
	suite.NotNil(resp)
	suite.NoError(err)
}

// TestCancel_NotFoundTask tests Cancel response returns not-found error, when
// an invalid task watch-id is passed in.
func (suite *WatchServiceHandlerTestSuite) TestCancel_NotFoundTask() {
	watchID := NewWatchID(ClientTypeTask)

	err := yarpcerrors.NotFoundErrorf("watch_id %s not exist for task watch client", watchID)

	suite.processor.EXPECT().
		StopTaskClient(watchID).
		Return(err)

	resp, err := suite.handler.Cancel(suite.ctx, &watchsvc.CancelRequest{
		WatchId: watchID,
	})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

// TestCancel_InvalidWatchID tests Cancel response returns not-found error,
// when an invalid watch id (without proper prefix) is passed in.
func (suite *WatchServiceHandlerTestSuite) TestCancel_InvalidWatchID() {
	watchID := uuid.New()

	resp, err := suite.handler.Cancel(suite.ctx, &watchsvc.CancelRequest{
		WatchId: watchID,
	})
	suite.Nil(resp)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}

func TestWatchServiceHandler(t *testing.T) {
	suite.Run(t, &WatchServiceHandlerTestSuite{})
}
