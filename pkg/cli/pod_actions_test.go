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

package cli

import (
	"context"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	testPodName = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1"
	testPodID   = "941ff353-ba82-49fe-8f80-fb5bc649b04d-1-2"
)

type podActionsTestSuite struct {
	suite.Suite
	ctx    context.Context
	client Client

	ctrl      *gomock.Controller
	podClient *mocks.MockPodServiceYARPCClient
}

func (suite *podActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.podClient = mocks.NewMockPodServiceYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:      false,
		podClient:  suite.podClient,
		dispatcher: nil,
		ctx:        suite.ctx,
	}
}

func (suite *podActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestClientPodGetCacheSuccess test the success case of getting cache
func (suite *podActionsTestSuite) TestClientPodGetCacheSuccess() {
	suite.podClient.EXPECT().
		GetPodCache(gomock.Any(), gomock.Any()).
		Return(&podsvc.GetPodCacheResponse{
			Status: &pod.PodStatus{
				State: pod.PodState_POD_STATE_RUNNING,
			},
		}, nil)

	suite.NoError(suite.client.PodGetCacheAction(testPodName))
}

// TestClientPodGetCacheSuccess test the failure case of getting cache
func (suite *podActionsTestSuite) TestClientPodGetCacheFail() {
	suite.podClient.EXPECT().
		GetPodCache(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodGetCacheAction(testPodName))
}

// TestPodGetEventsV1AlphaAction tests PodGetEventsV1AlphaAction
func (suite *podActionsTestSuite) TestPodGetEventsV1AlphaAction() {
	podname := &peloton.PodName{
		Value: "podname",
	}
	podID := &peloton.PodID{
		Value: "podID",
	}
	req := &podsvc.GetPodEventsRequest{
		PodName: podname,
		PodId:   podID,
	}

	podEvent := &pod.PodEvent{
		PodId: &peloton.PodID{
			Value: "podID",
		},
		PrevPodId: &peloton.PodID{
			Value: "prevPodID",
		},
		ActualState:  "PENDING",
		DesiredState: "RUNNING",
	}

	var podEvents []*pod.PodEvent
	podEvents = append(podEvents, podEvent)
	response := &podsvc.GetPodEventsResponse{
		Events: podEvents,
	}
	suite.podClient.EXPECT().GetPodEvents(context.Background(), req).
		Return(response, nil)
	err := suite.client.PodGetEventsV1AlphaAction(podname.GetValue(), podID.GetValue())
	suite.NoError(err)

	// Client with debug set to true
	suite.client.Debug = true
	suite.podClient.EXPECT().GetPodEvents(context.Background(), req).
		Return(response, nil)
	err = suite.client.PodGetEventsV1AlphaAction(podname.GetValue(), podID.GetValue())
	suite.NoError(err)
}

// TestPodGetEventsV1AlphaActionClientFailure tests GetPodEvents API error
func (suite *podActionsTestSuite) TestPodGetEventsV1AlphaActionAPIError() {
	podname := &peloton.PodName{
		Value: "podname",
	}
	podID := &peloton.PodID{
		Value: "podID",
	}
	req := &podsvc.GetPodEventsRequest{
		PodName: podname,
		PodId:   podID,
	}

	suite.podClient.EXPECT().GetPodEvents(suite.ctx, req).
		Return(nil, yarpcerrors.InternalErrorf("test GetPodEvents error"))
	err := suite.client.PodGetEventsV1AlphaAction(podname.GetValue(), podID.GetValue())
	suite.Error(err)
}

// TestClientPodRefreshSuccess test the success case of refreshing pod
func (suite *podActionsTestSuite) TestClientPodRefreshSuccess() {
	suite.podClient.EXPECT().
		RefreshPod(gomock.Any(), gomock.Any()).
		Return(&podsvc.RefreshPodResponse{}, nil)

	suite.NoError(suite.client.PodRefreshAction(testPodName))
}

// TestClientPodRefreshFail test the failure case of refreshing pod
func (suite *podActionsTestSuite) TestClientPodRefreshFail() {
	suite.podClient.EXPECT().
		RefreshPod(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodRefreshAction(testPodName))
}

// TestClientPodStartSuccess test the success case of starting pod
func (suite *podActionsTestSuite) TestClientPodStartSuccess() {
	suite.podClient.EXPECT().
		StartPod(gomock.Any(), gomock.Any()).
		Return(&podsvc.StartPodResponse{}, nil)

	suite.NoError(suite.client.PodStartAction(testPodName))
}

// TestClientPodStartFail test the failure case starting pod
func (suite *podActionsTestSuite) TestClientPodStartFail() {
	suite.podClient.EXPECT().
		StartPod(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodStartAction(testPodName))
}

// TestPodLogsGetActionSuccess tests failure of getting pod logs
// due to file not found error
func (suite *podActionsTestSuite) TestPodLogsGetActionFileNotFound() {
	podname := &peloton.PodName{
		Value: "podname",
	}
	podID := &peloton.PodID{
		Value: "podID",
	}
	req := &podsvc.BrowsePodSandboxRequest{
		PodName: podname,
		PodId:   podID,
	}
	resp := &podsvc.BrowsePodSandboxResponse{
		Hostname: "host1",
		Port:     "8000",
	}

	suite.podClient.EXPECT().
		BrowsePodSandbox(suite.ctx, req).
		Return(resp, nil)
	suite.Error(
		suite.client.PodLogsGetAction(
			"",
			podname.GetValue(),
			podID.GetValue(),
		),
	)
}

// TestPodLogsGetActionSuccess tests failure of getting pod logs
// due to BrowsePodSandbox API error
func (suite *podActionsTestSuite) TestPodLogsGetActionBrowsePodSandboxFailure() {
	suite.podClient.EXPECT().
		BrowsePodSandbox(suite.ctx, gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))
	suite.Error(
		suite.client.PodLogsGetAction(
			"",
			"",
			"",
		),
	)
}

// TestPodLogsGetActionSuccess tests failure of getting pod logs
// due to error while downloading file
func (suite *podActionsTestSuite) TestPodLogsGetActionFileGetFailure() {
	podname := &peloton.PodName{
		Value: "podname",
	}
	podID := &peloton.PodID{
		Value: "podID",
	}
	filename := "filename"
	req := &podsvc.BrowsePodSandboxRequest{
		PodName: podname,
		PodId:   podID,
	}
	resp := &podsvc.BrowsePodSandboxResponse{
		Hostname: "host1",
		Port:     "8000",
		Paths:    []string{filename},
	}

	suite.podClient.EXPECT().
		BrowsePodSandbox(suite.ctx, req).
		Return(resp, nil)
	suite.Error(
		suite.client.PodLogsGetAction(
			filename,
			podname.GetValue(),
			podID.GetValue(),
		),
	)
}

// TestClientPodRestartSuccess tests the success case of restarting pod
func (suite *podActionsTestSuite) TestClientPodRestartSuccess() {
	suite.podClient.EXPECT().
		RestartPod(gomock.Any(), gomock.Any()).
		Return(&podsvc.RestartPodResponse{}, nil)

	suite.NoError(suite.client.PodRestartAction(testPodName))
}

// TestClientPodRestartFail tests the failure case restarting pod
func (suite *podActionsTestSuite) TestClientPodRestartFail() {
	suite.podClient.EXPECT().
		RestartPod(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodRestartAction(testPodName))
}

// TestClientPodStopSuccess tests the success case of stopping pod
func (suite *podActionsTestSuite) TestClientPodStopSuccess() {
	suite.podClient.EXPECT().
		StopPod(gomock.Any(), gomock.Any()).
		Return(&podsvc.StopPodResponse{}, nil)

	suite.NoError(suite.client.PodStopAction(testPodName))
}

// TestClientPodStopSuccess tests the failure case stopping pod
func (suite *podActionsTestSuite) TestClientPodStopFail() {
	suite.podClient.EXPECT().
		StopPod(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodStopAction(testPodName))
}

// TestClientPodGetSuccess tests the success case of getting pod info
func (suite *podActionsTestSuite) TestClientPodGetSuccess() {
	suite.podClient.EXPECT().
		GetPod(gomock.Any(), gomock.Any()).
		Return(&podsvc.GetPodResponse{}, nil)

	suite.NoError(suite.client.PodGetAction(testPodName, false, uint32(2)))
}

// TestClientPodGetFailure tests the failure case of getting pod info
func (suite *podActionsTestSuite) TestClientPodGetFailure() {
	suite.podClient.EXPECT().
		GetPod(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodGetAction(testPodName, false, uint32(2)))
}

// TestClientPodDeleteEventsSuccess tests the success case of deleting pod events
func (suite *podActionsTestSuite) TestClientPodDeleteEventsSuccess() {
	suite.podClient.EXPECT().
		DeletePodEvents(gomock.Any(), gomock.Any()).
		Return(&podsvc.DeletePodEventsResponse{}, nil)

	suite.NoError(suite.client.PodDeleteEvents(testPodName, testPodID))
}

// TestClientPodDeleteEventsFailure tests the failure case of deleting pod events
func (suite *podActionsTestSuite) TestClientPodDeleteEventsFailure() {
	suite.podClient.EXPECT().
		DeletePodEvents(gomock.Any(), gomock.Any()).
		Return(nil, yarpcerrors.InternalErrorf("test error"))

	suite.Error(suite.client.PodDeleteEvents(testPodName, testPodID))
}

func TestPodActions(t *testing.T) {
	suite.Run(t, new(podActionsTestSuite))
}
