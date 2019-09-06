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
	"io"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type watchActionsTestSuite struct {
	suite.Suite
	ctx    context.Context
	client Client

	ctrl        *gomock.Controller
	watchClient *mocks.MockWatchServiceYARPCClient
}

func (suite *watchActionsTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.watchClient = mocks.NewMockWatchServiceYARPCClient(suite.ctrl)
	suite.ctx = context.Background()
	suite.client = Client{
		Debug:       false,
		watchClient: suite.watchClient,
		dispatcher:  nil,
		ctx:         suite.ctx,
	}
}

func (suite *watchActionsTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func (suite *watchActionsTestSuite) TestWatchPod() {
	var labels []string

	jobID := "test-job-id"
	podNames := []string{"test-pod-1", "test-pod-2"}
	watchID := uuid.New()
	label1 := "key1:value1"
	labels = append(labels, label1)

	stream := mocks.NewMockWatchServiceServiceWatchYARPCClient(suite.ctrl)
	resps := []*watchsvc.WatchResponse{
		{WatchId: watchID},
	}

	for _, podName := range podNames {
		resps = append(resps, &watchsvc.WatchResponse{
			WatchId: watchID,
			Pods: []*pod.PodSummary{
				{
					PodName: &peloton.PodName{Value: podName},
				},
			},
		})
	}

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	var calls []*gomock.Call
	for _, resp := range resps {
		calls = append(calls, stream.EXPECT().Recv().Return(resp, nil))
	}
	calls = append(calls, stream.EXPECT().Recv().Return(nil, io.EOF))

	gomock.InOrder(calls...)

	suite.NoError(suite.client.WatchPod(jobID, podNames, labels))
}

func (suite *watchActionsTestSuite) TestWatchPodLabelError() {
	var labels []string

	jobID := "test-job-id"
	podNames := []string{"test-pod-1", "test-pod-2"}
	label1 := "key1:value1:value2"
	labels = append(labels, label1)

	suite.Error(suite.client.WatchPod(jobID, podNames, labels))
}

func (suite *watchActionsTestSuite) TestWatchJob() {
	var labels []string

	jobIDs := []string{"job-id-0", "job-id-1"}
	watchID := uuid.New()
	label1 := "key1:value1"
	labels = append(labels, label1)

	stream := mocks.NewMockWatchServiceServiceWatchYARPCClient(suite.ctrl)
	resps := []*watchsvc.WatchResponse{
		{WatchId: watchID},
	}

	for _, jobID := range jobIDs {
		resps = append(resps, &watchsvc.WatchResponse{
			WatchId: watchID,
			StatelessJobs: []*stateless.JobSummary{
				{
					JobId: &peloton.JobID{Value: jobID},
				},
			},
		})
	}

	suite.watchClient.EXPECT().
		Watch(gomock.Any(), gomock.Any()).
		Return(stream, nil)

	var calls []*gomock.Call
	for _, resp := range resps {
		calls = append(calls, stream.EXPECT().Recv().Return(resp, nil))
	}
	calls = append(calls, stream.EXPECT().Recv().Return(nil, io.EOF))

	gomock.InOrder(calls...)

	suite.NoError(suite.client.WatchJob(jobIDs, labels))
}

func (suite *watchActionsTestSuite) TestWatchJobLabelError() {
	var labels []string

	jobIDs := []string{"job-id-0", "job-id-1"}
	label1 := "key1:value1:value2"
	labels = append(labels, label1)

	suite.Error(suite.client.WatchJob(jobIDs, labels))
}

func (suite *watchActionsTestSuite) TestCancelWatch() {
	watchID := uuid.New()

	suite.watchClient.EXPECT().
		Cancel(gomock.Any(), gomock.Any()).
		Return(&watchsvc.CancelResponse{}, nil)

	suite.NoError(suite.client.CancelWatch(watchID))
}

func TestWatchActions(t *testing.T) {
	suite.Run(t, new(watchActionsTestSuite))
}
