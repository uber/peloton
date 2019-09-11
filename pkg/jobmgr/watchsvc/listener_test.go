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
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/util"
	. "github.com/uber/peloton/pkg/jobmgr/watchsvc"
	watchmocks "github.com/uber/peloton/pkg/jobmgr/watchsvc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type WatchListenerTestSuite struct {
	suite.Suite

	listener WatchListener

	ctrl      *gomock.Controller
	processor *watchmocks.MockWatchProcessor
}

func (suite *WatchListenerTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.processor = watchmocks.NewMockWatchProcessor(suite.ctrl)

	suite.listener = NewWatchListener(suite.processor)
}

// TestWatchListenerName checks listener name
func (suite *WatchListenerTestSuite) TestWatchListenerName() {
	suite.NotEmpty(suite.listener.Name())
}

// TestPodSummaryChanged checks WatchProcessor.NotifyTaskChange() is called
// when TestPodSummaryChanged is called on listener
func (suite *WatchListenerTestSuite) TestPodSummaryChanged() {
	suite.processor.EXPECT().
		NotifyPodChange(gomock.Any(), gomock.Any()).
		Times(1)

	summary := &pod.PodSummary{
		PodName: &v1peloton.PodName{
			Value: util.CreatePelotonTaskID("test-job-1", 0),
		},
		Status: api.ConvertTaskRuntimeToPodStatus(&task.RuntimeInfo{}),
	}

	suite.listener.PodSummaryChanged(
		job.JobType_SERVICE,
		summary,
		[]*v1peloton.Label{},
	)
}

// TestPodSummaryChangedNonServiceType checks
// WatchProcessor.NotifyTaskChange() is not called when not service type
// event is passed in.
func (suite *WatchListenerTestSuite) TestPodSummaryChangedNonServiceType() {
	// do not expect call to processor.NotifyPodChange

	summary := &pod.PodSummary{
		PodName: &v1peloton.PodName{
			Value: util.CreatePelotonTaskID("test-job-1", 0),
		},
		Status: api.ConvertTaskRuntimeToPodStatus(&task.RuntimeInfo{}),
	}

	suite.listener.PodSummaryChanged(
		job.JobType_BATCH,
		summary,
		[]*v1peloton.Label{},
	)
}

// TestPodSummaryChangedNilFields checks WatchProcessor.NotifyTaskChange()
// is not called when not some of the fields are passed in as nil.
func (suite *WatchListenerTestSuite) TestPodSummaryChangedNilFields() {
	// do not expect calls to processor.NotifyPodChange

	summary := &pod.PodSummary{
		Status: api.ConvertTaskRuntimeToPodStatus(&task.RuntimeInfo{}),
	}

	suite.listener.PodSummaryChanged(
		job.JobType_SERVICE,
		summary,
		[]*v1peloton.Label{},
	)

	suite.listener.PodSummaryChanged(
		job.JobType_SERVICE,
		nil,
		[]*v1peloton.Label{},
	)
}

// TestStatelessJobSummaryChanged checks WatchProcessor.NotifyJobChange() is called
// when StatelessJobSummaryChanged is called on listener
func (suite *WatchListenerTestSuite) TestStatelessJobSummaryChanged() {
	suite.processor.EXPECT().
		NotifyJobChange(gomock.Any()).
		Times(1)

	suite.listener.StatelessJobSummaryChanged(
		&stateless.JobSummary{
			JobId: &v1peloton.JobID{
				Value: "test-job-1",
			},
			Status: &stateless.JobStatus{},
		},
	)
}

// TestJobSummaryChangedNonServiceType checks
// WatchProcessor.NotifyJobChange() is not called when not service type
// event is passed in.
func (suite *WatchListenerTestSuite) TestJobSummaryChangedNonServiceType() {
	// do not expect call to processor.NotifyTaskChange

	suite.listener.BatchJobSummaryChanged(
		&v0peloton.JobID{Value: "test-job-1"},
		&job.JobSummary{
			Runtime: &job.RuntimeInfo{},
		},
	)
}

// TestJobSummaryChangedNilFields checks WatchProcessor.NotifyJobChange()
// is not called when not some of the fields are passed in as nil.
func (suite *WatchListenerTestSuite) TestJobSummaryChangedNilFields() {
	// do not expect calls to processor.NotifyTaskChange

	suite.listener.StatelessJobSummaryChanged(
		&stateless.JobSummary{Status: &stateless.JobStatus{}},
	)

	suite.listener.StatelessJobSummaryChanged(nil)
}

func TestWatchListener(t *testing.T) {
	suite.Run(t, &WatchListenerTestSuite{})
}
