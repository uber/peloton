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

	watchmocks "github.com/uber/peloton/pkg/jobmgr/watchsvc/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	. "github.com/uber/peloton/pkg/jobmgr/watchsvc"
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

// TestWatchListenerName checks WatchProcessor.NotifyTaskChange() is called
// when TaskRuntimeChanged is called on listener
func (suite *WatchListenerTestSuite) TestTaskRuntimeChanged() {
	suite.processor.EXPECT().
		NotifyTaskChange(gomock.Any(), gomock.Any()).
		Times(1)

	suite.listener.TaskRuntimeChanged(
		&v0peloton.JobID{Value: "test-job-1"},
		0,
		job.JobType_SERVICE,
		&task.RuntimeInfo{},
		[]*v0peloton.Label{},
	)
}

// TestTaskRuntimeChanged_NonServiceType checks
// WatchProcessor.NotifyTaskChange() is not called when not service type
// event is passed in.
func (suite *WatchListenerTestSuite) TestTaskRuntimeChanged_NonServiceType() {
	// do not expect call to processor.NotifyTaskChange

	suite.listener.TaskRuntimeChanged(
		&v0peloton.JobID{Value: "test-job-1"},
		0,
		job.JobType_BATCH,
		&task.RuntimeInfo{},
		[]*v0peloton.Label{},
	)
}

// TestTaskRuntimeChanged_NilFields checks WatchProcessor.NotifyTaskChange()
// is not called when not some of the fields are passed in as nil.
func (suite *WatchListenerTestSuite) TestTaskRuntimeChanged_NilFields() {
	// do not expect calls to processor.NotifyTaskChange

	suite.listener.TaskRuntimeChanged(
		nil,
		0,
		job.JobType_SERVICE,
		&task.RuntimeInfo{},
		[]*v0peloton.Label{},
	)

	suite.listener.TaskRuntimeChanged(
		&v0peloton.JobID{Value: "test-job-1"},
		0,
		job.JobType_SERVICE,
		nil,
		nil,
	)
}

func TestWatchListener(t *testing.T) {
	suite.Run(t, &WatchListenerTestSuite{})
}
