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

package progress

import (
	"testing"
	"time"

	"github.com/uber/peloton/pkg/common"
	backgroundmocks "github.com/uber/peloton/pkg/common/background/mocks"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	cachemock "github.com/uber/peloton/pkg/jobmgr/cached/mocks"

	pbjob "github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ProgressCheckerTestSuite struct {
	suite.Suite
	mockCtrl *gomock.Controller

	testScope  tally.TestScope
	jobFactory *cachemock.MockJobFactory
	checker    *WorkflowProgressCheck
}

// TestCheckProgressSuccess tests progress check works as intended
func (s *ProgressCheckerTestSuite) TestCheckProgressSuccess() {
	job1 := cachemock.NewMockJob(s.mockCtrl)
	job2 := cachemock.NewMockJob(s.mockCtrl)
	job3 := cachemock.NewMockJob(s.mockCtrl)
	task0 := cachemock.NewMockTask(s.mockCtrl)
	task1 := cachemock.NewMockTask(s.mockCtrl)

	s.jobFactory.EXPECT().
		GetAllJobs().
		Return(map[string]cached.Job{
			"job1": job1,
			"job2": job2,
			"job3": job3,
		})

	job1.EXPECT().GetJobType().Return(pbjob.JobType_SERVICE)
	job2.EXPECT().GetJobType().Return(pbjob.JobType_SERVICE)
	job3.EXPECT().GetJobType().Return(pbjob.JobType_SERVICE)

	job1.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			UpdateID: &peloton.UpdateID{Value: "update1"},
		}, nil)
	job2.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			UpdateID: &peloton.UpdateID{Value: "update2"},
		}, nil)
	job3.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			UpdateID: &peloton.UpdateID{Value: "update3"},
		}, nil)

	// active non-stale update
	workflow1 := cachemock.NewMockUpdate(s.mockCtrl)
	job1.EXPECT().
		GetWorkflow(gomock.Any()).
		Return(workflow1)
	workflow1.EXPECT().GetState().Return(&cached.UpdateStateVector{State: update.State_ROLLING_FORWARD})
	workflow1.EXPECT().GetLastUpdateTime().Return(time.Now())

	// active stale update
	workflow2 := cachemock.NewMockUpdate(s.mockCtrl)
	job2.EXPECT().
		GetWorkflow(gomock.Any()).
		Return(workflow2)
	workflow2.EXPECT().GetState().Return(&cached.UpdateStateVector{State: update.State_ROLLING_FORWARD})
	workflow2.EXPECT().GetLastUpdateTime().Return(time.Now().Add(-100 * time.Hour))
	workflow2.EXPECT().GetWorkflowType().Return(models.WorkflowType_UPDATE)
	workflow2.EXPECT().GetInstancesCurrent().Return([]uint32{0})
	job2.EXPECT().ID().Return(&peloton.JobID{Value: "job2"})
	job2.EXPECT().GetTask(uint32(0)).Return(task0)
	task0.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING}, nil)

	// active stale update due to task throttling
	workflow3 := cachemock.NewMockUpdate(s.mockCtrl)
	job3.EXPECT().
		GetWorkflow(gomock.Any()).
		Return(workflow3)
	workflow3.EXPECT().GetState().Return(&cached.UpdateStateVector{State: update.State_ROLLING_FORWARD})
	workflow3.EXPECT().GetLastUpdateTime().Return(time.Now().Add(-100 * time.Hour))
	workflow3.EXPECT().GetInstancesCurrent().Return([]uint32{0})
	job3.EXPECT().GetTask(uint32(0)).Return(task1)
	task1.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{State: pbtask.TaskState_FAILED, Message: common.TaskThrottleMessage}, nil)

	s.checker.Check()

	gauges := s.testScope.Snapshot().Gauges()
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=all"].Value()), 3)
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=active"].Value()), 3)
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=stale"].Value()), 1)
}

// mockCtrl tests progress check would skip job that fail to get runtime
// and continue to check progress on the remaining workflows
func (s *ProgressCheckerTestSuite) TestGetJobRuntimeFailure() {
	job1 := cachemock.NewMockJob(s.mockCtrl)
	job2 := cachemock.NewMockJob(s.mockCtrl)
	task0 := cachemock.NewMockTask(s.mockCtrl)

	s.jobFactory.EXPECT().
		GetAllJobs().
		Return(map[string]cached.Job{
			"job1": job1,
			"job2": job2,
		})

	job1.EXPECT().GetJobType().Return(pbjob.JobType_SERVICE)
	job2.EXPECT().GetJobType().Return(pbjob.JobType_SERVICE)
	job1.EXPECT().ID().Return(&peloton.JobID{Value: "job1"})

	job1.EXPECT().
		GetRuntime(gomock.Any()).
		Return(nil, errors.New("test error"))
	job2.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbjob.RuntimeInfo{
			UpdateID: &peloton.UpdateID{Value: "update2"},
		}, nil)

	// active stale update
	workflow2 := cachemock.NewMockUpdate(s.mockCtrl)
	job2.EXPECT().
		GetWorkflow(gomock.Any()).
		Return(workflow2)
	workflow2.EXPECT().GetState().Return(&cached.UpdateStateVector{State: update.State_ROLLING_FORWARD})
	workflow2.EXPECT().GetLastUpdateTime().Return(time.Now().Add(-100 * time.Hour))
	workflow2.EXPECT().GetWorkflowType().Return(models.WorkflowType_UPDATE)
	workflow2.EXPECT().GetInstancesCurrent().Return([]uint32{0})
	job2.EXPECT().GetTask(uint32(0)).Return(task0)
	task0.EXPECT().
		GetRuntime(gomock.Any()).
		Return(&pbtask.RuntimeInfo{State: pbtask.TaskState_RUNNING}, nil)
	job2.EXPECT().ID().Return(&peloton.JobID{Value: "job2"})

	s.checker.Check()

	gauges := s.testScope.Snapshot().Gauges()
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=all"].Value()), 1)
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=active"].Value()), 1)
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=stale"].Value()), 1)
	s.Equal(int(gauges["workflow_progress.workflow.total_workflow+workflow_type=all"].Value()), 1)
	s.Equal(int(gauges["workflow_progress.job_runtime_failure+"].Value()), 1)
}

// TestCheckRegister tests WorkflowProgressCheck registers with
// background manager
func (s *ProgressCheckerTestSuite) TestCheckRegister() {
	mockBackgroundManager := backgroundmocks.NewMockManager(s.mockCtrl)
	mockBackgroundManager.EXPECT().RegisterWorks(gomock.Any()).Return(nil)
	s.NoError(s.checker.Register(mockBackgroundManager))
}

func (s *ProgressCheckerTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())

	s.testScope = tally.NewTestScope("", nil)
	s.jobFactory = cachemock.NewMockJobFactory(s.mockCtrl)

	config := &Config{}
	config.normalize()

	s.checker = &WorkflowProgressCheck{
		JobFactory: s.jobFactory,
		Metrics:    NewMetrics(s.testScope),
		Config:     config,
	}
}

func (s *ProgressCheckerTestSuite) TearDownTest() {
	s.mockCtrl.Finish()
}

func TestProgressCheckerTestSuite(t *testing.T) {
	suite.Run(t, new(ProgressCheckerTestSuite))
}
