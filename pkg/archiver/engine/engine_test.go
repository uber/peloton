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

package engine

import (
	"context"
	"fmt"
	nethttp "net/http"
	"net/url"
	"testing"
	"time"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	job_mocks "github.com/uber/peloton/.gen/peloton/api/v0/job/mocks"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	task_mocks "github.com/uber/peloton/.gen/peloton/api/v0/task/mocks"
	"github.com/uber/peloton/pkg/archiver/config"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/leader"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

var (
	streamJobStates = []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}
)

type archiverEngineTestSuite struct {
	suite.Suite

	mockCtrl       *gomock.Controller
	mockJobClient  *job_mocks.MockJobManagerYARPCClient
	mockTaskClient *task_mocks.MockTaskManagerYARPCClient
	retryPolicy    backoff.RetryPolicy
	e              *engine
}

func (suite *archiverEngineTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJobClient = job_mocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
	suite.mockTaskClient = task_mocks.NewMockTaskManagerYARPCClient(suite.mockCtrl)
	suite.retryPolicy = backoff.NewRetryPolicy(3, 100*time.Millisecond)
	suite.e = &engine{
		jobClient: suite.mockJobClient,
		metrics:   NewMetrics(tally.NoopScope),
	}
}

func (suite *archiverEngineTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func TestPelotonArchiver(t *testing.T) {
	suite.Run(t, new(archiverEngineTestSuite))
}

// TestEngineNew tests creating a new archiver engine
func (suite *archiverEngineTestSuite) TestEngineNew() {
	jobmgrURL, err := url.Parse("http://localhost:5292")
	suite.NoError(err)
	d, err := leader.NewStaticServiceDiscovery(
		jobmgrURL, &url.URL{}, &url.URL{})
	suite.NoError(err)
	_, err = New(config.Config{}, tally.NoopScope, nethttp.NewServeMux(),
		d, []transport.Inbound{})
	suite.NoError(err)
}

// TestDeletePodEvents tests deleting of pod events for a job + instance
func (suite *archiverEngineTestSuite) TestDeletePodEvents() {
	e := &engine{
		jobClient:  suite.mockJobClient,
		taskClient: suite.mockTaskClient,
		config: config.Config{
			Archiver: config.ArchiverConfig{
				Enable:           false,
				PodEventsCleanup: true,
				ArchiveInterval:  10 * time.Millisecond,
				BootstrapDelay:   10 * time.Millisecond,
			},
		},
		metrics:     NewMetrics(tally.NoopScope),
		dispatcher:  yarpc.NewDispatcher(yarpc.Config{Name: config.PelotonArchiver}),
		retryPolicy: suite.retryPolicy,
	}

	jobID := &peloton.JobID{Value: "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06"}
	taskID := "7ac74273-4ef0-4ca4-8fd2-34bc52aeac06-0-101"
	mesosTaskID := &mesos.TaskID{Value: &taskID}
	queryResp := &job.QueryResponse{
		Results: []*job.JobSummary{
			{
				Id:            jobID,
				InstanceCount: 1,
				Type:          job.JobType_SERVICE,
			},
		},
	}

	getPodEventRequest := &task.GetPodEventsRequest{
		JobId:      jobID,
		InstanceId: uint32(0),
		Limit:      uint64(1),
	}

	deletePodEventsRequest := &task.DeletePodEventsRequest{
		JobId:      jobID,
		InstanceId: uint32(0),
		RunId:      uint64(1),
	}

	var events []*task.PodEvent
	event := &task.PodEvent{
		TaskId: mesosTaskID,
	}
	events = append(events, event)

	getPodEventsResponse := &task.GetPodEventsResponse{
		Result: events,
	}

	gomock.InOrder(
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(queryResp, nil),
		suite.mockTaskClient.EXPECT().GetPodEvents(gomock.Any(), getPodEventRequest).
			Return(getPodEventsResponse, nil),
		suite.mockTaskClient.EXPECT().DeletePodEvents(gomock.Any(), deletePodEventsRequest).
			Return(nil, nil),

		// Job Query fails on all three retries. At this point we will send error to errChan
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
	)

	if err := e.Start(); err != nil {
		e.Cleanup()
	}
}

// TestEngineStartCleanup tests starting the archiver and cleaning up
// on receiving errors
func (suite *archiverEngineTestSuite) TestEngineStartCleanup() {
	e := &engine{
		jobClient: suite.mockJobClient,
		config: config.Config{
			Archiver: config.ArchiverConfig{
				Enable:          true,
				ArchiveInterval: 10 * time.Millisecond,
				BootstrapDelay:  10 * time.Millisecond,
			},
		},
		metrics:     NewMetrics(tally.NoopScope),
		dispatcher:  yarpc.NewDispatcher(yarpc.Config{Name: config.PelotonArchiver}),
		retryPolicy: suite.retryPolicy,
	}

	queryResp := &job.QueryResponse{
		Results: []*job.JobSummary{
			{
				Id: &peloton.JobID{Value: "my-job-0"},
			},
		},
	}

	gomock.InOrder(
		// query succeeds and returns 0 jobs. In this case, we don't call delete
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(&job.QueryResponse{}, nil),

		// query succeeds and returns 1 job
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(queryResp, nil),
		// delete succeeds
		suite.mockJobClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
			Return(&job.DeleteResponse{}, nil),

		// query succeeds and returns 1 job
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(queryResp, nil),
		// delete fails. In this case we will log the error and continue
		suite.mockJobClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Delete failed")),

		// Job Query fails on all three retries. At this point we will send error to errChan
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Query failed")),
	)

	if err := e.Start(); err != nil {
		e.Cleanup()
	}
}

// TestQueryJobs tests archiver calls to the Job Query API
func (suite *archiverEngineTestSuite) TestQueryJobs() {
	maxTime := time.Now().UTC()
	minTime := maxTime.Add(-10 * time.Second)

	max, _ := ptypes.TimestampProto(maxTime)
	min, _ := ptypes.TimestampProto(minTime)

	spec := job.QuerySpec{
		JobStates: []job.JobState{
			job.JobState_SUCCEEDED,
			job.JobState_FAILED,
			job.JobState_KILLED,
		},
		CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
		Pagination: &query.PaginationSpec{
			Offset:   0,
			Limit:    uint32(10),
			MaxLimit: uint32(10),
		},
	}

	req := &job.QueryRequest{
		Spec:        &spec,
		SummaryOnly: true,
	}

	expectedResp := &job.QueryResponse{}
	suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
		Return(expectedResp, nil)
	p := backoff.NewRetrier(suite.retryPolicy)

	resp, err := suite.e.queryJobs(context.Background(), req, p)
	suite.NoError(err)
	suite.Equal(expectedResp, resp)

	// Job Query fails on all three retries
	gomock.InOrder(
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(nil, fmt.Errorf("Job Query failed")),
	)
	p = backoff.NewRetrier(suite.retryPolicy)
	_, err = suite.e.queryJobs(context.Background(), req, p)
	suite.Error(err)

	// Job Query succeeds on the third and final attempt
	p = backoff.NewRetrier(suite.retryPolicy)
	gomock.InOrder(
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(nil, fmt.Errorf("Job Query failed")),
		suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
			Return(expectedResp, nil),
	)
	resp, err = suite.e.queryJobs(context.Background(), req, p)
	suite.NoError(err)
	suite.Equal(expectedResp, resp)
}

func (suite *archiverEngineTestSuite) testArchiveResultMap(
	m map[string]int, expectedSucceeded, expectedFailed int) {
	succeededCount, ok := m[archiverSuccessKey]
	suite.True(ok)
	failedCount, ok := m[archiverFailureKey]
	suite.True(ok)
	suite.Equal(expectedSucceeded, succeededCount)
	suite.Equal(expectedFailed, failedCount)
}

// TestArchiveJobs tests how archiver archives the jobs returned
// by Job Query.
func (suite *archiverEngineTestSuite) TestArchiveJobs() {
	summaryList := []*job.JobSummary{
		{
			Id: &peloton.JobID{Value: "my-job-0"},
		},
		{
			Id: &peloton.JobID{Value: "my-job-1"},
		},
	}

	gomock.InOrder(
		suite.mockJobClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
			Return(&job.DeleteResponse{}, nil),
		suite.mockJobClient.EXPECT().Delete(gomock.Any(), gomock.Any()).
			Return(nil, fmt.Errorf("Job Delete failed")),
	)

	suite.e.archiveJobs(
		context.Background(),
		summaryList)
}

// TestArchiveJobsService tests that service jobs are not archived
func (suite *archiverEngineTestSuite) TestArchiveJobsService() {
	summaryList := []*job.JobSummary{
		{
			Type: job.JobType_SERVICE,
			Id:   &peloton.JobID{Value: "my-job-0"},
		},
	}

	suite.e.archiveJobs(
		context.Background(),
		summaryList)
}

// TestArchiveJobsStreamOnly tests that for stream only mode, jobs are not
// deleted from the local DB.
func (suite *archiverEngineTestSuite) TestArchiveJobsStreamOnly() {
	suite.e.config.Archiver.StreamOnlyMode = true
	summaryList := []*job.JobSummary{
		{
			Type: job.JobType_BATCH,
			Id:   &peloton.JobID{Value: "my-job-0"},
		},
	}
	suite.e.archiveJobs(
		context.Background(),
		summaryList)
}
