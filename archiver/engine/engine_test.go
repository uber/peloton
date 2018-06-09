package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	job_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/archiver/config"
	"code.uber.internal/infra/peloton/common/backoff"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
)

type archiverEngineTestSuite struct {
	suite.Suite

	mockCtrl      *gomock.Controller
	mockJobClient *job_mocks.MockJobManagerYARPCClient
	retryPolicy   backoff.RetryPolicy
}

func (suite *archiverEngineTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJobClient = job_mocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
	suite.retryPolicy = backoff.NewRetryPolicy(3, 100*time.Millisecond)
}

func (suite *archiverEngineTestSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func TestPelotonArchiver(t *testing.T) {
	suite.Run(t, new(archiverEngineTestSuite))
}

// TestEngineStartCleanup tests starting the archiver and cleaning up
// on receiving errors
func (suite *archiverEngineTestSuite) TestEngineStartCleanup() {
	var err error

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
	errChan := make(chan error, 1)
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

	e.Start(errChan)
	select {
	case err = <-errChan:
		e.Cleanup()
		close(errChan)
		break
	}
	suite.Error(err)

}

// TestBuildQueryRequest tests the way job query request is constructed
func (suite *archiverEngineTestSuite) TestBuildQueryRequest() {
	e := &engine{}
	maxTime := time.Now().UTC()
	minTime := maxTime.Add(-10 * time.Second)
	max, _ := ptypes.TimestampProto(maxTime)
	min, _ := ptypes.TimestampProto(minTime)
	expectedjobStates := []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}

	req, err := e.buildQueryRequest(minTime, maxTime)
	suite.NoError(err)
	suite.Equal(true, req.SummaryOnly)
	suite.Equal(expectedjobStates, req.Spec.JobStates)
	suite.Equal(max, req.Spec.CompletionTimeRange.Max)
	suite.Equal(min, req.Spec.CompletionTimeRange.Min)
}

// TestQueryJobs tests archiver calls to the Job Query API
func (suite *archiverEngineTestSuite) TestQueryJobs() {
	e := &engine{
		jobClient: suite.mockJobClient,
		metrics:   NewMetrics(tally.NoopScope),
	}
	maxTime := time.Now().UTC()
	minTime := maxTime.Add(-10 * time.Second)
	req, err := e.buildQueryRequest(minTime, maxTime)
	suite.NoError(err)

	expectedResp := &job.QueryResponse{}
	suite.mockJobClient.EXPECT().Query(gomock.Any(), req).
		Return(expectedResp, nil)
	p := backoff.NewRetrier(suite.retryPolicy)

	resp, err := e.queryJobs(context.Background(), req, p)
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
	_, err = e.queryJobs(context.Background(), req, p)
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
	resp, err = e.queryJobs(context.Background(), req, p)
	suite.NoError(err)
	suite.Equal(expectedResp, resp)
}

// TestArchiveJobs tests how archiver archives the jobs returned
// by Job Query.
func (suite *archiverEngineTestSuite) TestArchiveJobs() {
	e := &engine{
		jobClient: suite.mockJobClient,
		metrics:   NewMetrics(tally.NoopScope),
	}

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

	m := e.archiveJobs(context.Background(), summaryList)
	succeededCount, _ := m["SUCCEEDED"]
	failedCount, _ := m["FAILED"]
	suite.Equal(1, succeededCount)
	suite.Equal(1, failedCount)
}
