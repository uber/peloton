package engine

import (
	"context"
	"fmt"
	"math"
	nethttp "net/http"
	"net/url"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	job_mocks "code.uber.internal/infra/peloton/.gen/peloton/api/v0/job/mocks"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/archiver/config"
	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/leader"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
)

type archiverEngineTestSuite struct {
	suite.Suite

	mockCtrl      *gomock.Controller
	mockJobClient *job_mocks.MockJobManagerYARPCClient
	retryPolicy   backoff.RetryPolicy
	e             *engine
}

func (suite *archiverEngineTestSuite) SetupSuite() {
	suite.mockCtrl = gomock.NewController(suite.T())
	suite.mockJobClient = job_mocks.NewMockJobManagerYARPCClient(suite.mockCtrl)
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
	maxTime := time.Now().UTC()
	minTime := maxTime.Add(-10 * time.Second)
	max, _ := ptypes.TimestampProto(maxTime)
	min, _ := ptypes.TimestampProto(minTime)
	expectedjobStates := []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}

	req, err := suite.e.buildQueryRequest(minTime, maxTime)
	suite.NoError(err)
	suite.Equal(true, req.SummaryOnly)
	suite.Equal(expectedjobStates, req.Spec.JobStates)
	suite.Equal(max, req.Spec.CompletionTimeRange.Max)
	suite.Equal(min, req.Spec.CompletionTimeRange.Min)

	// Simulate invalid timestamp proto errors
	req, err = suite.e.buildQueryRequest(
		minTime, time.Unix(math.MinInt64, 0).UTC())
	suite.Error(err)
	req, err = suite.e.buildQueryRequest(
		time.Unix(math.MinInt64, 0).UTC(), maxTime)
	suite.Error(err)

}

// TestQueryJobs tests archiver calls to the Job Query API
func (suite *archiverEngineTestSuite) TestQueryJobs() {
	maxTime := time.Now().UTC()
	minTime := maxTime.Add(-10 * time.Second)
	req, err := suite.e.buildQueryRequest(minTime, maxTime)
	suite.NoError(err)

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

	m := suite.e.archiveJobs(context.Background(), summaryList)
	suite.testArchiveResultMap(m, 1, 1)
}

// TestArchiveJobsService tests that service jobs are not archived
func (suite *archiverEngineTestSuite) TestArchiveJobsService() {
	summaryList := []*job.JobSummary{
		{
			Type: job.JobType_SERVICE,
			Id:   &peloton.JobID{Value: "my-job-0"},
		},
	}

	m := suite.e.archiveJobs(context.Background(), summaryList)
	suite.testArchiveResultMap(m, 0, 0)
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
	m := suite.e.archiveJobs(context.Background(), summaryList)
	suite.testArchiveResultMap(m, 0, 0)
}
