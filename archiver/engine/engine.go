package engine

import (
	"context"
	"fmt"
	"math/rand"
	nethttp "net/http"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/query"
	"code.uber.internal/infra/peloton/archiver/config"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/leader"

	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
)

const (

	// Delay between consecutive delete API requests, value chosen at random.
	delayDelete = 100 * time.Millisecond

	// Good practice to add some jitter to archive intervals,
	// in case we add more archiver instances in future.
	// Keep default max jitter to 100ms
	jitterMax = 100

	// The string "completed_job" will be used to tag the logs that contain
	// job summary. This will be used by logstash and streamed using a heatpipe
	// kafka topic to Hive table
	completedJobTag = "completed_job"

	// archiver summary map keys
	archiverSuccessKey = "SUCCESS"
	archiverFailureKey = "FAILURE"
)

// Engine defines the interface used to query a peloton component
// for data and then archive that data to secondary storage using
// message queue
type Engine interface {
	// Start starts the archiver goroutines
	Start(chan<- error)
	// Cleanup cleans the archiver engine before
	// restart
	Cleanup()
}

type engine struct {
	// Jobmgr client to make Job Query/Delete API requests
	jobClient job.JobManagerYARPCClient
	// Yarpc dispatcher
	dispatcher *yarpc.Dispatcher
	// Archiver config
	config config.Config
	// Archiver metrics
	metrics *Metrics
	// Archiver backoff/retry policy
	retryPolicy backoff.RetryPolicy
}

// New creates a new Archiver Engine.
func New(
	cfg config.Config,
	scope tally.Scope,
	mux *nethttp.ServeMux,
	discovery leader.Discovery,
	inbounds []transport.Inbound,
) (Engine, error) {
	cfg.Archiver.Normalize()

	jobmgrURL, err := discovery.GetAppURL(common.JobManagerRole)
	if err != nil {
		return nil, err
	}

	t := http.NewTransport()

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     config.PelotonArchiver,
		Inbounds: inbounds,
		Outbounds: yarpc.Outbounds{
			common.PelotonJobManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(jobmgrURL.String()),
			},
		},
		Metrics: yarpc.MetricsConfig{
			Tally: scope,
		},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	return &engine{
		jobClient: job.NewJobManagerYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		dispatcher: dispatcher,
		config:     cfg,
		metrics:    NewMetrics(scope),
		retryPolicy: backoff.NewRetryPolicy(
			cfg.Archiver.MaxRetryAttemptsJobQuery,
			cfg.Archiver.RetryIntervalJobQuery),
	}, nil
}

// Start starts archiver thread
func (e *engine) Start(errChan chan<- error) {
	go e.runArchiver(errChan)
	e.metrics.ArchiverStart.Inc(1)
}

// Cleanup cleans the archiver engine before restarting
func (e *engine) Cleanup() {
	e.dispatcher.Stop()
	return
}

func (e *engine) buildQueryRequest(minTime time.Time,
	maxTime time.Time) (*job.QueryRequest, error) {
	jobStates := []job.JobState{
		job.JobState_SUCCEEDED,
		job.JobState_FAILED,
		job.JobState_KILLED,
	}
	max, err := ptypes.TimestampProto(maxTime)
	if err != nil {
		return nil, err
	}
	min, err := ptypes.TimestampProto(minTime)
	if err != nil {
		return nil, err
	}
	spec := &job.QuerySpec{
		JobStates:           jobStates,
		CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
		Pagination: &query.PaginationSpec{
			Offset: 0,
			// Query for max of MaxArchiveEntries entries
			Limit:    uint32(e.config.Archiver.MaxArchiveEntries),
			MaxLimit: uint32(e.config.Archiver.MaxArchiveEntries),
		},
	}
	return &job.QueryRequest{
		Spec:        spec,
		SummaryOnly: true,
	}, nil
}

func (e *engine) queryJobs(ctx context.Context,
	req *job.QueryRequest,
	p backoff.Retrier) (*job.QueryResponse, error) {
	for {
		ctx, cancel := context.WithTimeout(ctx,
			e.config.Archiver.PelotonClientTimeout)
		resp, err := e.jobClient.Query(ctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		if backoff.CheckRetry(p) {
			log.WithError(err).
				Error("job query failed, retry after backoff")
			continue
		} else {
			return nil, err
		}
	}
}

func (e *engine) archiveJobs(ctx context.Context,
	results []*job.JobSummary) map[string]int {
	archiveSummary := map[string]int{archiverFailureKey: 0, archiverSuccessKey: 0}
	for _, summary := range results {
		// Archive only BATCH jobs
		if summary.GetType() != job.JobType_BATCH {
			continue
		}
		// Sleep between consecutive Job Delete requests
		time.Sleep(delayDelete)

		// The log event for completedJobTag will be logged to Archiver stdout
		// Filebeat configured on the Peloton host will ship this log out to
		// logstash. Logstash will be configured to stream this specific log
		// event to Hive via a heatpipe topic.
		log.WithField(completedJobTag, summary).Info("completed job")

		if e.config.Archiver.StreamOnlyMode {
			continue
		}

		log.WithFields(
			log.Fields{
				"job_id": summary.GetId().GetValue(),
				"state":  summary.GetRuntime().GetState(),
			}).
			Info("Deleting job")
		deleteReq := &job.DeleteRequest{
			Id: summary.GetId(),
		}
		ctx, cancel := context.WithTimeout(ctx,
			e.config.Archiver.PelotonClientTimeout)
		defer cancel()
		_, err := e.jobClient.Delete(ctx, deleteReq)
		if err != nil {
			// TODO: have a reasonable threshold for tolerating such failures
			// For now, just continue processing the next job in the list
			log.WithError(err).WithField("job_id",
				summary.GetId().GetValue()).
				Error("job delete failed")
			e.metrics.ArchiverJobDeleteFail.Inc(1)
			archiveSummary[archiverFailureKey]++
		} else {
			e.metrics.ArchiverJobDeleteSuccess.Inc(1)
			archiveSummary[archiverSuccessKey]++
		}
	}
	return archiveSummary
}

// runArchiver queries jobs in a time range and archives them periodically
func (e *engine) runArchiver(errChan chan<- error) {
	// Account for time taken for jobmgr to start and finish recovery
	// This is more of a precaution so that archiver does not mess
	// around with core jobmgr functionality.
	// TODO (adityacb): remove this delay once we move to API server
	jitter := time.Duration(rand.Intn(jitterMax)) * time.Millisecond
	time.Sleep(e.config.Archiver.BootstrapDelay + jitter)

	// At first, the time range will be [(t-30d-1d), (t-30d))
	maxTime := time.Now().UTC().Add(-e.config.Archiver.ArchiveAge)
	minTime := maxTime.Add(-e.config.Archiver.ArchiveStepSize)
	for {
		startTime := time.Now()
		queryReq, err := e.buildQueryRequest(minTime, maxTime)
		if err != nil {
			log.WithError(err).Error("buildQueryRequest failed")
			errChan <- err
			return
		}
		p := backoff.NewRetrier(e.retryPolicy)
		queryResp, err := e.queryJobs(context.Background(), queryReq, p)
		if err != nil {
			// TODO (adityacb): T1782505
			// C* timeouts can range up to 30 seconds.
			// Add a task to monitor these and fix retry policy in case
			// retrying 3 times after 30
			log.WithError(err).Error("job query failed. retries exhausted")
			e.metrics.ArchiverJobQueryFail.Inc(1)
			errChan <- err
			return
		}
		e.metrics.ArchiverJobQuerySuccess.Inc(1)

		results := queryResp.GetResults()

		if len(results) > 0 {
			archiveSummary := e.archiveJobs(context.Background(), results)
			succeededCount, _ := archiveSummary[archiverSuccessKey]
			failedCount, _ := archiveSummary[archiverFailureKey]
			log.WithFields(log.Fields{
				"total":     len(results),
				"succeeded": succeededCount,
				"failed":    failedCount,
				"minTime":   minTime.Format(time.RFC3339),
				"maxTime":   maxTime.Format(time.RFC3339),
			}).Info("Archive summary")
		} else {
			log.WithFields(log.Fields{
				"minTime": minTime.Format(time.RFC3339),
				"maxTime": maxTime.Format(time.RFC3339),
			}).Debug("No jobs in timerange")
			// TODO (adityacb): T1782511
			// Reset here if there are no more jobs left to be archived.
			// This means that for n consecutive attempts if we get no
			// results, we should move the archive window back to now - 30days
			e.metrics.ArchiverNoJobsInTimerange.Inc(1)
		}
		e.metrics.ArchiverRunDuration.Record(time.Since(startTime))

		// get new time range. example: [(minTime-1d), (minTime))
		maxTime = minTime
		minTime = minTime.Add(-e.config.Archiver.ArchiveStepSize)
		jitter := time.Duration(rand.Intn(jitterMax)) * time.Millisecond
		time.Sleep(e.config.Archiver.ArchiveInterval + jitter)
	}
}
