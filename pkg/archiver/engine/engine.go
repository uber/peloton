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
	"math/rand"
	nethttp "net/http"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/pkg/archiver/config"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"

	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
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

	// The key "filebeat_topic" will be used by filebeat to stream completed
	// jobs to kafka topic specified
	filebeatTopic = "filebeat_topic"

	// archiver summary map keys
	archiverSuccessKey = "SUCCESS"
	archiverFailureKey = "FAILURE"

	// Number of pod events run to persist in DB.
	_defaultPodEventsToConstraint = uint64(100)
)

// Engine defines the interface used to query a peloton component
// for data and then archive that data to secondary storage using
// message queue
type Engine interface {
	// Start starts the archiver goroutines
	Start() error
	// Cleanup cleans the archiver engine before
	// restart
	Cleanup()
}

type engine struct {
	// Jobmgr client to make Job Query/Delete API requests
	jobClient job.JobManagerYARPCClient
	// Task Manager Client to query task events.
	taskClient task.TaskManagerYARPCClient
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
	inbounds []transport.Inbound) (Engine, error) {
	cfg.Archiver.Normalize()

	jobmgrURL, err := discovery.GetAppURL(common.JobManagerRole)
	if err != nil {
		return nil, err
	}

	securityManager, err := auth_impl.CreateNewSecurityManager(&cfg.Auth)
	if err != nil {
		return nil, err
	}
	authInboundMiddleware := inbound.NewAuthInboundMiddleware(securityManager)

	securityClient, err := auth_impl.CreateNewSecurityClient(&cfg.Auth)
	if err != nil {
		return nil, err
	}
	authOutboundMiddleware := outbound.NewAuthOutboundMiddleware(securityClient)

	t := grpc.NewTransport()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     config.PelotonArchiver,
		Inbounds: inbounds,
		Outbounds: yarpc.Outbounds{
			common.PelotonJobManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(jobmgrURL.Host),
			},
		},
		Metrics: yarpc.MetricsConfig{
			Tally: scope,
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Oneway: authInboundMiddleware,
			Unary:  authInboundMiddleware,
			Stream: authInboundMiddleware,
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Oneway: authOutboundMiddleware,
			Unary:  authOutboundMiddleware,
			Stream: authOutboundMiddleware,
		},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	return &engine{
		jobClient: job.NewJobManagerYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		taskClient: task.NewTaskManagerYARPCClient(
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

// Start starts archiver with actions such as
// 1) archive terminal batch jobs
// 2) constraint pod events for RUNNING stateless jobs.
// Actions are iterated sequentially to minimize the load on
// Cassandra cluster to not impact real-time workload.
func (e *engine) Start() error {
	// Account for time taken for jobmgr to start and finish recovery
	// This is more of a precaution so that archiver does not mess
	// around with core jobmgr functionality.
	// TODO: remove this delay once we move to API server
	e.metrics.ArchiverStart.Inc(1)
	jitter := time.Duration(rand.Intn(jitterMax)) * time.Millisecond
	time.Sleep(e.config.Archiver.BootstrapDelay + jitter)
	// At first, the time range will be [(t-30d-1d), (t-30d))
	maxTime := time.Now().UTC().Add(-e.config.Archiver.ArchiveAge)
	minTime := maxTime.Add(-e.config.Archiver.ArchiveStepSize)

	for {
		if e.config.Archiver.Enable {
			startTime := time.Now()
			max, err := ptypes.TimestampProto(maxTime)
			if err != nil {
				return err
			}
			min, err := ptypes.TimestampProto(minTime)
			if err != nil {
				return err
			}

			spec := job.QuerySpec{
				JobStates: []job.JobState{
					job.JobState_SUCCEEDED,
					job.JobState_FAILED,
					job.JobState_KILLED,
				},
				CompletionTimeRange: &peloton.TimeRange{Min: min, Max: max},
				Pagination: &query.PaginationSpec{
					Offset:   0,
					Limit:    uint32(e.config.Archiver.MaxArchiveEntries),
					MaxLimit: uint32(e.config.Archiver.MaxArchiveEntries),
				},
			}

			if err := e.runArchiver(
				&job.QueryRequest{
					Spec:        &spec,
					SummaryOnly: true,
				},
				e.archiveJobs); err != nil {
				return err
			}

			e.metrics.ArchiverRun.Inc(1)
			e.metrics.ArchiverRunDuration.Record(time.Since(startTime))
			maxTime = minTime
			minTime = minTime.Add(-e.config.Archiver.ArchiveStepSize)
		}

		if e.config.Archiver.PodEventsCleanup {
			startTime := time.Now()
			spec := job.QuerySpec{
				JobStates: []job.JobState{
					job.JobState_RUNNING,
				},
				Pagination: &query.PaginationSpec{
					Offset:   0,
					Limit:    uint32(e.config.Archiver.MaxArchiveEntries),
					MaxLimit: uint32(e.config.Archiver.MaxArchiveEntries),
				},
			}

			if err := e.runArchiver(
				&job.QueryRequest{
					Spec:        &spec,
					SummaryOnly: true,
				},
				e.deletePodEvents); err != nil {
				return err
			}
			e.metrics.PodDeleteEventsRun.Inc(1)
			e.metrics.PodDeleteEventsRunDuration.Record(time.Since(startTime))
		}

		jitter := time.Duration(rand.Intn(jitterMax)) * time.Millisecond
		time.Sleep(e.config.Archiver.ArchiveInterval + jitter)
	}
}

// Cleanup cleans the archiver engine before restarting
func (e *engine) Cleanup() {
	e.dispatcher.Stop()
	return
}

// runArchiver runs the action(s) for Archiver
func (e *engine) runArchiver(
	queryReq *job.QueryRequest,
	action func(
		ctx context.Context,
		results []*job.JobSummary)) error {
	p := backoff.NewRetrier(e.retryPolicy)
	queryResp, err := e.queryJobs(
		context.Background(),
		queryReq,
		p)
	if err != nil {
		return err
	}

	results := queryResp.GetResults()
	action(
		context.Background(),
		results)
	return nil
}

// archiveJobs archives only batch jobs.
func (e *engine) archiveJobs(
	ctx context.Context,
	results []*job.JobSummary) {
	if len(results) > 0 {
		archiveSummary := map[string]int{archiverFailureKey: 0, archiverSuccessKey: 0}
		for _, summary := range results {
			if summary.GetType() != job.JobType_BATCH {
				continue
			}
			// Sleep between consecutive Job Delete requests
			time.Sleep(delayDelete)

			// The log event for completedJobTag will be logged to Archiver stdout
			// Filebeat configured on the Peloton host will ship this log out to
			// logstash. Logstash will be configured to stream this specific log
			// event to Hive via a heatpipe topic.
			log.WithFields(log.Fields{
				filebeatTopic:   e.config.Archiver.KafkaTopic,
				completedJobTag: summary,
			}).Info("completed job")

			if e.config.Archiver.StreamOnlyMode {
				continue
			}

			log.WithFields(log.Fields{
				"job_id": summary.GetId().GetValue(),
				"state":  summary.GetRuntime().GetState()}).
				Info("Deleting job")

			deleteReq := &job.DeleteRequest{
				Id: summary.GetId(),
			}
			ctx, cancel := context.WithTimeout(
				ctx,
				e.config.Archiver.PelotonClientTimeout,
			)
			defer cancel()
			_, err := e.jobClient.Delete(ctx, deleteReq)
			if err != nil {
				// TODO: have a reasonable threshold for tolerating such failures
				// For now, just continue processing the next job in the list
				log.WithError(err).
					WithField("job_id", summary.GetId().GetValue()).
					Error("job delete failed")
				e.metrics.ArchiverJobDeleteFail.Inc(1)
				archiveSummary[archiverFailureKey]++
			} else {
				e.metrics.ArchiverJobDeleteSuccess.Inc(1)
				archiveSummary[archiverSuccessKey]++
			}
		}
		succeededCount, _ := archiveSummary[archiverSuccessKey]
		failedCount, _ := archiveSummary[archiverFailureKey]
		e.metrics.ArchiverJobQuerySuccess.Inc(1)
		log.WithFields(log.Fields{
			"total":     len(results),
			"succeeded": succeededCount,
			"failed":    failedCount,
		}).Info("Archive summary")
	} else {
		log.Debug("No jobs in timerange")
		// TODO (adityacb)
		// Reset here if there are no more jobs left to be archived.
		// This means that for n consecutive attempts if we get no
		// results, we should move the archive window back to now - 30days
		e.metrics.ArchiverNoJobsInTimerange.Inc(1)
	}
}

// deletePodEvents reads RUNNING service jobs and deletes,
// runs (monotonically increasing counter) if more than 100.
// This action is to constraint #runs in DB, to prevent large partitions
// 1) Get the most recent run_id from DB.
// 2) If more than 100 runs exist, delete the delta.
func (e *engine) deletePodEvents(
	ctx context.Context,
	results []*job.JobSummary) {
	var i uint32
	for _, jobSummary := range results {
		if jobSummary.GetType() != job.JobType_SERVICE {
			continue
		}
		log.WithFields(log.Fields{
			"job_id": jobSummary.GetId().GetValue(),
			"state":  jobSummary.GetRuntime().GetState()}).
			Debug("Deleting pod events")

		for i = 0; i < jobSummary.GetInstanceCount(); i++ {
			ctx, cancel := context.WithTimeout(
				ctx,
				e.config.Archiver.PelotonClientTimeout,
			)
			defer cancel()

			response, err := e.taskClient.GetPodEvents(
				ctx,
				&task.GetPodEventsRequest{
					JobId:      jobSummary.GetId(),
					InstanceId: i,
					Limit:      1})
			if err != nil {
				log.WithFields(log.Fields{
					"job_id":      jobSummary.GetId(),
					"instance_id": i,
				}).WithError(err).
					Error("unable to fetch pod events")
				e.metrics.PodDeleteEventsFail.Inc(1)
				continue
			}

			if len(response.GetResult()) == 0 {
				continue
			}

			runID, err := util.ParseRunID(response.GetResult()[0].GetTaskId().GetValue())
			if err != nil {
				log.WithFields(log.Fields{
					"job_id":      jobSummary.GetId(),
					"instance_id": i,
					"run_id":      response.GetResult()[0].GetTaskId().GetValue(),
				}).WithError(err).
					Error("error parsing runID")
				e.metrics.PodDeleteEventsFail.Inc(1)
				continue
			}

			if runID > _defaultPodEventsToConstraint {
				log.WithFields(log.Fields{
					"job_id":      jobSummary.GetId(),
					"instance_id": i,
					"run_id":      runID - _defaultPodEventsToConstraint,
					"task_id":     response.GetResult()[0].GetTaskId().GetValue(),
				}).Info("Delete runs")
				ctx, cancel := context.WithTimeout(
					ctx,
					e.config.Archiver.PelotonClientTimeout,
				)
				defer cancel()
				_, err = e.taskClient.DeletePodEvents(
					ctx,
					&task.DeletePodEventsRequest{
						JobId:      jobSummary.GetId(),
						InstanceId: i,
						RunId:      runID - _defaultPodEventsToConstraint})
				if err != nil {
					log.WithFields(log.Fields{
						"job_id":      jobSummary.GetId(),
						"instance_id": i,
						"run_id":      response.GetResult()[0].GetTaskId().GetValue(),
					}).WithError(err).
						Error("unable to delete pod events")
					e.metrics.PodDeleteEventsFail.Inc(1)
					continue
				}
			}
			e.metrics.PodDeleteEventsSuccess.Inc(1)
		}
	}
}

func (e *engine) queryJobs(
	ctx context.Context,
	req *job.QueryRequest,
	p backoff.Retrier) (*job.QueryResponse, error) {
	for {
		ctx, cancel := context.WithTimeout(
			ctx,
			e.config.Archiver.PelotonClientTimeout,
		)
		resp, err := e.jobClient.Query(ctx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		if backoff.CheckRetry(p) {
			continue
		} else {
			return nil, err
		}
	}
}
