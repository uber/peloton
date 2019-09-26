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

package cassandra

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/query"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	pb_volume "github.com/uber/peloton/.gen/peloton/api/v0/volume"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/private/models"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"

	"github.com/uber/peloton/pkg/common"
	apiconvertor "github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/storage"
	"github.com/uber/peloton/pkg/storage/cassandra/api"
	"github.com/uber/peloton/pkg/storage/cassandra/impl"
	ormcassandra "github.com/uber/peloton/pkg/storage/connectors/cassandra"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	qb "github.com/uber/peloton/pkg/storage/querybuilder"

	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	taskIDFmt = "%s-%d"

	// DB table names
	jobConfigTable         = "job_config"
	jobRuntimeTable        = "job_runtime"
	jobIndexTable          = "job_index"
	taskConfigV2Table      = "task_config_v2"
	taskConfigTable        = "task_config"
	taskRuntimeTable       = "task_runtime"
	podEventsTable         = "pod_events"
	updatesTable           = "update_info"
	podWorkflowEventsTable = "pod_workflow_events"
	frameworksTable        = "frameworks"
	updatesByJobView       = "mv_updates_by_job"
	volumeTable            = "persistent_volumes"

	// DB field names
	creationTimeField   = "creation_time"
	completionTimeField = "completion_time"
	stateField          = "state"

	// Task query sort by field
	hostField       = "host"
	instanceIDField = "instanceId"
	messageField    = "message"
	nameField       = "name"
	reasonField     = "reason"

	_defaultQueryLimit    uint32 = 10
	_defaultQueryMaxLimit uint32 = 100

	_defaultWorkflowEventsDedupeWarnLimit = 1000

	jobIndexTimeFormat        = "20060102150405"
	jobQueryDefaultSpanInDays = 7
	jobQueryJitter            = time.Second * 30

	// _defaultPodEventsLimit is default number of pod events
	// to read if not provided for jobID + instanceID
	_defaultPodEventsLimit = 100

	// Default context timeout for the method to cleanup old
	// job updates from the storage
	_jobUpdatesCleanupTimeout = 120 * time.Second
)

// GenerateTestCassandraConfig generates a test config for local C* client
// This is meant for sharing testing code only, not for production
func GenerateTestCassandraConfig() *Config {
	return &Config{
		CassandraConn: &impl.CassandraConn{
			ContactPoints: []string{"127.0.0.1"},
			Port:          9043,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
		},
		StoreName:  "peloton_test",
		Migrations: "migrations",
		Replication: &Replication{
			Strategy: "SimpleStrategy",
			Replicas: []*Replica{
				{
					Name:  "replication_factor",
					Value: 1,
				},
			},
		},
	}
}

// ToOrmConfig is needed to generate ORM config from legacy config so that the
// ORM code doesn't depend on legacy storage code and can be imported into the
// legacy code
func ToOrmConfig(c *Config) *ormcassandra.Config {
	return &ormcassandra.Config{
		CassandraConn: &ormcassandra.CassandraConn{
			ContactPoints:      c.CassandraConn.ContactPoints,
			Port:               c.CassandraConn.Port,
			Username:           c.CassandraConn.Username,
			Password:           c.CassandraConn.Password,
			Consistency:        c.CassandraConn.Consistency,
			ConnectionsPerHost: c.CassandraConn.ConnectionsPerHost,
			Timeout:            c.CassandraConn.Timeout,
			SocketKeepalive:    c.CassandraConn.SocketKeepalive,
			ProtoVersion:       c.CassandraConn.ProtoVersion,
			TTL:                c.CassandraConn.TTL,
			LocalDCOnly:        c.CassandraConn.LocalDCOnly,
			DataCenter:         c.CassandraConn.DataCenter,
			PageSize:           c.CassandraConn.PageSize,
			RetryCount:         c.CassandraConn.RetryCount,
			HostPolicy:         c.CassandraConn.HostPolicy,
			TimeoutLimit:       c.CassandraConn.TimeoutLimit,
			CQLVersion:         c.CassandraConn.CQLVersion,
			MaxGoRoutines:      c.CassandraConn.MaxGoRoutines,
		},
		StoreName: c.StoreName,
	}
}

type luceneClauses []string

// AutoMigrate migrates the db schemas for cassandra
func (c *Config) AutoMigrate() []error {
	connString := c.MigrateString()
	errs, ok := migrate.UpSync(connString, c.Migrations)
	log.Infof("UpSync complete")
	if !ok {
		log.Errorf("UpSync failed with errors: %v", errs)
		return errs
	}
	return nil
}

// MigrateString returns the db string required for database migration
// The code assumes that the keyspace (indicated by StoreName) is already created
func (c *Config) MigrateString() string {
	// see https://github.com/gemnasium/migrate/pull/17 on why disable_init_host_lookup is needed
	// This is for making local testing faster with docker running on mac
	connStr := fmt.Sprintf("cassandra://%v:%v/%v?protocol=4&disable_init_host_lookup",
		c.CassandraConn.ContactPoints[0],
		c.CassandraConn.Port,
		c.StoreName)

	if len(c.CassandraConn.Username) != 0 {
		connStr = fmt.Sprintf("cassandra://%v:%v@%v:%v/%v",
			c.CassandraConn.Username,
			c.CassandraConn.Password,
			c.CassandraConn.ContactPoints[0],
			c.CassandraConn.Port,
			c.StoreName)
	}
	connStr = strings.Replace(connStr, " ", "", -1)
	log.Infof("Cassandra migration string %v", connStr)
	return connStr
}

// Store implements JobStore, TaskStore, UpdateStore, FrameworkInfoStore,
// and PersistentVolumeStore using a cassandra backend
// TODO: Break this up into different files (and or structs) that implement
// each of these interfaces to keep code modular.
type Store struct {
	DataStore          api.DataStore
	jobConfigOps       ormobjects.JobConfigOps
	jobRuntimeOps      ormobjects.JobRuntimeOps
	jobUpdateEventsOps ormobjects.JobUpdateEventsOps
	taskConfigV2Ops    ormobjects.TaskConfigV2Ops
	metrics            *storage.Metrics
	Conf               *Config
	retryPolicy        backoff.RetryPolicy
}

// NewStore creates a Store
func NewStore(config *Config, scope tally.Scope) (*Store, error) {
	dataStore, err := impl.CreateStore(config.CassandraConn, config.StoreName, scope)
	if err != nil {
		log.Errorf("Failed to NewStore, err=%v", err)
		return nil, err
	}
	ormStore, ormErr := ormobjects.NewCassandraStore(
		ToOrmConfig(config),
		scope)
	if ormErr != nil {
		log.WithError(ormErr).Fatal("Failed to create ORM store for Cassandra")
	}

	return &Store{
		DataStore: dataStore,

		// DO NOT ADD MORE ORM Objects here. These are added here for
		// supporting Job.Query() which cannot be fully moved to ORM
		jobConfigOps:       ormobjects.NewJobConfigOps(ormStore),
		jobRuntimeOps:      ormobjects.NewJobRuntimeOps(ormStore),
		jobUpdateEventsOps: ormobjects.NewJobUpdateEventsOps(ormStore),
		taskConfigV2Ops:    ormobjects.NewTaskConfigV2Ops(ormStore),

		metrics:     storage.NewMetrics(scope.SubScope("storage")),
		Conf:        config,
		retryPolicy: backoff.NewRetryPolicy(5, 50*time.Millisecond),
	}, nil
}

func (s *Store) handleDataStoreError(err error, p backoff.Retrier) error {
	retry := false
	newErr := err

	switch err.(type) {
	// TBD handle errOverloaded and errBootstrapping after error types added in gocql
	case *gocql.RequestErrReadFailure:
		s.metrics.ErrorMetrics.ReadFailure.Inc(1)
		return yarpcerrors.AbortedErrorf("read failure during statement execution %v", err.Error())
	case *gocql.RequestErrWriteFailure:
		s.metrics.ErrorMetrics.WriteFailure.Inc(1)
		return yarpcerrors.AbortedErrorf("write failure during statement execution %v", err.Error())
	case *gocql.RequestErrAlreadyExists:
		s.metrics.ErrorMetrics.AlreadyExists.Inc(1)
		return yarpcerrors.AlreadyExistsErrorf("already exists error during statement execution %v", err.Error())
	case *gocql.RequestErrReadTimeout:
		s.metrics.ErrorMetrics.ReadTimeout.Inc(1)
		return yarpcerrors.DeadlineExceededErrorf("read timeout during statement execution: %v", err.Error())
	case *gocql.RequestErrWriteTimeout:
		s.metrics.ErrorMetrics.WriteTimeout.Inc(1)
		return yarpcerrors.DeadlineExceededErrorf("write timeout during statement execution: %v", err.Error())
	case *gocql.RequestErrUnavailable:
		s.metrics.ErrorMetrics.RequestUnavailable.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("request unavailable during statement execution: %v", err.Error())
	}

	switch err {
	case gocql.ErrTooManyTimeouts:
		s.metrics.ErrorMetrics.TooManyTimeouts.Inc(1)
		return yarpcerrors.DeadlineExceededErrorf("too many timeouts during statement execution: %v", err.Error())
	case gocql.ErrUnavailable:
		s.metrics.ErrorMetrics.ConnUnavailable.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("unavailable error during statement execution: %v", err.Error())
	case gocql.ErrSessionClosed:
		s.metrics.ErrorMetrics.SessionClosed.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("session closed during statement execution: %v", err.Error())
	case gocql.ErrNoConnections:
		s.metrics.ErrorMetrics.NoConnections.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("no connections during statement execution: %v", err.Error())
	case gocql.ErrConnectionClosed:
		s.metrics.ErrorMetrics.ConnectionClosed.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("connections closed during statement execution: %v", err.Error())
	case gocql.ErrNoStreams:
		s.metrics.ErrorMetrics.NoStreams.Inc(1)
		retry = true
		newErr = yarpcerrors.UnavailableErrorf("no streams during statement execution: %v", err.Error())
	}

	if retry {
		if backoff.CheckRetry(p) {
			return nil
		}
		return newErr
	}

	return newErr
}

func (s *Store) executeWrite(ctx context.Context, stmt api.Statement) (api.ResultSet, error) {
	p := backoff.NewRetrier(s.retryPolicy)
	for {
		result, err := s.DataStore.Execute(ctx, stmt)
		if err == nil {
			return result, err
		}
		err = s.handleDataStoreError(err, p)

		if err != nil {
			if !common.IsTransientError(err) {
				s.metrics.ErrorMetrics.NotTransient.Inc(1)
			}
			return result, err
		}
	}
}

func (s *Store) executeRead(
	ctx context.Context,
	stmt api.Statement) ([]map[string]interface{}, error) {
	p := backoff.NewRetrier(s.retryPolicy)
	for {
		result, err := s.DataStore.Execute(ctx, stmt)
		if err == nil {
			if result != nil {
				defer result.Close()
			}
			allResults, nErr := result.All(ctx)
			if nErr == nil {
				return allResults, nErr
			}
			result.Close()
			err = nErr
		}
		err = s.handleDataStoreError(err, p)

		if err != nil {
			if !common.IsTransientError(err) {
				s.metrics.ErrorMetrics.NotTransient.Inc(1)
			}
			return nil, err
		}
	}
}

// Compress a blob using gzip
func compress(buffer []byte) ([]byte, error) {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(buffer); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Uncompress a blob using gzip, return original blob if it was not compressed
func uncompress(buffer []byte) ([]byte, error) {
	b := bytes.NewBuffer(buffer)
	r, err := gzip.NewReader(b)

	if err != nil {
		if err == gzip.ErrHeader {
			// blob was not compressed, so we can ignore this error. We can
			// look for only checksum errors which will mean data corruption
			return buffer, nil
		}
		return nil, err
	}

	defer r.Close()
	uncompressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return uncompressed, nil
}

// GetMaxJobConfigVersion returns the maximum version of configs of a given job
func (s *Store) GetMaxJobConfigVersion(
	ctx context.Context,
	jobID string) (uint64, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("MAX(version)").From(jobConfigTable).
		Where(qb.Eq{"job_id": jobID})

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to get max version of job %v: %v", jobID, err)
		return 0, err
	}

	log.Debugf("max version: %v", allResults)
	for _, value := range allResults {
		for _, max := range value {
			// version is store as big int in Cassandra
			// gocql would cast big int to int64
			return uint64(max.(int64)), nil
		}
	}
	return 0, nil
}

// WithTimeRangeFilter will take timerange and time_field (creation_time|completion_time) as
// input and create a range filter on those fields and append to the clauses list
func (c *luceneClauses) WithTimeRangeFilter(timeRange *peloton.TimeRange, timeField string) error {
	if timeRange == nil || c == nil {
		return nil
	}
	if timeField != creationTimeField && timeField != completionTimeField {
		return fmt.Errorf("Invalid time field %s", timeField)
	}
	// Create filter if time range is not nil
	min, err := ptypes.Timestamp(timeRange.GetMin())
	if err != nil {
		log.WithField("timeRange", timeRange).
			WithField("timeField", timeField).
			WithError(err).
			Error("fail to get min time range")
		return err
	}
	max, err := ptypes.Timestamp(timeRange.GetMax())
	if err != nil {
		log.WithField("timeRange", timeRange).
			WithField("timeField", timeField).
			WithError(err).
			Error("fail to get max time range")
		return err
	}
	// validate min and max limits are legit (i.e. max > min)
	if max.Before(min) {
		return fmt.Errorf("Incorrect timerange")
	}
	timeRangeMinStr := fmt.Sprintf(min.Format(jobIndexTimeFormat))
	timeRangeMaxStr := fmt.Sprintf(max.Format(jobIndexTimeFormat))
	*c = append(*c, fmt.Sprintf(`{type: "range", field:"%s", lower: "%s", upper: "%s", include_lower: true}`, timeField, timeRangeMinStr, timeRangeMaxStr))
	return nil
}

// QueryJobs returns all jobs in the resource pool that matches the spec.
func (s *Store) QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec, summaryOnly bool) ([]*job.JobInfo, []*job.JobSummary, uint32, error) {
	// Query is based on stratio lucene index on jobs.
	// See https://github.com/Stratio/cassandra-lucene-index
	// We are using "must" for the labels and only return the jobs that contains all
	// label values
	// TODO: investigate if there are any golang library that can build lucene query
	var clauses luceneClauses

	if spec == nil {
		return nil, nil, 0, nil
	}

	// Labels field must contain value of the specified labels
	for _, label := range spec.GetLabels() {
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"labels", values:%s}`, strconv.Quote(label.Value)))
	}

	// jobconfig field must contain all specified keywords
	for _, word := range spec.GetKeywords() {
		// Lucene for some reason does wildcard search as case insensitive
		// However, to match individual words we still need to match
		// by exact keyword. Using boolean filter to do this.
		// using the "should" syntax will enable us to match on either
		// wildcard search or exact match
		wildcardWord := fmt.Sprintf("*%s*", strings.ToLower(word))
		clauses = append(clauses, fmt.Sprintf(
			`{type: "boolean",`+
				`should: [`+
				`{type: "wildcard", field:"config", value:%s},`+
				`{type: "match", field:"config", value:%s}`+
				`]`+
				`}`, strconv.Quote(wildcardWord), strconv.Quote(word)))
	}

	// Add support on query by job state
	// queryTerminalStates will be set if the spec contains any
	// terminal job state. In this case we will restrict the
	// job query to query for jobs over the last 7 days.
	// This is a temporary fix so that lucene index query doesn't
	// time out when searching for ALL jobs with terminal states
	//  which is a huge number.
	// TODO (adityacb): change this once we have query spec support
	// a custom time range
	queryTerminalStates := false
	if len(spec.GetJobStates()) > 0 {
		values := ""
		for i, s := range spec.GetJobStates() {
			if util.IsPelotonJobStateTerminal(s) {
				queryTerminalStates = true
			}
			values = values + strconv.Quote(s.String())
			if i < len(spec.JobStates)-1 {
				values = values + ","
			}
		}
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"state", values:[%s]}`, values))
	}

	if respoolID != nil {
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"respool_id", values:%s}`, strconv.Quote(respoolID.GetValue())))
	}

	owner := spec.GetOwner()
	if owner != "" {
		clauses = append(clauses, fmt.Sprintf(`{type: "match", field:"owner", value:%s}`, strconv.Quote(owner)))
	}

	name := spec.GetName()
	if name != "" {
		wildcardName := fmt.Sprintf("*%s*", name)
		clauses = append(clauses, fmt.Sprintf(`{type: "wildcard", field:"name", value:%s}`, strconv.Quote(wildcardName)))
	}

	creationTimeRange := spec.GetCreationTimeRange()
	completionTimeRange := spec.GetCompletionTimeRange()
	err := clauses.WithTimeRangeFilter(creationTimeRange, creationTimeField)
	if err != nil {
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, nil, 0, err
	}

	err = clauses.WithTimeRangeFilter(completionTimeRange, completionTimeField)
	if err != nil {
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, nil, 0, err
	}

	// If no time range is specified in query spec, but the query is for terminal state,
	// use default time range
	if creationTimeRange == nil && completionTimeRange == nil && queryTerminalStates {
		// Add jobQueryJitter to max bound to account for jobs
		// that have just been created.
		// if time range is not specified and the job is in terminal state,
		// apply a default range of last 7 days
		// TODO (adityacb): remove artificially enforcing default time range for
		// completed jobs once UI supports query by time range.
		now := time.Now().Add(jobQueryJitter).UTC()
		max, err := ptypes.TimestampProto(now)
		if err != nil {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		min, err := ptypes.TimestampProto(now.AddDate(0, 0, -jobQueryDefaultSpanInDays))
		if err != nil {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		defaultCreationTimeRange := &peloton.TimeRange{Min: min, Max: max}
		err = clauses.WithTimeRangeFilter(defaultCreationTimeRange, "creation_time")
		if err != nil {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
	}

	where := `expr(job_index_lucene_v2, '{filter: [`
	for i, c := range clauses {
		if i > 0 {
			where += ", "
		}
		where += c
	}
	where += "]"

	// add default sorting by creation time in descending order in case orderby
	// is not specificed in the query spec
	var orderBy = spec.GetPagination().GetOrderBy()
	if orderBy == nil || len(orderBy) == 0 {
		orderBy = []*query.OrderBy{
			{
				Order: query.OrderBy_DESC,
				Property: &query.PropertyPath{
					Value: "creation_time",
				},
			},
		}
	}

	// add sorter into the query
	where += ", sort:["
	count := 0
	for _, order := range orderBy {
		where += fmt.Sprintf("{field: \"%s\"", order.Property.GetValue())
		if order.Order == query.OrderBy_DESC {
			where += ", reverse: true"
		}
		where += "}"
		if count < len(orderBy)-1 {
			where += ","
		}
		count++
	}
	where += "]"

	where += "}')"

	maxLimit := _defaultQueryMaxLimit
	if spec.GetPagination().GetMaxLimit() != 0 {
		maxLimit = spec.GetPagination().GetMaxLimit()
	}
	where += fmt.Sprintf(" Limit %d", maxLimit)

	log.WithField("where", where).Debug("query string")

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("job_id",
		"name",
		"owner",
		"job_type",
		"respool_id",
		"instance_count",
		"labels",
		"runtime_info").
		From(jobIndexTable)
	stmt = stmt.Where(where)

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		uql, args, _, _ := stmt.ToUql()
		log.WithField("labels", spec.GetLabels()).
			WithField("uql", uql).
			WithField("args", args).
			WithError(err).
			Error("fail to query jobs")
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, nil, 0, err
	}

	total := uint32(len(allResults))

	// Apply offset and limit.
	begin := spec.GetPagination().GetOffset()
	if begin > total {
		begin = total
	}
	allResults = allResults[begin:]

	end := _defaultQueryLimit
	if spec.GetPagination() != nil {
		limit := spec.GetPagination().GetLimit()
		if limit > 0 {
			// end should not be 0, it will yield in empty result
			end = limit
		}
	}
	if end > uint32(len(allResults)) {
		end = uint32(len(allResults))
	}
	allResults = allResults[:end]

	summaryResults, err := s.getJobSummaryFromResultMap(ctx, allResults)
	if summaryOnly {
		if err != nil {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		// Lucene index entry for some batch jobs may be out of sync with the
		// base job_index table. Scrub such jobs from the summary list.
		summaryResults, err := s.reconcileStaleBatchJobsFromJobSummaryList(
			ctx, summaryResults, queryTerminalStates)
		if err != nil {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		s.metrics.JobMetrics.JobQuery.Inc(1)
		return nil, summaryResults, total, nil
	}

	var results []*job.JobInfo
	for _, value := range allResults {
		id, ok := value["job_id"].(qb.UUID)
		if !ok {
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, fmt.Errorf("got invalid response from cassandra")
		}

		jobID := &peloton.JobID{
			Value: id.String(),
		}

		jobRuntime, err := s.jobRuntimeOps.Get(ctx, jobID)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id.String()).
				Warn("no job runtime found when executing jobs query")
			continue
		}
		// TODO (chunyang.shen): use job/task cache to get JobConfig T1760469
		jobConfig, _, err := s.jobConfigOps.GetCurrentVersion(ctx, jobID)
		if err != nil {
			log.WithField("labels", spec.GetLabels()).
				WithField("job_id", id.String()).
				WithError(err).
				Error("fail to query jobs as not able to get job config")
			continue
		}

		// Unset instance config as its size can be huge as a workaround for UI query.
		// We should figure out long term support for grpc size limit.
		jobConfig.InstanceConfig = nil

		results = append(results, &job.JobInfo{
			Id:      jobID,
			Config:  jobConfig,
			Runtime: jobRuntime,
		})
	}

	s.metrics.JobMetrics.JobQuery.Inc(1)
	return results, summaryResults, total, nil
}

// CreateTaskRuntime creates a task runtime for a peloton job
func (s *Store) CreateTaskRuntime(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *task.RuntimeInfo,
	owner string,
	jobType job.JobType) error {
	runtimeBuffer, err := proto.Marshal(runtime)
	if err != nil {
		log.WithField("job_id", jobID.GetValue()).
			WithField("instance_id", instanceID).
			WithError(err).
			Error("Failed to create task runtime")
		s.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(taskRuntimeTable).
		Columns(
			"job_id",
			"instance_id",
			"version",
			"update_time",
			"state",
			"runtime_info").
		Values(
			jobID.GetValue(),
			instanceID,
			runtime.GetRevision().GetVersion(),
			time.Now().UTC(),
			runtime.GetState().String(),
			runtimeBuffer)

	// IfNotExist() will cause Writing task runtimes to Cassandra concurrently
	// failed with Operation timed out issue when batch size is small, e.g. 1.
	// For now, we have to drop the IfNotExist()

	taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
	if err := s.applyStatement(ctx, stmt, taskID); err != nil {
		s.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.TaskCreate.Inc(1)

	err = s.addPodEvent(ctx, jobID, instanceID, runtime)
	if err != nil {
		log.Errorf("Unable to log task state changes for job ID %v instance %v, error = %v", jobID.GetValue(), instanceID, err)
		return err
	}
	return nil
}

// addPodEvent upserts single pod state change for a Job -> Instance -> Run.
// Task state events are sorted by reverse chronological run_id and time of event.
func (s *Store) addPodEvent(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *task.RuntimeInfo) error {
	var runID, prevRunID, desiredRunID uint64
	var err, errMessage error

	errLog := false
	if runID, err = util.ParseRunID(
		runtime.GetMesosTaskId().GetValue()); err != nil {
		errLog = true
		errMessage = err
	}
	// when creating a task, GetPrevMesosTaskId is empty,
	// set prevRunID to 0
	if len(runtime.GetPrevMesosTaskId().GetValue()) == 0 {
		prevRunID = 0
	} else if prevRunID, err = util.ParseRunID(
		runtime.GetPrevMesosTaskId().GetValue()); err != nil {
		errLog = true
		errMessage = err
	}

	// old job does not have desired mesos task id, make it the same as runID
	// TODO: remove the line after all tasks have desired mesos task id
	if len(runtime.GetDesiredMesosTaskId().GetValue()) == 0 {
		desiredRunID = runID
	} else if desiredRunID, err = util.ParseRunID(
		runtime.GetDesiredMesosTaskId().GetValue()); err != nil {
		errLog = true
		errMessage = err
	}

	if errLog {
		s.metrics.TaskMetrics.PodEventsAddFail.Inc(1)
		return errMessage
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(podEventsTable).
		Columns(
			"job_id",
			"instance_id",
			"run_id",
			"desired_run_id",
			"previous_run_id",
			"update_time",
			"actual_state",
			"goal_state",
			"healthy",
			"hostname",
			"agent_id",
			"config_version",
			"desired_config_version",
			"volumeID",
			"message",
			"reason",
			"update_timestamp").
		Values(
			jobID.GetValue(),
			instanceID,
			runID,
			desiredRunID,
			prevRunID,
			qb.UUID{UUID: gocql.UUIDFromTime(time.Now())},
			runtime.GetState().String(),
			runtime.GetGoalState().String(),
			runtime.GetHealthy().String(),
			runtime.GetHost(),
			runtime.GetAgentID().GetValue(),
			runtime.GetConfigVersion(),
			runtime.GetDesiredConfigVersion(),
			runtime.GetVolumeID().GetValue(),
			runtime.GetMessage(),
			runtime.GetReason(),
			time.Now()).Into(podEventsTable)

	err = s.applyStatement(ctx, stmt, runtime.GetMesosTaskId().GetValue())
	if err != nil {
		s.metrics.TaskMetrics.PodEventsAddFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.PodEventsAddSuccess.Inc(1)
	return nil
}

// GetPodEvents returns pod events for a Job + Instance + PodID (optional)
// Pod events are sorted by PodID + Timestamp
// only is called from this file
func (s *Store) GetPodEvents(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	podID ...string) ([]*pod.PodEvent, error) {
	var stmt qb.SelectBuilder
	queryBuilder := s.DataStore.NewQuery()

	// Events are sorted in descinding order by PodID and then update time.
	stmt = queryBuilder.Select("*").From(podEventsTable).
		Where(qb.Eq{
			"job_id":      jobID,
			"instance_id": instanceID})

	if len(podID) > 0 && len(podID[0]) > 0 {
		runID, err := util.ParseRunID(podID[0])
		if err != nil {
			return nil, err
		}
		stmt = stmt.Where(qb.Eq{"run_id": runID})
	} else {
		statement := queryBuilder.Select("run_id").From(podEventsTable).
			Where(qb.Eq{
				"job_id":      jobID,
				"instance_id": instanceID}).
			Limit(1)
		res, err := s.executeRead(ctx, statement)
		if err != nil {
			s.metrics.TaskMetrics.PodEventsGetFail.Inc(1)
			return nil, err
		}
		for _, value := range res {
			stmt = stmt.Where(qb.Eq{"run_id": value["run_id"].(int64)})
		}
	}

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		s.metrics.TaskMetrics.PodEventsGetFail.Inc(1)
		return nil, err
	}

	var podEvents []*pod.PodEvent
	b := bytes.Buffer{}
	b.WriteString(jobID)
	b.WriteString("-")
	b.WriteString(strconv.FormatUint(uint64(instanceID), 10))
	podName := b.String()
	for _, value := range allResults {
		podEvent := &pod.PodEvent{}

		b.Reset()
		b.WriteString(podName)
		b.WriteString("-")
		b.WriteString(strconv.FormatInt(value["run_id"].(int64), 10))
		mesosTaskID := b.String()

		b.Reset()
		b.WriteString(podName)
		b.WriteString("-")
		b.WriteString(strconv.FormatInt(value["previous_run_id"].(int64), 10))
		prevMesosTaskID := b.String()

		b.Reset()
		b.WriteString(podName)
		b.WriteString("-")
		b.WriteString(strconv.FormatInt(value["desired_run_id"].(int64), 10))
		desiredMesosTaskID := b.String()

		// Set podEvent fields
		podEvent.PodId = &v1alphapeloton.PodID{
			Value: mesosTaskID,
		}
		podEvent.PrevPodId = &v1alphapeloton.PodID{
			Value: prevMesosTaskID,
		}
		podEvent.DesiredPodId = &v1alphapeloton.PodID{
			Value: desiredMesosTaskID,
		}

		podEvent.Timestamp =
			value["update_time"].(qb.UUID).Time().Format(time.RFC3339)

		podEvent.Version = versionutil.GetPodEntityVersion(
			uint64(value["config_version"].(int64)))
		podEvent.DesiredVersion = versionutil.GetPodEntityVersion(
			uint64(value["desired_config_version"].(int64)))

		podEvent.ActualState = apiconvertor.ConvertTaskStateToPodState(
			task.TaskState(task.TaskState_value[value["actual_state"].(string)])).String()
		podEvent.DesiredState = apiconvertor.ConvertTaskStateToPodState(
			task.TaskState(task.TaskState_value[value["goal_state"].(string)])).String()

		podEvent.Healthy = pod.HealthState(
			task.HealthState_value[value["healthy"].(string)]).String()

		podEvent.Message = value["message"].(string)
		podEvent.Reason = value["reason"].(string)
		podEvent.AgentId = value["agent_id"].(string)
		podEvent.Hostname = value["hostname"].(string)

		podEvents = append(podEvents, podEvent)
	}
	s.metrics.TaskMetrics.PodEventsGetSucess.Inc(1)

	return podEvents, nil
}

// DeletePodEvents deletes the pod events for provided JobID,
// InstanceID and RunID in the range [fromRunID-toRunID)
func (s *Store) DeletePodEvents(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	fromRunID uint64,
	toRunID uint64,
) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Delete(podEventsTable).
		Where(qb.Eq{"job_id": jobID, "instance_id": instanceID}).
		Where("run_id >= ?", fromRunID).
		Where("run_id < ?", toRunID)
	if err := s.applyStatement(ctx, stmt, jobID); err != nil {
		s.metrics.TaskMetrics.PodEventsDeleteFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.PodEventsDeleteSucess.Inc(1)
	return nil
}

// GetTasksForJobResultSet returns the result set that can be used to iterate each task in a job
// Caller need to call result.Close()
func (s *Store) GetTasksForJobResultSet(ctx context.Context, id *peloton.JobID) ([]map[string]interface{}, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID})
	result, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobResultSet by jobId %v, err=%v", jobID, err)
		return nil, err
	}
	return result, nil
}

// GetTasksForJob returns all the task runtimes (no configuration) in a map of tasks.TaskInfo for a peloton job
func (s *Store) GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error) {
	allResults, err := s.GetTasksForJobResultSet(ctx, id)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Fail to GetTasksForJob")
		s.metrics.TaskMetrics.TaskGetForJobFail.Inc(1)
		return nil, err
	}
	resultMap := make(map[uint32]*task.TaskInfo)

	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("value", value).
				WithError(err).
				Error("Failed to Fill into TaskRuntimeRecord")
			s.metrics.TaskMetrics.TaskGetForJobFail.Inc(1)
			continue
		}
		runtime, err := record.GetTaskRuntime()
		if err != nil {
			log.WithField("record", record).
				WithError(err).
				Error("Failed to parse task runtime from record")
			s.metrics.TaskMetrics.TaskGetForJobFail.Inc(1)
			continue
		}

		taskInfo := &task.TaskInfo{
			Runtime:    runtime,
			InstanceId: uint32(record.InstanceID),
			JobId:      id,
		}

		s.metrics.TaskMetrics.TaskGetForJob.Inc(1)
		resultMap[taskInfo.InstanceId] = taskInfo
	}
	return resultMap, nil
}

// GetTaskConfigs returns the task configs for a list of instance IDs,
// job ID and config version.
func (s *Store) GetTaskConfigs(ctx context.Context, id *peloton.JobID,
	instanceIDs []uint32, version uint64) (map[uint32]*task.TaskConfig, *models.ConfigAddOn, error) {
	taskConfigMap := make(map[uint32]*task.TaskConfig)
	var configAddOn *models.ConfigAddOn
	var backFill bool

	// add default instance ID to read the default config
	var dbInstanceIDs []int
	for _, instance := range instanceIDs {
		dbInstanceIDs = append(dbInstanceIDs, int(instance))
	}
	dbInstanceIDs = append(dbInstanceIDs, common.DefaultTaskConfigID)

	stmt := s.DataStore.NewQuery().Select("*").From(taskConfigV2Table).
		Where(
			qb.Eq{
				"job_id":      id.GetValue(),
				"version":     version,
				"instance_id": dbInstanceIDs,
			})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithField("instance_ids", instanceIDs).
			WithField("version", version).
			WithError(err).
			Error("Failed to get task configs")
		s.metrics.TaskMetrics.TaskGetConfigsFail.Inc(1)
		return taskConfigMap, nil, err
	}

	if len(allResults) == 0 {
		// Try to get task configs from legacy task_config table
		stmt := s.DataStore.NewQuery().Select("*").From(taskConfigTable).
			Where(
				qb.Eq{
					"job_id":      id.GetValue(),
					"version":     version,
					"instance_id": dbInstanceIDs,
				})
		allResults, err = s.executeRead(ctx, stmt)
		if err != nil {
			s.metrics.TaskMetrics.TaskGetConfigsFail.Inc(1)
			return taskConfigMap,
				nil,
				errors.Wrap(
					err,
					fmt.Sprintf(
						"failed to get task configs for %v", id.GetValue()),
				)
		}
		if len(allResults) == 0 {
			return taskConfigMap, nil, nil
		}
		s.metrics.TaskMetrics.TaskGetConfigLegacy.Inc(1)
		backFill = true
	}

	var defaultConfig *task.TaskConfig
	// Read all the overridden task configs and the default task config
	for _, value := range allResults {
		var record TaskConfigRecord
		if err := FillObject(value, &record, reflect.TypeOf(record)); err != nil {
			log.WithField("value", value).
				WithError(err).
				Error("Failed to Fill into TaskRecord")
			s.metrics.TaskMetrics.TaskGetConfigsFail.Inc(1)
			return nil, nil, err
		}
		taskConfig, err := record.GetTaskConfig()
		if err != nil {
			return nil, nil, err
		}
		if record.InstanceID == common.DefaultTaskConfigID {
			// get the default config
			defaultConfig = taskConfig
			continue
		}
		taskConfigMap[uint32(record.InstanceID)] = taskConfig
		// Read config addon from the first result entry. This is because config
		// add-on is same for all tasks of a job
		if configAddOn != nil {
			continue
		}
		if configAddOn, err = record.GetConfigAddOn(); err != nil {
			log.WithField("value", value).
				WithError(err).
				Error("Failed to Unmarshal system labels")
			s.metrics.TaskMetrics.TaskGetConfigsFail.Inc(1)
			return nil, nil, err
		}
	}

	// Fill the instances which don't have a overridden config with the default
	// config
	for _, instance := range instanceIDs {
		if _, ok := taskConfigMap[instance]; !ok {
			// use the default config for this instance
			if defaultConfig == nil {
				// we should never be here.
				// Either every instance has a override config or we have a
				// default config.
				s.metrics.TaskMetrics.TaskGetConfigFail.Inc(1)
				return nil, nil, yarpcerrors.NotFoundErrorf("unable to read default task config")
			}
			taskConfigMap[instance] = defaultConfig
		}
	}

	if backFill {
		// back fill entry from task_config to task_config_v2
		worker := func(i uint32) error {
			var cfg *task.TaskConfig
			var ok bool
			if cfg, ok = taskConfigMap[i]; !ok {
				return yarpcerrors.NotFoundErrorf(
					"failed to get config for instance %v", id,
				)
			}
			return s.taskConfigV2Ops.Create(
				ctx,
				id,
				int64(i),
				cfg,
				configAddOn,
				nil,
				version,
			)
		}
		err := util.RunInParallel(id.GetValue(), instanceIDs, worker)
		if err != nil {
			log.WithError(err).Info("failed to backfill task_config_v2")
			s.metrics.TaskMetrics.TaskConfigBackFillFail.Inc(1)
		} else {
			s.metrics.TaskMetrics.TaskConfigBackFill.Inc(1)
		}
	}
	s.metrics.TaskMetrics.TaskGetConfigs.Inc(1)
	return taskConfigMap, configAddOn, nil
}

func (s *Store) getTaskInfoFromRuntimeRecord(ctx context.Context, id *peloton.JobID, record *TaskRuntimeRecord) (*task.TaskInfo, error) {
	runtime, err := record.GetTaskRuntime()
	if err != nil {
		log.Errorf("Failed to parse task runtime from record, val = %v err= %v", record, err)
		return nil, err
	}

	config, _, err := s.taskConfigV2Ops.GetTaskConfig(ctx, id, uint32(record.InstanceID),
		runtime.ConfigVersion)
	if err != nil {
		return nil, err
	}

	return &task.TaskInfo{
		Runtime:    runtime,
		Config:     config,
		InstanceId: uint32(record.InstanceID),
		JobId:      id,
	}, nil
}

// GetTasksForJobAndStates returns the tasks for a peloton job which are in
// one of the specified states.
// result map key is TaskID, value is TaskHost
func (s *Store) GetTasksForJobAndStates(
	ctx context.Context,
	id *peloton.JobID,
	states []task.TaskState) (map[uint32]*task.TaskInfo, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	taskStates := make(map[string]bool)
	for _, state := range states {
		taskStates[state.String()] = true
	}
	stmt := queryBuilder.Select("instance_id", "state").From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Error("Failed to GetTasksForJobAndStates")
		s.metrics.TaskMetrics.TaskGetForJobAndStatesFail.Inc(1)
		return nil, err
	}
	resultMap := make(map[uint32]*task.TaskInfo)

	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				WithField("value", value).
				Error("GetTasksForJobAndStates failed to Fill into TaskRecord")
			s.metrics.TaskMetrics.TaskGetForJobAndStatesFail.Inc(1)
			return nil, err
		}
		for i := 0; i < len(taskStates); i++ {
			if _, ok := taskStates[record.State]; !ok {
				continue
			}
			resultMap[uint32(record.InstanceID)], err = s.getTask(ctx,
				id.GetValue(), uint32(record.InstanceID))
			if err != nil {
				log.WithError(err).
					WithField("job_id", jobID).
					WithField("instance_id", record.InstanceID).
					WithField("value", value).
					Error("Failed to get taskInfo from task")
				s.metrics.TaskMetrics.TaskGetForJobAndStatesFail.Inc(1)
				return nil, err
			}
			s.metrics.TaskMetrics.TaskGetForJobAndStates.Inc(1)
		}
	}
	s.metrics.TaskMetrics.TaskGetForJobAndStates.Inc(1)
	return resultMap, nil
}

func specContains(specifier []string, item string) bool {
	if len(specifier) == 0 {
		return true
	}
	return util.Contains(specifier, item)
}

// GetTasksByQuerySpec returns the tasks for a peloton job which satisfy the QuerySpec
// field 'state' is filtered by DB query,  field 'name', 'host' is filter
func (s *Store) GetTasksByQuerySpec(
	ctx context.Context,
	jobID *peloton.JobID,
	spec *task.QuerySpec) (map[uint32]*task.TaskInfo, error) {

	taskStates := spec.GetTaskStates()
	names := spec.GetNames()
	hosts := spec.GetHosts()

	var tasks map[uint32]*task.TaskInfo
	var err error

	if len(taskStates) == 0 {
		//Get all tasks for the job if query doesn't specify the task state(s)
		tasks, err = s.GetTasksForJobByRange(ctx, jobID, nil)

	} else {
		//Get tasks with specified states
		tasks, err = s.GetTasksForJobAndStates(ctx, jobID, taskStates)
	}

	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			WithField("states", taskStates).
			Error("QueryTasks failed to get tasks for the job")
		s.metrics.TaskMetrics.TaskQueryTasksFail.Inc(1)
		return nil, err
	}
	filteredTasks := make(map[uint32]*task.TaskInfo)
	// Filtering name and host
	start := time.Now()
	for _, task := range tasks {
		taskName := task.GetConfig().GetName()
		taskHost := task.GetRuntime().GetHost()

		if specContains(names, taskName) && specContains(hosts, taskHost) {
			filteredTasks[task.InstanceId] = task
		}
		// Deleting a task, to let it GC and not block memory till entire task list if iterated.
		delete(tasks, task.InstanceId)
	}
	log.WithFields(log.Fields{
		"jobID":      jobID,
		"query_type": "In memory filtering",
		"Names":      names,
		"hosts":      hosts,
		"task_size":  len(tasks),
		"duration":   time.Since(start).Seconds(),
	}).Debug("Query in memory filtering time")
	return filteredTasks, nil
}

// GetTaskRuntimesForJobByRange returns the Task RuntimeInfo for batch jobs by
// instance ID range.
func (s *Store) GetTaskRuntimesForJobByRange(ctx context.Context,
	id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.RuntimeInfo, error) {
	jobID := id.GetValue()
	result := make(map[uint32]*task.RuntimeInfo)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").
		From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID})
	if instanceRange != nil {
		stmt = stmt.Where("instance_id >= ?", instanceRange.From).
			Where("instance_id < ?", instanceRange.To)
	}

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("range", instanceRange).
			Error("fail to get task rutimes for jobs by range")
		s.metrics.TaskMetrics.TaskGetRuntimesForJobRangeFail.Inc(1)
		return nil, err
	}
	if len(allResults) == 0 {
		return result, nil
	}

	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("job_id", jobID).
				WithField("range", instanceRange).
				WithError(err).
				Error("failed to fill runtime into task record")
			s.metrics.TaskMetrics.TaskGetRuntimesForJobRangeFail.Inc(1)
			return nil, err
		}
		runtime, err := record.GetTaskRuntime()
		if err != nil {
			return result, err
		}
		result[uint32(record.InstanceID)] = runtime
	}

	s.metrics.TaskMetrics.TaskGetRuntimesForJobRange.Inc(1)
	return result, nil
}

// GetTasksForJobByRange returns the TaskInfo for batch jobs by
// instance ID range.
func (s *Store) GetTasksForJobByRange(ctx context.Context,
	id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	jobID := id.GetValue()
	result := make(map[uint32]*task.TaskInfo)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").
		From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID})
	if instanceRange != nil {
		stmt = stmt.Where("instance_id >= ?", instanceRange.From).
			Where("instance_id < ?", instanceRange.To)
	}

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("range", instanceRange).
			Error("Fail to GetTasksForBatchJobsByRange")
		s.metrics.TaskMetrics.TaskGetForJobRangeFail.Inc(1)
		return nil, err
	}
	if len(allResults) == 0 {
		return result, nil
	}

	// create map of instanceID->runtime
	runtimeMap := make(map[uint32]*task.RuntimeInfo)
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("job_id", jobID).
				WithField("range", instanceRange).
				WithError(err).
				Error("Failed to Fill into TaskRecord")
			s.metrics.TaskMetrics.TaskGetForJobRangeFail.Inc(1)
			return nil, err
		}
		runtime, err := record.GetTaskRuntime()
		if err != nil {
			return result, err
		}
		runtimeMap[uint32(record.InstanceID)] = runtime
	}

	if len(runtimeMap) == 0 {
		return result, nil
	}

	// map of configVersion-> list of instance IDS with that version
	//
	// NB: For batch jobs the assumption is that most(
	// if not all) of the tasks will have the same task config version.
	// So we can use this optimization to get all the configs with just 1 DB
	// call. In the worst case if all tasks have a different config version
	// then it'll take 1 DB call for each task config.
	configVersions := make(map[uint64][]uint32)
	for instanceID, runtime := range runtimeMap {
		instances, ok := configVersions[runtime.GetConfigVersion()]
		if !ok {
			instances = []uint32{}
		}
		instances = append(instances, instanceID)
		configVersions[runtime.GetConfigVersion()] = instances
	}

	log.WithField("config_versions_map",
		configVersions).Debug("config versions to read")

	// map of instanceID -> task config
	configMap := make(map[uint32]*task.TaskConfig)

	for configVersion, instances := range configVersions {
		// Get the configs for a particular config version
		configs, _, err := s.GetTaskConfigs(ctx, id, instances,
			configVersion)
		if err != nil {
			return result, err
		}

		// appends the configs
		for instanceID, config := range configs {
			configMap[instanceID] = config
		}
	}

	// We have the task configs and the task runtimes, so we can
	// create task infos
	for instanceID, runtime := range runtimeMap {
		config := configMap[instanceID]
		result[instanceID] = &task.TaskInfo{
			InstanceId: instanceID,
			JobId:      id,
			Config:     config,
			Runtime:    runtime,
		}
	}

	// The count should be the same
	log.WithField("count_runtime", len(runtimeMap)).
		WithField("count_config", len(configMap)).
		Debug("runtime vs config")
	s.metrics.TaskMetrics.TaskGetForJobRange.Inc(1)
	return result, nil
}

// GetTaskRuntime for a job and instance id.
func (s *Store) GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error) {
	record, err := s.getTaskRuntimeRecord(ctx, jobID.GetValue(), instanceID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			WithField("instance_id", instanceID).
			Errorf("failed to get task runtime record")
		return nil, err
	}

	runtime, err := record.GetTaskRuntime()
	if err != nil {
		log.WithError(err).
			WithField("record", record).
			Errorf("failed to parse task runtime from record")
		return nil, err
	}

	return runtime, err
}

// UpdateTaskRuntime updates a task for a peloton job
func (s *Store) UpdateTaskRuntime(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *task.RuntimeInfo,
	jobType job.JobType) error {
	runtimeBuffer, err := proto.Marshal(runtime)
	if err != nil {
		s.metrics.TaskMetrics.TaskUpdateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(taskRuntimeTable).
		Set("version", runtime.Revision.Version).
		Set("update_time", time.Now().UTC()).
		Set("state", runtime.GetState().String()).
		Set("runtime_info", runtimeBuffer).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})

	if err := s.applyStatement(ctx, stmt, fmt.Sprintf(taskIDFmt, jobID.GetValue(), instanceID)); err != nil {
		s.metrics.TaskMetrics.TaskUpdateFail.Inc(1)
		return err
	}

	s.metrics.TaskMetrics.TaskUpdate.Inc(1)
	s.addPodEvent(ctx, jobID, instanceID, runtime)

	return nil
}

// GetTaskForJob returns a task by jobID and instanceID
func (s *Store) GetTaskForJob(ctx context.Context, jobID string, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	taskID := fmt.Sprintf(taskIDFmt, jobID, int(instanceID))
	taskInfo, err := s.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]*task.TaskInfo)
	result[instanceID] = taskInfo
	return result, nil
}

// DeleteTaskRuntime deletes runtime of a particular task .
// It is used to delete a task when update workflow reduces the instance
// count during an update. The pod events are retained in case the user
// wants to fetch the events or the logs from a previous run of a deleted task.
// The task configurations from previous versions are retained in case
// auto-rollback gets triggered.
func (s *Store) DeleteTaskRuntime(
	ctx context.Context,
	id *peloton.JobID,
	instanceID uint32) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(taskRuntimeTable).
		Where(qb.Eq{"job_id": id.GetValue(), "instance_id": instanceID})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.TaskMetrics.TaskDeleteFail.Inc(1)
		return err
	}

	s.metrics.TaskMetrics.TaskDelete.Inc(1)
	return nil
}

// 1) Pod Events table has partition key job_id + instance_id,
// so pod events need to be deleted per instance.
// 2) Fetch instance count from job config, and delete pod events
// incrementally for each Instance.
// 3) There maybe a scenario, were instance count is shrunk, in order to delete
// pod events for shrunk instances, first read pod event for shrunk instances,
// if exist then delete. If result is zero, that means we have reached
// maximum instance count ever for that job.
// 4) Performance optimization for deleting shrunk instances,
// read pod events for every - instance_id % 100 = 0
// If pod event exist then continue to delete pod events for next 100 instances
// If pod event not exist means pod events are deleted for all shrunk instances
func (s *Store) deletePodEventsOnDeleteJob(
	ctx context.Context,
	jobID string) error {
	queryBuilder := s.DataStore.NewQuery()
	instanceCount := uint32(0)
	jobConfig, _, err := s.jobConfigOps.GetCurrentVersion(
		ctx,
		&peloton.JobID{Value: jobID},
	)
	if err != nil {
		// if the config is not found, then the job has already been deleted.
		if yarpcerrors.IsNotFound(errors.Cause(err)) {
			return nil
		}
		return err
	}

	for {
		// 1) read pod events to identify shrunk instances
		// 2) read pod events if instance_id (shrunk instances) % 100 = 0
		if instanceCount > jobConfig.InstanceCount &&
			instanceCount%_defaultPodEventsLimit == 0 {
			events, err := s.GetPodEvents(
				ctx,
				jobID,
				instanceCount)
			if err != nil {
				s.metrics.JobMetrics.JobDeleteFail.Inc(1)
				return err
			}
			if len(events) == 0 {
				break
			}
		}
		stmt := queryBuilder.Delete(podEventsTable).
			Where(qb.Eq{"job_id": jobID}).
			Where(qb.Eq{"instance_id": instanceCount})

		if err := s.applyStatement(ctx, stmt, jobID); err != nil {
			s.metrics.JobMetrics.JobDeleteFail.Inc(1)
			return err
		}
		instanceCount++
	}
	return nil
}

// DeleteJob deletes a job and associated tasks, by job id.
// TODO: This implementation is not perfect, as if it's getting an transient
// error, the job or some tasks may not be fully deleted.
func (s *Store) DeleteJob(
	ctx context.Context,
	jobID string) error {
	if err := s.deletePodEventsOnDeleteJob(ctx, jobID); err != nil {
		return err
	}

	if err := s.deleteTaskConfigV2OnDeleteJob(ctx, jobID); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(taskRuntimeTable).Where(qb.Eq{"job_id": jobID})
	if err := s.applyStatement(ctx, stmt, jobID); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	// Delete all updates for the job
	updateIDs, err := s.GetUpdatesForJob(ctx, jobID)
	if err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	for _, id := range updateIDs {
		if err := s.deleteSingleUpdate(ctx, id); err != nil {
			return err
		}
	}

	stmt = queryBuilder.Delete(jobConfigTable).Where(qb.Eq{"job_id": jobID})
	if err := s.applyStatement(ctx, stmt, jobID); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	stmt = queryBuilder.Delete(jobRuntimeTable).Where(qb.Eq{"job_id": jobID})
	err = s.applyStatement(ctx, stmt, jobID)
	if err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
	} else {
		s.metrics.JobMetrics.JobDelete.Inc(1)
	}
	return err
}

// task_config_v2 has partition key of jobID, version, instance_id
// so we need to delete this table per job, per version, per instance
func (s *Store) deleteTaskConfigV2OnDeleteJob(
	ctx context.Context,
	jobID string) error {
	queryBuilder := s.DataStore.NewQuery()
	jobConfig, _, err := s.jobConfigOps.GetCurrentVersion(ctx,
		&peloton.JobID{Value: jobID})
	if err != nil {
		// if the config is not found, then the job has already been deleted.
		if yarpcerrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	// loop through all the job config versions
	for i := uint64(1); i <= jobConfig.GetChangeLog().GetVersion(); i++ {
		// get the job config for this version
		jobConfigWithVersoin, _, err := s.jobConfigOps.Get(ctx,
			&peloton.JobID{Value: jobID}, i)
		if err != nil {
			return err
		}
		// get the instance count for this version
		instanceCountWithVersion := uint32(jobConfigWithVersoin.
			GetInstanceCount())
		for j := uint32(0); j < instanceCountWithVersion; j++ {
			stmt := queryBuilder.Delete(taskConfigV2Table).
				Where(qb.Eq{"job_id": jobID}).
				Where(qb.Eq{"instance_id": j}).
				Where(qb.Eq{"version": i})

			if err := s.applyStatement(ctx, stmt, jobID); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error) {
	jobID, instanceID, err := util.ParseTaskID(taskID)
	if err != nil {
		log.WithError(err).
			WithField("task_id", taskID).
			Error("Invalid task id")
		return nil, err
	}
	return s.getTask(ctx, jobID, uint32(instanceID))
}

func (s *Store) getTask(ctx context.Context, jobID string, instanceID uint32) (*task.TaskInfo, error) {
	record, err := s.getTaskRuntimeRecord(ctx, jobID, instanceID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("instance_id", instanceID).
			Error("failed to fetch task runtime record in get task")
		return nil, err
	}

	return s.getTaskInfoFromRuntimeRecord(ctx, &peloton.JobID{Value: jobID}, record)
}

// getTaskRuntimeRecord returns the runtime record for a peloton task
func (s *Store) getTaskRuntimeRecord(ctx context.Context, jobID string, instanceID uint32) (*TaskRuntimeRecord, error) {
	taskID := fmt.Sprintf(taskIDFmt, jobID, int(instanceID))
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID, "instance_id": instanceID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithField("task_id", taskID).
			WithError(err).
			Error("Fail to GetTask")
		s.metrics.TaskMetrics.TaskGetFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("task_id", taskID).
				WithError(err).
				Error("Failed to Fill into TaskRecord")
			s.metrics.TaskMetrics.TaskGetFail.Inc(1)
			return nil, err
		}

		s.metrics.TaskMetrics.TaskGet.Inc(1)
		return &record, nil
	}
	s.metrics.TaskMetrics.TaskNotFound.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf("task:%s not found", taskID)
}

//SetMesosStreamID stores the mesos framework id for a framework name
func (s *Store) SetMesosStreamID(ctx context.Context, frameworkName string, mesosStreamID string) error {
	return s.updateFrameworkTable(ctx, map[string]interface{}{"framework_name": frameworkName, "mesos_stream_id": mesosStreamID})
}

//SetMesosFrameworkID stores the mesos framework id for a framework name
func (s *Store) SetMesosFrameworkID(ctx context.Context, frameworkName string, frameworkID string) error {
	return s.updateFrameworkTable(ctx, map[string]interface{}{"framework_name": frameworkName, "framework_id": frameworkID})
}

func (s *Store) updateFrameworkTable(ctx context.Context, content map[string]interface{}) error {
	hostName, err := os.Hostname()
	if err != nil {
		return err
	}
	queryBuilder := s.DataStore.NewQuery()
	content["update_host"] = hostName
	content["update_time"] = time.Now().UTC()

	var columns []string
	var values []interface{}
	for col, val := range content {
		columns = append(columns, col)
		values = append(values, val)
	}
	stmt := queryBuilder.Insert(frameworksTable).
		Columns(columns...).
		Values(values...)
	err = s.applyStatement(ctx, stmt, frameworksTable)
	if err != nil {
		s.metrics.FrameworkStoreMetrics.FrameworkUpdateFail.Inc(1)
		return err
	}
	s.metrics.FrameworkStoreMetrics.FrameworkUpdate.Inc(1)
	return nil
}

//GetMesosStreamID reads the mesos stream id for a framework name
func (s *Store) GetMesosStreamID(ctx context.Context, frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(ctx, frameworkName)
	if err != nil {
		s.metrics.FrameworkStoreMetrics.StreamIDGetFail.Inc(1)
		return "", err
	}

	s.metrics.FrameworkStoreMetrics.StreamIDGet.Inc(1)
	return frameworkInfoRecord.MesosStreamID, nil
}

//GetFrameworkID reads the framework id for a framework name
func (s *Store) GetFrameworkID(ctx context.Context, frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(ctx, frameworkName)
	if err != nil {
		s.metrics.FrameworkStoreMetrics.FrameworkIDGetFail.Inc(1)
		return "", err
	}

	s.metrics.FrameworkStoreMetrics.FrameworkIDGet.Inc(1)
	return frameworkInfoRecord.FrameworkID, nil
}

func (s *Store) getFrameworkInfo(ctx context.Context, frameworkName string) (*FrameworkInfoRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(frameworksTable).
		Where(qb.Eq{"framework_name": frameworkName})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to getFrameworkInfo by frameworkName %v, err=%v", frameworkName, err)
		return nil, err
	}

	for _, value := range allResults {
		var record FrameworkInfoRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into FrameworkInfoRecord, err= %v", err)
			return nil, err
		}
		return &record, nil
	}
	return nil, fmt.Errorf("FrameworkInfo not found for framework %v", frameworkName)
}

func (s *Store) applyStatement(ctx context.Context, stmt api.Statement, itemName string) error {
	stmtString, _, _ := stmt.ToSQL()
	// Use common.DBStmtLogField to log CQL queries here. Log formatter will use
	// this string to redact secret_info table queries
	log.WithField(common.DBStmtLogField, stmtString).Debug("DB stmt string")
	result, err := s.executeWrite(ctx, stmt)
	if err != nil {
		log.WithError(err).WithFields(
			log.Fields{common.DBStmtLogField: stmtString, "itemName": itemName}).
			Debug("Fail to execute stmt")
		return err
	}
	if result != nil {
		defer result.Close()
	}
	// In case the insert stmt has IfNotExist set (create case), it would fail to apply if
	// the underlying job/task already exists
	if result != nil && !result.Applied() {
		errMsg := fmt.Sprintf("%v is not applied, item could exist already", itemName)
		s.metrics.ErrorMetrics.CASNotApplied.Inc(1)
		log.Error(errMsg)
		return yarpcerrors.AlreadyExistsErrorf(errMsg)
	}
	return nil
}

// getJobSummaryFromIndex gets the job summary from job index table.
// This is a helper function used by QueryJobs(). Do not use it for
// anything other than QueryJobs; consider using ORM directly.
// TODO Remove this when QueryJobs() uses ORM.
func (s *Store) getJobSummaryFromIndex(
	ctx context.Context, id *peloton.JobID) (*job.JobSummary, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select(
		"job_id",
		"name",
		"owner",
		"job_type",
		"respool_id",
		"instance_count",
		"labels",
		"runtime_info").
		From(jobIndexTable).
		Where(qb.Eq{"job_id": id.GetValue()})

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		return nil, err
	}
	summary, err := s.getJobSummaryFromResultMap(ctx, allResults)
	if err != nil {
		return nil, err
	}
	if len(summary) != 1 {
		return nil, yarpcerrors.FailedPreconditionErrorf(
			"found %d jobs %v for job id %v", len(allResults), allResults, id)
	}
	return summary[0], nil
}

// Less function holds the task sorting logic
func Less(orderByList []*query.OrderBy, t1 *task.TaskInfo, t2 *task.TaskInfo) bool {
	// Keep comparing the two tasks by the field related with Order from the OrderbyList
	// until they are not equal on one fields
	for _, orderBy := range orderByList {
		desc := orderBy.GetOrder() == query.OrderBy_DESC
		property := orderBy.GetProperty().GetValue()
		if property == creationTimeField {
			time1, err1 := time.Parse(time.RFC3339, t1.GetRuntime().GetStartTime())
			time2, err2 := time.Parse(time.RFC3339, t2.GetRuntime().GetStartTime())
			if err1 != nil || err2 != nil {
				// if any StartTime of two tasks can't get parsed (or not exist)
				// task with a valid StartTime is less
				if err1 == nil {
					return !desc
				} else if err2 == nil {
					return desc
				}

				// both tasks have invalid StartTime, goto next loop
				continue
			}
			// return result if not equal, otherwise goto next loop
			if time1.Before(time2) {
				return !desc
			} else if time1.After(time2) {
				return desc
			}
		} else if property == hostField {
			if t1.GetRuntime().GetHost() < t2.GetRuntime().GetHost() {
				return !desc
			} else if t1.GetRuntime().GetHost() > t2.GetRuntime().GetHost() {
				return desc
			}
		} else if property == instanceIDField {
			if t1.GetInstanceId() < t2.GetInstanceId() {
				return !desc
			} else if t1.GetInstanceId() > t2.GetInstanceId() {
				return desc
			}
		} else if property == messageField {
			if t1.GetRuntime().GetMessage() < t2.GetRuntime().GetMessage() {
				return !desc
			} else if t1.GetRuntime().GetMessage() > t2.GetRuntime().GetMessage() {
				return desc
			}
		} else if property == nameField {
			if t1.GetConfig().GetName() < t2.GetConfig().GetName() {
				return !desc
			} else if t1.GetConfig().GetName() > t2.GetConfig().GetName() {
				return desc
			}
		} else if property == reasonField {
			if t1.GetRuntime().GetReason() < t2.GetRuntime().GetReason() {
				return !desc
			} else if t1.GetRuntime().GetReason() > t2.GetRuntime().GetReason() {
				return desc
			}
		} else if property == stateField {
			if t1.GetRuntime().GetState() < t2.GetRuntime().GetState() {
				return !desc
			} else if t1.GetRuntime().GetState() > t2.GetRuntime().GetState() {
				return desc
			}
		}
	}
	// Default order by InstanceId with increase order
	return t1.GetInstanceId() < t2.GetInstanceId()
}

// QueryTasks returns the tasks filtered on states(spec.TaskStates) in the given offset..offset+limit range.
func (s *Store) QueryTasks(
	ctx context.Context,
	jobID *peloton.JobID,
	spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error) {

	tasks, err := s.GetTasksByQuerySpec(ctx, jobID, spec)
	if err != nil {
		s.metrics.TaskMetrics.TaskQueryTasksFail.Inc(1)
		return nil, 0, err
	}

	//sortedTasksResult is sorted (by instanceID) list of tasksResult
	var sortedTasksResult SortedTaskInfoList
	for _, taskInfo := range tasks {
		sortedTasksResult = append(sortedTasksResult, taskInfo)
	}
	//sorting fields validation check
	var orderByList = spec.GetPagination().GetOrderBy()
	for _, orderBy := range orderByList {
		property := orderBy.GetProperty().GetValue()
		switch property {
		case
			creationTimeField,
			hostField,
			instanceIDField,
			messageField,
			nameField,
			reasonField,
			stateField:
			continue
		}
		return nil, 0, errors.New("Sort only supports fields: creation_time, host, instanceId, message, name, reason, state")
	}

	sort.Slice(sortedTasksResult, func(i, j int) bool {
		return Less(orderByList, sortedTasksResult[i], sortedTasksResult[j])
	})

	offset := spec.GetPagination().GetOffset()
	limit := _defaultQueryLimit
	if spec.GetPagination().GetLimit() != 0 {
		limit = spec.GetPagination().GetLimit()
	}

	end := offset + limit
	if end > uint32(len(sortedTasksResult)) {
		end = uint32(len(sortedTasksResult))
	}

	var result []*task.TaskInfo
	if offset < end {
		result = sortedTasksResult[offset:end]
	}

	s.metrics.TaskMetrics.TaskQueryTasks.Inc(1)
	return result, uint32(len(sortedTasksResult)), nil
}

// CreatePersistentVolume creates a persistent volume entry.
func (s *Store) CreatePersistentVolume(ctx context.Context, volume *pb_volume.PersistentVolumeInfo) error {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(volumeTable).
		Columns("volume_id", "state", "goal_state", "job_id", "instance_id", "hostname", "size_mb", "container_path", "creation_time", "update_time").
		Values(
			volume.GetId().GetValue(),
			volume.State.String(),
			volume.GoalState.String(),
			volume.GetJobId().GetValue(),
			volume.InstanceId,
			volume.Hostname,
			volume.SizeMB,
			volume.ContainerPath,
			time.Now().UTC(),
			time.Now().UTC()).
		IfNotExist()

	err := s.applyStatement(ctx, stmt, volume.GetId().GetValue())
	if err != nil {
		s.metrics.VolumeMetrics.VolumeCreateFail.Inc(1)
		return err
	}

	s.metrics.VolumeMetrics.VolumeCreate.Inc(1)
	return nil
}

// UpdatePersistentVolume updates persistent volume info.
func (s *Store) UpdatePersistentVolume(ctx context.Context, volumeInfo *pb_volume.PersistentVolumeInfo) error {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Update(volumeTable).
		Set("state", volumeInfo.GetState().String()).
		Set("goal_state", volumeInfo.GetGoalState().String()).
		Set("update_time", time.Now().UTC()).
		Where(qb.Eq{"volume_id": volumeInfo.GetId().GetValue()})

	err := s.applyStatement(ctx, stmt, volumeInfo.GetId().GetValue())
	if err != nil {
		s.metrics.VolumeMetrics.VolumeUpdateFail.Inc(1)
		return err
	}

	s.metrics.VolumeMetrics.VolumeUpdate.Inc(1)
	return nil
}

// GetPersistentVolume gets the persistent volume object.
func (s *Store) GetPersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) (*pb_volume.PersistentVolumeInfo, error) {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Select("*").
		From(volumeTable).
		Where(qb.Eq{"volume_id": volumeID.GetValue()})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("volume_id", volumeID).
			Error("Fail to GetPersistentVolume by volumeID.")
		s.metrics.VolumeMetrics.VolumeGetFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record PersistentVolumeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("raw_volume_value", value).
				Error("Failed to Fill into PersistentVolumeRecord.")
			s.metrics.VolumeMetrics.VolumeGetFail.Inc(1)
			return nil, err
		}
		s.metrics.VolumeMetrics.VolumeGet.Inc(1)
		return &pb_volume.PersistentVolumeInfo{
			Id: &peloton.VolumeID{
				Value: record.VolumeID,
			},
			State: pb_volume.VolumeState(
				pb_volume.VolumeState_value[record.State]),
			GoalState: pb_volume.VolumeState(
				pb_volume.VolumeState_value[record.GoalState]),
			JobId: &peloton.JobID{
				Value: record.JobID,
			},
			InstanceId:    uint32(record.InstanceID),
			Hostname:      record.Hostname,
			SizeMB:        uint32(record.SizeMB),
			ContainerPath: record.ContainerPath,
			CreateTime:    record.CreateTime.String(),
			UpdateTime:    record.UpdateTime.String(),
		}, nil
	}
	s.metrics.VolumeMetrics.VolumeGetFail.Inc(1)
	return nil, &storage.VolumeNotFoundError{VolumeID: volumeID}
}

// CreateUpdate creates a new update entry in DB.
// If it already exists, the create will return an error.
func (s *Store) CreateUpdate(
	ctx context.Context,
	updateInfo *models.UpdateModel,
) error {
	creationTime, err := time.Parse(time.RFC3339Nano, updateInfo.GetCreationTime())
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()), "fail to parse creationTime")
	}

	updateTime, err := time.Parse(time.RFC3339Nano, updateInfo.GetUpdateTime())
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()), "fail to parse updateTime")
	}

	updateConfigBuffer, err := proto.Marshal(updateInfo.GetUpdateConfig())
	if err != nil {
		log.WithError(err).
			WithField("update_id", updateInfo.GetUpdateID().GetValue()).
			WithField("job_id", updateInfo.GetJobID().GetValue()).
			Error("failed to marshal update config")
		s.metrics.UpdateMetrics.UpdateCreateFail.Inc(1)
		return err
	}

	// Insert the update into the DB. Use CAS to ensure
	// that it does not exist already.
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(updatesTable).
		Columns(
			"update_id",
			"update_type",
			"update_options",
			"update_state",
			"update_prev_state",
			"instances_total",
			"instances_added",
			"instances_updated",
			"instances_removed",
			"instances_done",
			"instances_current",
			"instances_failed",
			"job_id",
			"job_config_version",
			"job_config_prev_version",
			"opaque_data",
			"creation_time",
			"update_time").
		Values(
			updateInfo.GetUpdateID().GetValue(),
			updateInfo.GetType().String(),
			updateConfigBuffer,
			updateInfo.GetState().String(),
			updateInfo.GetPrevState().String(),
			updateInfo.GetInstancesTotal(),
			updateInfo.GetInstancesAdded(),
			updateInfo.GetInstancesUpdated(),
			updateInfo.GetInstancesRemoved(),
			0,
			[]int{},
			0,
			updateInfo.GetJobID().GetValue(),
			updateInfo.GetJobConfigVersion(),
			updateInfo.GetPrevJobConfigVersion(),
			updateInfo.GetOpaqueData().GetData(),
			creationTime.UTC(),
			updateTime.UTC()).
		IfNotExist()

	if err := s.applyStatement(
		ctx,
		stmt,
		updateInfo.GetUpdateID().GetValue()); err != nil {
		log.WithError(err).
			WithField("update_id", updateInfo.GetUpdateID().GetValue()).
			WithField("job_id", updateInfo.GetJobID().GetValue()).
			Info("create update in DB failed")
		s.metrics.UpdateMetrics.UpdateCreateFail.Inc(1)
		return err
	}

	// best effort to clean up previous updates for the job
	go func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			ctx,
			_jobUpdatesCleanupTimeout,
		)
		defer cleanupCancel()

		if err := s.cleanupPreviousUpdatesForJob(
			cleanupCtx,
			updateInfo.GetJobID()); err != nil {
			log.WithError(err).
				WithField("job_id", updateInfo.GetJobID().GetValue()).
				Info("failed to clean up previous updates")
		}
	}()

	s.metrics.UpdateMetrics.UpdateCreate.Inc(1)
	return nil
}

// convertWorkflowEvents is a helper method to return workflow events slice
// from Cassandra read result of workflow events.
func (s *Store) convertToWorkflowEvents(
	ctx context.Context,
	updateID *peloton.UpdateID,
	result []map[string]interface{},
) []*stateless.WorkflowEvent {

	var count int
	var isLogged bool
	var prevWorkflowState stateless.WorkflowState

	var workflowEvents []*stateless.WorkflowEvent
	for _, value := range result {
		workflowEvent := &stateless.WorkflowEvent{
			Type: stateless.WorkflowType(
				models.WorkflowType_value[value["type"].(string)]),
			State: stateless.WorkflowState(
				update.State_value[value["state"].(string)]),
			Timestamp: value["create_time"].(qb.UUID).Time().Format(time.RFC3339),
		}

		if prevWorkflowState != workflowEvent.GetState() {
			workflowEvents = append(workflowEvents, workflowEvent)
			count = 0
			isLogged = false
			prevWorkflowState = workflowEvent.GetState()
		} else {
			count++
			if count > _defaultWorkflowEventsDedupeWarnLimit && !isLogged {
				log.WithFields(log.Fields{
					"workflow_state": workflowEvent.GetState().String(),
					"workflow_type":  workflowEvent.GetType().String(),
					"update_id":      updateID.GetValue(),
				}).Warn("too many job workflow events in the same state")
				isLogged = true
			}
		}
	}

	return workflowEvents
}

// deleteJobUpdateEvents deletes job update events for an update of a job
func (s *Store) deleteJobUpdateEvents(
	ctx context.Context,
	updateID *peloton.UpdateID,
) error {
	if err := s.jobUpdateEventsOps.Delete(ctx, updateID); err != nil {
		s.metrics.UpdateMetrics.JobUpdateEventDeleteFail.Inc(1)
		return err
	}

	s.metrics.UpdateMetrics.JobUpdateEventDelete.Inc(1)
	return nil
}

// AddWorkflowEvent adds workflow events for an update and instance
// to track the progress
func (s *Store) AddWorkflowEvent(
	ctx context.Context,
	updateID *peloton.UpdateID,
	instanceID uint32,
	workflowType models.WorkflowType,
	workflowState update.State) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(podWorkflowEventsTable).
		Columns(
			"update_id",
			"instance_id",
			"type",
			"state",
			"create_time",
			"update_timestamp").
		Values(
			updateID.GetValue(),
			int(instanceID),
			workflowType.String(),
			workflowState.String(),
			qb.UUID{UUID: gocql.UUIDFromTime(time.Now())},
			time.Now())
	err := s.applyStatement(ctx, stmt, updateID.GetValue())
	if err != nil {
		s.metrics.WorkflowMetrics.WorkflowEventsAddFail.Inc(1)
		return err
	}

	s.metrics.WorkflowMetrics.WorkflowEventsAdd.Inc(1)
	return nil
}

// GetWorkflowEvents gets workflow events for an update and instance,
// events are sorted in descending create timestamp
func (s *Store) GetWorkflowEvents(
	ctx context.Context,
	updateID *peloton.UpdateID,
	instanceID uint32,
	limit uint32,
) ([]*stateless.WorkflowEvent, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(podWorkflowEventsTable).
		Where(qb.Eq{"update_id": updateID.GetValue()}).
		Where(qb.Eq{"instance_id": int(instanceID)})

	if limit > 0 {
		stmt = stmt.Limit(uint64(limit))
	}

	result, err := s.executeRead(ctx, stmt)
	if err != nil {
		s.metrics.WorkflowMetrics.WorkflowEventsGetFail.Inc(1)
		return nil, err
	}

	workflowEvents := s.convertToWorkflowEvents(ctx, updateID, result)

	s.metrics.WorkflowMetrics.WorkflowEventsGet.Inc(1)
	return workflowEvents, nil
}

// deleteWorkflowEvents deletes the workflow events for an update and instance
func (s *Store) deleteWorkflowEvents(
	ctx context.Context,
	id *peloton.UpdateID,
	instanceID uint32) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(podWorkflowEventsTable).
		Where(qb.Eq{"update_id": id.GetValue()}).
		Where(qb.Eq{"instance_id": int(instanceID)})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.WorkflowMetrics.WorkflowEventsDeleteFail.Inc(1)
		return err
	}

	s.metrics.WorkflowMetrics.WorkflowEventsDelete.Inc(1)
	return nil
}

// TODO determine if this function should be part of storage or api handler.
// cleanupPreviousUpdatesForJob cleans up the old job configurations
// and updates. This is called when a new update is created, and ensures
// that the number of configurations and updates in the DB do not keep
// increasing continuously.
func (s *Store) cleanupPreviousUpdatesForJob(
	ctx context.Context,
	jobID *peloton.JobID) error {
	var updateList []*SortUpdateInfo
	var nonUpdateList []*SortUpdateInfo

	// first fetch the updates for the job
	updates, err := s.GetUpdatesForJob(ctx, jobID.GetValue())
	if err != nil {
		return err
	}

	for _, updateID := range updates {
		var allResults []map[string]interface{}

		// get the job configuration version
		queryBuilder := s.DataStore.NewQuery()
		stmt := queryBuilder.Select("job_config_version").From(updatesTable).
			Where(qb.Eq{"update_id": updateID.GetValue()})

		allResults, err = s.executeRead(ctx, stmt)
		if err != nil {
			log.WithError(err).
				WithField("update_id", updateID.GetValue()).
				Info("failed to get job config version")
			continue
		}

		for _, value := range allResults {
			var record UpdateRecord
			if err := FillObject(value, &record,
				reflect.TypeOf(record)); err != nil {
				log.WithError(err).
					WithField("update_id", updateID.GetValue()).
					Info("failed to fill the update record")
				continue
			}

			// sort as per the job configuration version
			updateInfo := &SortUpdateInfo{
				updateID:         updateID,
				jobConfigVersion: uint64(record.JobConfigVersion),
			}
			if record.Type == models.WorkflowType_UPDATE.String() {
				updateList = append(updateList, updateInfo)
			} else {
				nonUpdateList = append(nonUpdateList, updateInfo)
			}

		}
	}

	// updates and non-updates are handled separately. Each category would keep
	// up to Conf.MaxUpdatesPerJob
	if len(updateList) > s.Conf.MaxUpdatesPerJob {
		sort.Sort(sort.Reverse(SortedUpdateList(updateList)))
		for _, u := range updateList[s.Conf.MaxUpdatesPerJob:] {
			// delete the old job and task configurations, and then the update
			s.DeleteUpdate(ctx, u.updateID, jobID, u.jobConfigVersion)
		}
	}

	if len(nonUpdateList) > s.Conf.MaxUpdatesPerJob {
		sort.Sort(sort.Reverse(SortedUpdateList(nonUpdateList)))
		for _, u := range nonUpdateList[s.Conf.MaxUpdatesPerJob:] {
			// delete the old job and task configurations, and then the update
			s.DeleteUpdate(ctx, u.updateID, jobID, u.jobConfigVersion)
		}
	}
	return nil
}

// DeleteUpdate deletes the update from the update_info table and deletes all
// job and task configurations created for the update.
func (s *Store) DeleteUpdate(
	ctx context.Context,
	updateID *peloton.UpdateID,
	jobID *peloton.JobID,
	jobConfigVersion uint64) error {
	// first delete the task and job configurations created for this update
	if err := s.deleteJobConfigVersion(ctx, jobID, jobConfigVersion); err != nil {
		return err
	}

	// next clean up the update from the update_info table
	return s.deleteSingleUpdate(ctx, updateID)
}

// GetUpdate fetches the job update stored in the DB.
func (s *Store) GetUpdate(ctx context.Context, id *peloton.UpdateID) (
	*models.UpdateModel,
	error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(updatesTable).
		Where(qb.Eq{"update_id": id.GetValue()})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			Info("failed to get job update")
		s.metrics.UpdateMetrics.UpdateGetFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record UpdateRecord
		if err = FillObject(value, &record,
			reflect.TypeOf(record)); err != nil {
			s.metrics.UpdateMetrics.UpdateGetFail.Inc(1)
			return nil, err
		}

		updateConfig, err := record.GetUpdateConfig()
		if err != nil {
			s.metrics.UpdateMetrics.UpdateGetFail.Inc(1)
			return nil, err
		}

		updateInfo := &models.UpdateModel{
			UpdateID:             id,
			UpdateConfig:         updateConfig,
			JobID:                &peloton.JobID{Value: record.JobID.String()},
			JobConfigVersion:     uint64(record.JobConfigVersion),
			PrevJobConfigVersion: uint64(record.PrevJobConfigVersion),
			State:                update.State(update.State_value[record.State]),
			PrevState:            update.State(update.State_value[record.PrevState]),
			Type:                 models.WorkflowType(models.WorkflowType_value[record.Type]),
			InstancesTotal:       uint32(record.InstancesTotal),
			InstancesAdded:       record.GetInstancesAdded(),
			InstancesUpdated:     record.GetInstancesUpdated(),
			InstancesRemoved:     record.GetInstancesRemoved(),
			InstancesFailed:      uint32(record.InstancesFailed),
			InstancesDone:        uint32(record.InstancesDone),
			InstancesCurrent:     record.GetProcessingInstances(),
			CreationTime:         record.CreationTime.Format(time.RFC3339Nano),
			UpdateTime:           record.UpdateTime.Format(time.RFC3339Nano),
			OpaqueData:           &peloton.OpaqueData{Data: record.OpaqueData},
			CompletionTime:       record.CompletionTime,
		}

		s.metrics.UpdateMetrics.UpdateGet.Inc(1)
		return updateInfo, nil
	}

	s.metrics.UpdateMetrics.UpdateGetFail.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf("update not found")
}

// deleteSingleUpdate deletes a given update from following tables
// - pod_workflow_events table for all instances included in the update
// - job_update_events table for update state change events
// - update_info table
func (s *Store) deleteSingleUpdate(ctx context.Context, id *peloton.UpdateID) error {
	update, err := s.GetUpdate(ctx, id)
	if err != nil {
		s.metrics.UpdateMetrics.UpdateDeleteFail.Inc(1)
		log.WithFields(log.Fields{
			"update_id": id.GetValue(),
		}).WithError(err).Info("failed to get update for deleting workflow events")
		return err
	}

	instances := append(update.GetInstancesUpdated(),
		update.GetInstancesAdded()...)
	instances = append(instances, update.GetInstancesRemoved()...)
	for _, instance := range instances {
		if err := s.deleteWorkflowEvents(ctx, id, instance); err != nil {
			log.WithFields(log.Fields{
				"update_id":   id.GetValue(),
				"instance_id": instance,
			}).WithError(err).Info("failed to delete workflow events")
			s.metrics.UpdateMetrics.UpdateDeleteFail.Inc(1)
			return err
		}
	}

	if err := s.deleteJobUpdateEvents(ctx, id); err != nil {
		log.WithFields(log.Fields{
			"update_id": id.GetValue(),
		}).WithError(err).Info("failed to delete job update events")
		s.metrics.UpdateMetrics.UpdateDeleteFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(updatesTable).Where(qb.Eq{
		"update_id": id.GetValue()})

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			Info("failed to delete the update")
		s.metrics.UpdateMetrics.UpdateDeleteFail.Inc(1)
		return err
	}

	s.metrics.UpdateMetrics.UpdateDelete.Inc(1)
	return nil
}

// deleteJobConfigVersion deletes the job and task configurations for a given
// job identifier and a configuration version.
func (s *Store) deleteJobConfigVersion(
	ctx context.Context,
	jobID *peloton.JobID,
	version uint64) error {
	queryBuilder := s.DataStore.NewQuery()

	// next delete the job configuration
	stmt := queryBuilder.Delete(jobConfigTable).Where(qb.Eq{
		"job_id": jobID.GetValue(), "version": version})
	err := s.applyStatement(ctx, stmt, jobID.GetValue())
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			WithField("version", version).
			Info("failed to delete the job configuration")
	}
	return err
}

// WriteUpdateProgress writes the progress of the job update to the DB.
// The inputs to this function are the only mutable fields in update.
func (s *Store) WriteUpdateProgress(
	ctx context.Context,
	updateInfo *models.UpdateModel) error {
	updateTime, err := time.Parse(time.RFC3339Nano, updateInfo.GetUpdateTime())
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()), "fail to parse updateTime")
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(updatesTable).
		Set("update_time", updateTime.UTC())

	// updateInfo can either have updateTime set only, or
	// set State, PreState and other fields altogether.
	// For now, there is no better way to differentiate which fields
	// are set especially for slice fields.
	if updateInfo.GetState() != update.State_INVALID {
		stmt = stmt.Set("update_state", updateInfo.GetState().String()).
			Set("update_prev_state", updateInfo.GetPrevState().String()).
			Set("instances_done", updateInfo.GetInstancesDone()).
			Set("instances_failed", updateInfo.GetInstancesFailed()).
			Set("instances_current", updateInfo.GetInstancesCurrent())
	}

	if updateInfo.GetOpaqueData() != nil {
		stmt = stmt.Set("opaque_data", updateInfo.GetOpaqueData().GetData())
	}

	if len(updateInfo.GetCompletionTime()) != 0 {
		stmt = stmt.Set("completion_time", updateInfo.GetCompletionTime())
	}

	stmt = stmt.Where(qb.Eq{"update_id": updateInfo.GetUpdateID().GetValue()})

	if err := s.applyStatement(
		ctx,
		stmt,
		updateInfo.GetUpdateID().GetValue()); err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"update_id":               updateInfo.GetUpdateID().GetValue(),
				"update_state":            updateInfo.GetState().String(),
				"update_prev_state":       updateInfo.GetPrevState().String(),
				"update_instances_done":   updateInfo.GetInstancesDone(),
				"update_instances_failed": updateInfo.GetInstancesFailed(),
			}).Info("modify update in DB failed")
		s.metrics.UpdateMetrics.UpdateWriteProgressFail.Inc(1)
		return err
	}

	s.metrics.UpdateMetrics.UpdateWriteProgress.Inc(1)
	return nil
}

// ModifyUpdate modify the progress of an update,
// instances to update/remove/add and the job config version
func (s *Store) ModifyUpdate(
	ctx context.Context,
	updateInfo *models.UpdateModel) error {
	updateTime, err := time.Parse(time.RFC3339Nano, updateInfo.GetUpdateTime())
	if err != nil {
		return errors.Wrap(yarpcerrors.InvalidArgumentErrorf(err.Error()), "fail to parse updateTime")
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(updatesTable).
		Set("update_state", updateInfo.GetState().String()).
		Set("update_prev_state", updateInfo.GetPrevState().String()).
		Set("instances_done", updateInfo.GetInstancesDone()).
		Set("instances_failed", updateInfo.GetInstancesFailed()).
		Set("instances_current", updateInfo.GetInstancesCurrent()).
		Set("instances_added", updateInfo.GetInstancesAdded()).
		Set("instances_updated", updateInfo.GetInstancesUpdated()).
		Set("instances_removed", updateInfo.GetInstancesRemoved()).
		Set("instances_total", updateInfo.GetInstancesTotal()).
		Set("job_config_version", updateInfo.GetJobConfigVersion()).
		Set("job_config_prev_version", updateInfo.GetPrevJobConfigVersion()).
		Set("update_time", updateTime.UTC())

	if updateInfo.GetOpaqueData() != nil {
		stmt = stmt.Set("opaque_data", updateInfo.GetOpaqueData().GetData())
	}

	stmt = stmt.Where(qb.Eq{"update_id": updateInfo.GetUpdateID().GetValue()})
	if err := s.applyStatement(
		ctx,
		stmt,
		updateInfo.GetUpdateID().GetValue()); err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"update_id":               updateInfo.GetUpdateID().GetValue(),
				"update_state":            updateInfo.GetState().String(),
				"update_prev_state":       updateInfo.GetPrevState().String(),
				"update_instances_done":   updateInfo.GetInstancesDone(),
				"update_instances_failed": updateInfo.GetInstancesFailed(),
			}).Info("write update progress in DB failed")
		s.metrics.UpdateMetrics.UpdateWriteProgressFail.Inc(1)
		return err
	}

	s.metrics.UpdateMetrics.UpdateWriteProgress.Inc(1)
	return nil
}

// GetUpdateProgress fetches the job update progress, which includes the
// instances already updated, instances being updated and the current
// state of the update.
func (s *Store) GetUpdateProgress(ctx context.Context, id *peloton.UpdateID) (
	*models.UpdateModel,
	error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(updatesTable).
		Where(qb.Eq{"update_id": id.GetValue()})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			Info("failed to get job update")
		s.metrics.UpdateMetrics.UpdateGetProgessFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record UpdateRecord
		if err = FillObject(value, &record, reflect.TypeOf(record)); err != nil {
			s.metrics.UpdateMetrics.UpdateGetProgessFail.Inc(1)
			return nil, err
		}

		updateInfo := &models.UpdateModel{
			UpdateID:         id,
			State:            update.State(update.State_value[record.State]),
			PrevState:        update.State(update.State_value[record.PrevState]),
			InstancesTotal:   uint32(record.InstancesTotal),
			InstancesDone:    uint32(record.InstancesDone),
			InstancesFailed:  uint32(record.InstancesFailed),
			InstancesCurrent: record.GetProcessingInstances(),
			UpdateTime:       record.UpdateTime.Format(time.RFC3339Nano),
			CompletionTime:   record.CompletionTime,
		}

		s.metrics.UpdateMetrics.UpdateGetProgess.Inc(1)
		return updateInfo, nil
	}

	s.metrics.UpdateMetrics.UpdateGetProgessFail.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf("update not found")
}

// GetUpdatesForJob returns the list of job updates created for a given job.
func (s *Store) GetUpdatesForJob(
	ctx context.Context,
	jobID string,
) ([]*peloton.UpdateID, error) {
	var updateIDs []*peloton.UpdateID
	var updateList []*SortUpdateInfoTS

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("update_id", "job_id", "creation_time").
		From(updatesByJobView).
		Where(qb.Eq{"job_id": jobID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			Info("failed to fetch updates for a given job")
		s.metrics.UpdateMetrics.UpdateGetForJobFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record UpdateViewRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				Info("failed to fill update record for the job")
			s.metrics.UpdateMetrics.UpdateGetForJobFail.Inc(1)
			return nil, err
		}

		// sort as per the job configuration version
		updateInfo := &SortUpdateInfoTS{
			updateID:   &peloton.UpdateID{Value: record.UpdateID.String()},
			createTime: record.CreationTime,
		}

		updateList = append(updateList, updateInfo)
	}

	sort.Sort(sort.Reverse(SortedUpdateListTS(updateList)))

	for _, update := range updateList {
		updateIDs = append(updateIDs, update.updateID)
	}

	s.metrics.UpdateMetrics.UpdateGetForJob.Inc(1)
	return updateIDs, nil
}

func parseTime(v string) time.Time {
	r, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}
	}
	return r
}

// If a BATCH job is in active state for more than a threshold of time, it is
// possible that the lucene index is out of sync with the job_index table so we
// can read job summary from job_index table for such jobs. This function goes
// through a list of job summary and looks for such stale jobs. If the query is
// for only active jobs, the stale jobs are skipped from the job summary list
// and a new list is returned.
func (s *Store) reconcileStaleBatchJobsFromJobSummaryList(
	ctx context.Context, summaryList []*job.JobSummary,
	queryTerminalStates bool) ([]*job.JobSummary, error) {
	newSummaryList := []*job.JobSummary{}
	var err error
	for _, summary := range summaryList {
		if summary.GetType() == job.JobType_BATCH &&
			!util.IsPelotonJobStateTerminal(summary.GetRuntime().GetState()) &&
			time.Since(
				parseTime(summary.GetRuntime().GetCreationTime()),
			) > common.StaleJobStateDurationThreshold {
			// get job summary from DB table instead of index
			summary, err = s.getJobSummaryFromIndex(ctx, summary.Id)
			if err != nil {
				return nil, err
			}
			if util.IsPelotonJobStateTerminal(
				summary.GetRuntime().GetState()) &&
				!queryTerminalStates {
				// Since now the job shows up as terminal, we can conclude
				// that lucene index entry for this job is stale. Because
				// the query is for getting active jobs only, we can skip
				// this job entry.
				continue
			}
		}
		newSummaryList = append(newSummaryList, summary)
	}
	return newSummaryList, nil
}

func (s *Store) getJobSummaryFromResultMap(
	ctx context.Context, allResults []map[string]interface{},
) ([]*job.JobSummary, error) {
	var summaryResults []*job.JobSummary
	for _, value := range allResults {
		summary := &job.JobSummary{}
		id, ok := value["job_id"].(qb.UUID)
		if !ok {
			return nil, yarpcerrors.InternalErrorf(
				"invalid job_id %v", value["job_id"])
		}
		summary.Id = &peloton.JobID{Value: id.String()}
		if name, ok := value["name"].(string); ok {
			summary.Name = name
		}
		if runtimeInfo, ok := value["runtime_info"].(string); ok {
			err := json.Unmarshal([]byte(runtimeInfo), &summary.Runtime)
			if err != nil {
				log.WithError(err).
					WithField("runtime_info", runtimeInfo).
					Info("failed to unmarshal runtime info")
			}
		}
		if owningTeam, ok := value["owner"].(string); ok {
			summary.Owner = owningTeam
			summary.OwningTeam = owningTeam
		}
		if instcnt, ok := value["instance_count"].(int); ok {
			summary.InstanceCount = uint32(instcnt)
		}
		if jobType, ok := value["job_type"].(int); ok {
			summary.Type = job.JobType(jobType)
		}
		if respoolIDStr, ok := value["respool_id"].(string); ok {
			summary.RespoolID = &peloton.ResourcePoolID{Value: respoolIDStr}
		}
		if labelBuffer, ok := value["labels"].(string); ok {
			err := json.Unmarshal([]byte(labelBuffer), &summary.Labels)
			if err != nil {
				log.WithError(err).
					WithField("labels", labelBuffer).
					Info("failed to unmarshal labels")
			}
		}
		summaryResults = append(summaryResults, summary)
	}
	return summaryResults, nil
}

// SortedTaskInfoList makes TaskInfo implement sortable interface
type SortedTaskInfoList []*task.TaskInfo

func (a SortedTaskInfoList) Len() int           { return len(a) }
func (a SortedTaskInfoList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedTaskInfoList) Less(i, j int) bool { return a[i].InstanceId < a[j].InstanceId }

// SortUpdateInfo is the structure used by the sortable interface for
// updates, where the sorting will be done according to the job configuration
// version for a given job.
type SortUpdateInfo struct {
	updateID         *peloton.UpdateID
	jobConfigVersion uint64
}

// SortedUpdateList implements a sortable interface for updates according
// to the job configuration versions for a given job.
type SortedUpdateList []*SortUpdateInfo

func (u SortedUpdateList) Len() int      { return len(u) }
func (u SortedUpdateList) Swap(i, j int) { u[i], u[j] = u[j], u[i] }
func (u SortedUpdateList) Less(i, j int) bool {
	return u[i].jobConfigVersion < u[j].jobConfigVersion
}

// SortUpdateInfoTS is the structure used by the sortable interface for
// updates, where the sorting will be done according to the update create
// timestamp for a given job.
type SortUpdateInfoTS struct {
	updateID   *peloton.UpdateID
	createTime time.Time
}

// SortedUpdateListTS implements a sortable interface for updates according
// to the create time for a given job.
type SortedUpdateListTS []*SortUpdateInfoTS

func (u SortedUpdateListTS) Len() int      { return len(u) }
func (u SortedUpdateListTS) Swap(i, j int) { u[i], u[j] = u[j], u[i] }
func (u SortedUpdateListTS) Less(i, j int) bool {
	return u[i].createTime.UnixNano() < u[j].createTime.UnixNano()
}
