package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	pb_volume "code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/backoff"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra/api"
	"code.uber.internal/infra/peloton/storage/cassandra/impl"
	qb "code.uber.internal/infra/peloton/storage/querybuilder"
	"code.uber.internal/infra/peloton/util"

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
	jobConfigTable        = "job_config"
	jobRuntimeTable       = "job_runtime"
	jobIndexTable         = "job_index"
	taskConfigTable       = "task_config"
	taskRuntimeTable      = "task_runtime"
	taskStateChangesTable = "task_state_changes"
	podEventsTable        = "pod_events"
	updatesTable          = "update_info"
	frameworksTable       = "frameworks"
	taskJobStateView      = "mv_task_by_state"
	jobByStateView        = "mv_job_by_state"
	updatesByJobView      = "mv_updates_by_job"
	resPoolsTable         = "respools"
	resPoolsOwnerView     = "mv_respools_by_owner"
	volumeTable           = "persistent_volumes"
	secretInfoTable       = "secret_info"

	// DB field names
	creationTimeField   = "creation_time"
	completionTimeField = "completion_time"
	stateField          = "state"

	secretVersion0 = 0
	secretValid    = true

	_defaultQueryLimit    uint32 = 10
	_defaultQueryMaxLimit uint32 = 100

	// _defaultTaskConfigID is used for storing, and retrieving, the default
	// task configuration, when no specific is available.
	_defaultTaskConfigID = -1

	jobIndexTimeFormat        = "20060102150405"
	jobQueryDefaultSpanInDays = 7
	jobQueryJitter            = time.Second * 30
)

// Config is the config for cassandra Store
type Config struct {
	CassandraConn *impl.CassandraConn `yaml:"connection"`
	StoreName     string              `yaml:"store_name"`
	Migrations    string              `yaml:"migrations"`
	// MaxBatchSize makes sure we avoid batching too many statements and avoid
	// http://docs.datastax.com/en/archived/cassandra/3.x/cassandra/configuration/configCassandra_yaml.html#configCassandra_yaml__batch_size_fail_threshold_in_kb
	// This value is the number of records that are included in a single transaction/commit RPC request
	MaxBatchSize int `yaml:"max_batch_size_rows"`
	// MaxParallelBatches controls the maximum number of go routines run to create tasks
	MaxParallelBatches int `yaml:"max_parallel_batches"`
	// MaxUpdatesPerJob controls the maximum number of
	// updates per job kept in the database
	MaxUpdatesPerJob int `yaml:"max_updates_job"`
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
// ResourcePoolStore, PersistentVolumeStore and SecretStore using a cassandra backend
// TODO: Break this up into different files (and or structs) that implement
// each of these interfaces to keep code modular.
type Store struct {
	DataStore   api.DataStore
	metrics     *storage.Metrics
	Conf        *Config
	retryPolicy backoff.RetryPolicy
}

// NewStore creates a Store
func NewStore(config *Config, scope tally.Scope) (*Store, error) {
	dataStore, err := impl.CreateStore(config.CassandraConn, config.StoreName, scope)
	if err != nil {
		log.Errorf("Failed to NewStore, err=%v", err)
		return nil, err
	}
	return &Store{
		DataStore:   dataStore,
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

func (s *Store) executeRead(ctx context.Context, stmt api.Statement) ([]map[string]interface{}, error) {
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

func (s *Store) executeBatch(ctx context.Context, stmts []api.Statement) error {
	p := backoff.NewRetrier(s.retryPolicy)
	for {
		err := s.DataStore.ExecuteBatch(ctx, stmts)
		if err == nil {
			return err
		}
		err = s.handleDataStoreError(err, p)

		if err != nil {
			if !common.IsTransientError(err) {
				s.metrics.ErrorMetrics.NotTransient.Inc(1)
			}
			return err
		}
	}
}

// CreateJobConfig creates a job config in db
func (s *Store) CreateJobConfig(
	ctx context.Context,
	id *peloton.JobID,
	jobConfig *job.JobConfig,
	version uint64,
	owner string) error {
	jobID := id.GetValue()
	configBuffer, err := proto.Marshal(jobConfig)
	if err != nil {
		log.Errorf("Failed to marshal jobConfig, error = %v", err)
		s.metrics.JobMetrics.JobCreateConfigFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(jobConfigTable).
		Columns(
			"job_id",
			"version",
			"creation_time",
			"config").
		Values(
			jobID,
			version,
			time.Now().UTC(),
			configBuffer).
		IfNotExist()
	err = s.applyStatement(ctx, stmt, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("createJobConfig failed")
		s.metrics.JobMetrics.JobCreateConfigFail.Inc(1)
		return err
	}
	s.metrics.JobMetrics.JobCreateConfig.Inc(1)

	// TODO: Create TaskConfig here. This requires the task configs to be
	// flattened at this point, see jobmgr.

	return nil
}

// CreateTaskConfigs from the job config.
func (s *Store) CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	version := jobConfig.GetChangeLog().GetVersion()
	if jobConfig.GetDefaultConfig() != nil {
		if err := s.createTaskConfig(ctx, id, _defaultTaskConfigID, jobConfig.GetDefaultConfig(), version); err != nil {
			return err
		}
	}

	for instanceID, cfg := range jobConfig.GetInstanceConfig() {
		merged := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)
		// TODO set correct version
		if err := s.createTaskConfig(ctx, id, int64(instanceID), merged, version); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) createTaskConfig(ctx context.Context, id *peloton.JobID, instanceID int64, taskConfig *task.TaskConfig, version uint64) error {
	configBuffer, err := proto.Marshal(taskConfig)
	if err != nil {
		s.metrics.TaskMetrics.TaskCreateConfigFail.Inc(1)
		log.Errorf("Failed to marshal taskConfig, error = %v", err)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(taskConfigTable).
		Columns(
			"job_id",
			"version",
			"instance_id",
			"creation_time",
			"config").
		Values(
			id.GetValue(),
			version,
			instanceID,
			time.Now().UTC(),
			configBuffer)

	// IfNotExist() will cause Writing task configs to Cassandra concurrently
	// failed with Operation timed out issue when batch size is small, e.g. 1.
	// For now, we have to drop the IfNotExist()

	err = s.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("createTaskConfig failed")
		s.metrics.TaskMetrics.TaskCreateConfigFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.TaskCreateConfig.Inc(1)

	return nil
}

// CreateJobRuntimeWithConfig creates runtime for a job
func (s *Store) CreateJobRuntimeWithConfig(
	ctx context.Context,
	id *peloton.JobID,
	initialRuntime *job.RuntimeInfo,
	config *job.JobConfig) error {
	err := s.updateJobRuntimeWithConfig(ctx, id, initialRuntime, config)
	if err != nil {
		s.metrics.JobMetrics.JobCreateRuntimeFail.Inc(1)
		return err
	}
	s.metrics.JobMetrics.JobCreateRuntime.Inc(1)
	return nil
}

// GetMaxJobConfigVersion returns the maximum version of configs of a given job
func (s *Store) GetMaxJobConfigVersion(
	ctx context.Context,
	id *peloton.JobID) (uint64, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("MAX(version)").From(jobConfigTable).
		Where(qb.Eq{"job_id": id.GetValue()})

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to get max version of job %v: %v", id.GetValue(), err)
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

// UpdateJobConfig updates a job with the job id and the config value
// TODO(zhixin): consider remove the function signature
func (s *Store) UpdateJobConfig(
	ctx context.Context,
	id *peloton.JobID,
	jobConfig *job.JobConfig) error {
	return s.CreateJobConfig(ctx,
		id, jobConfig, jobConfig.GetChangeLog().GetVersion(),
		"<missing owner>")
}

// GetJobConfig returns a job config given the job id
// TODO(zhixin): GetJobConfig takes version as param when write through cache is implemented
func (s *Store) GetJobConfig(
	ctx context.Context,
	id *peloton.JobID) (*job.JobConfig, error) {
	r, err := s.GetJobRuntime(ctx, id)
	if err != nil {
		return nil, err
	}

	// ConfigurationVersion will be 0 for old jobs created before the
	// migration to using of ConfigurationVersion. In this case, just
	// copy over ConfigVersion to ConfigurationVersion.
	if r.ConfigurationVersion == uint64(0) {
		r.ConfigurationVersion = uint64(r.ConfigVersion)
	}

	return s.GetJobConfigWithVersion(ctx, id, r.GetConfigurationVersion())
}

// GetJobConfigWithVersion fetches the job configuration for a given
// job of a given version
func (s *Store) GetJobConfigWithVersion(ctx context.Context,
	id *peloton.JobID,
	version uint64,
) (*job.JobConfig, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("config").From(jobConfigTable).
		Where(qb.Eq{"job_id": jobID, "version": version})
	stmtString, _, _ := stmt.ToSQL()
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to execute stmt %v, err=%v", stmtString, err)
		s.metrics.JobMetrics.JobGetFail.Inc(1)
		return nil, err
	}

	if len(allResults) > 1 {
		s.metrics.JobMetrics.JobGetFail.Inc(1)
		return nil,
			yarpcerrors.FailedPreconditionErrorf(
				"found %d jobs %v for job id %v",
				len(allResults), allResults, jobID)
	}
	for _, value := range allResults {
		var record JobConfigRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into JobRecord, err= %v", err)
			s.metrics.JobMetrics.JobGetFail.Inc(1)
			return nil, err
		}
		s.metrics.JobMetrics.JobGet.Inc(1)

		jobConfig, err := record.GetJobConfig()
		if err != nil {
			s.metrics.JobMetrics.JobGetFail.Inc(1)
			return nil, err
		}
		if jobConfig.GetChangeLog().GetVersion() < 1 {
			// Older job which does not have changelog.
			// TODO (zhixin): remove this after no more job in the system
			// does not have a changelog version.
			v, err := s.GetMaxJobConfigVersion(ctx, id)
			if err != nil {
				s.metrics.JobMetrics.JobGetFail.Inc(1)
				return nil, err
			}
			jobConfig.ChangeLog = &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   v,
			}
		}
		return jobConfig, nil
	}
	s.metrics.JobMetrics.JobNotFound.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf(
		"cannot find job wth jobID %v", jobID)
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
		log.WithField("creationTimeRange", creationTimeRange).
			WithError(err).
			Error("fail to build time range filter")
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, nil, 0, err
	}

	err = clauses.WithTimeRangeFilter(completionTimeRange, completionTimeField)
	if err != nil {
		log.WithField("completionTimeRange", completionTimeRange).
			WithError(err).
			Error("fail to build time range filter")
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
			log.WithField("now", now).
				WithError(err).
				Error("fail to create time stamp proto")
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		min, err := ptypes.TimestampProto(now.AddDate(0, 0, -jobQueryDefaultSpanInDays))
		if err != nil {
			log.WithField("now", now).
				WithError(err).
				Error("fail to create time stamp proto")
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, err
		}
		defaultCreationTimeRange := &peloton.TimeRange{Min: min, Max: max}
		err = clauses.WithTimeRangeFilter(defaultCreationTimeRange, "creation_time")
		if err != nil {
			log.WithField("defaultCreationTimeRange", defaultCreationTimeRange).
				WithError(err).
				Error("fail to build time range filter")
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

	summaryResults, err := s.getJobSummaryFromLuceneResult(ctx, allResults)
	if summaryOnly {
		if err != nil {
			// Process error if caller requested only job summary
			uql, args, _, _ := stmt.ToUql()
			log.WithField("uql", uql).
				WithField("args", args).
				WithField("labels", spec.GetLabels()).
				WithError(err).
				Error("Failed to get JobSummary")
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
			uql, args, _, _ := stmt.ToUql()
			log.WithField("uql", uql).
				WithField("args", args).
				WithField("labels", spec.GetLabels()).
				WithField("job_id", value).
				Error("fail to find job_id in query result")
			s.metrics.JobMetrics.JobQueryFail.Inc(1)
			return nil, nil, 0, fmt.Errorf("got invalid response from cassandra")
		}

		jobID := &peloton.JobID{
			Value: id.String(),
		}

		jobRuntime, err := s.GetJobRuntime(ctx, jobID)
		if err != nil {
			log.WithError(err).
				WithField("job_id", id.String()).
				Warn("no job runtime found when executing jobs query")
			continue
		}
		// TODO (chunyang.shen): use job/task cache to get JobConfig T1760469
		jobConfig, err := s.GetJobConfig(ctx, jobID)
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

// GetJobsByStates returns all Jobs which belong one of the states
func (s *Store) GetJobsByStates(ctx context.Context, states []job.JobState) ([]peloton.JobID, error) {
	queryBuilder := s.DataStore.NewQuery()

	var jobStates []string
	for _, state := range states {
		jobStates = append(jobStates, state.String())
	}
	callStart := time.Now()

	stmt := queryBuilder.Select("job_id").From(jobByStateView).
		Where(qb.Eq{"state": jobStates})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			Error("GetJobsByStates failed")
		s.metrics.JobMetrics.JobGetByStatesFail.Inc(1)
		callDuration := time.Since(callStart)
		s.metrics.JobMetrics.JobGetByStatesFailDuration.Record(callDuration)
		return nil, err
	}

	var jobs []peloton.JobID
	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Failed to get JobRuntimeRecord from record")
			s.metrics.JobMetrics.JobGetByStatesFail.Inc(1)
			callDuration := time.Since(callStart)
			s.metrics.JobMetrics.JobGetByStatesFailDuration.Record(callDuration)
			return nil, err
		}
		jobs = append(jobs, peloton.JobID{Value: record.JobID.String()})
	}
	s.metrics.JobMetrics.JobGetByStates.Inc(1)
	callDuration := time.Since(callStart)
	s.metrics.JobMetrics.JobGetByStatesDuration.Record(callDuration)
	return jobs, nil
}

// GetAllJobs returns all jobs
// TODO: introduce jobstate and add GetJobsByState
func (s *Store) GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error) {
	// TODO: Get jobs from  all edge respools
	var resultMap = make(map[string]*job.RuntimeInfo)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobRuntimeTable)
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			Error("Fail to Query all jobs")
		s.metrics.JobMetrics.JobGetAllFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobMetrics.JobGetAllFail.Inc(1)
			return nil, err
		}
		jobRuntime, err := record.GetJobRuntime()
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobMetrics.JobGetAllFail.Inc(1)
			return nil, err
		}
		resultMap[record.JobID.String()] = jobRuntime
	}
	s.metrics.JobMetrics.JobGetAll.Inc(1)
	return resultMap, nil
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
	if jobType == job.JobType_BATCH {
		err = s.logTaskStateChange(ctx, jobID, instanceID, runtime)
	} else if jobType == job.JobType_SERVICE {
		err = s.addPodEvent(ctx, jobID, instanceID, runtime)
	}
	if err != nil {
		log.Errorf("Unable to log task state changes for job ID %v instance %v, error = %v", jobID.GetValue(), instanceID, err)
		return err
	}
	return nil
}

// CreateTaskRuntimes creates task runtimes for the given slice of task runtimes, instances 0..n
func (s *Store) CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo, owner string) error {
	var instanceIDList []uint32

	maxBatchSize := uint32(s.Conf.MaxBatchSize)
	maxParallelBatches := uint32(s.Conf.MaxParallelBatches)
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxUint32
	}
	jobID := id.GetValue()
	for instanceID := range runtimes {
		instanceIDList = append(instanceIDList, instanceID)
	}
	nTasks := uint32(len(runtimes))

	tasksNotCreated := uint32(0)
	timeStart := time.Now()
	nBatches := nTasks / maxBatchSize
	if (nTasks % maxBatchSize) > 0 {
		nBatches = nBatches + 1
	}
	increment := uint32(0)
	if (nBatches % maxParallelBatches) > 0 {
		increment = 1
	}

	wg := new(sync.WaitGroup)
	prevEnd := uint32(0)
	log.WithField("batches", nBatches).
		WithField("tasks", nTasks).
		Debug("Creating tasks")

	for i := uint32(0); i < maxParallelBatches; i++ {
		batchStart := prevEnd
		batchEnd := batchStart + (nBatches / maxParallelBatches) + increment
		if batchEnd > nTasks {
			batchEnd = nTasks
		}
		prevEnd = batchEnd
		if batchStart == batchEnd {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := batchStart; batch < batchEnd; batch++ {
				// do batching by rows, up to s.Conf.MaxBatchSize
				start := batch * maxBatchSize // the starting instance ID
				end := nTasks                 // the end bounds (noninclusive)
				if nTasks >= (batch+1)*maxBatchSize {
					end = (batch + 1) * maxBatchSize
				}
				batchSize := end - start // how many tasks in this batch
				if batchSize < 1 {
					// skip if it overflows
					continue
				}
				batchTimeStart := time.Now()
				insertStatements := []api.Statement{}
				idsToTaskRuntimes := map[string]*task.RuntimeInfo{}
				log.WithField("id", id.GetValue()).
					WithField("start", start).
					WithField("end", end).
					Debug("creating tasks")
				for k := start; k < end; k++ {
					instanceID := instanceIDList[k]
					runtime := runtimes[instanceID]
					if runtime == nil {
						continue
					}

					taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
					idsToTaskRuntimes[taskID] = runtime

					runtimeBuffer, err := proto.Marshal(runtime)
					if err != nil {
						log.WithField("job_id", id.GetValue()).
							WithField("instance_id", instanceID).
							WithError(err).
							Error("Failed to create task runtime")
						atomic.AddUint32(&tasksNotCreated, batchSize)
						return
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
							jobID,
							instanceID,
							runtime.GetRevision().GetVersion(),
							time.Now().UTC(),
							runtime.GetState().String(),
							runtimeBuffer)

					// IfNotExist() will cause Writing 20 tasks (0:19) for TestJob2 to Cassandra failed in 8.756852ms with
					// Batch with conditions cannot span multiple partitions. For now, drop the IfNotExist()

					insertStatements = append(insertStatements, stmt)
				}
				err := s.applyStatements(ctx, insertStatements, jobID)
				if err != nil {
					log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
						Errorf("Writing %d tasks (%d:%d) for %v to Cassandra failed in %v with %v", batchSize, start, end-1, id.GetValue(), time.Since(batchTimeStart), err)
					atomic.AddUint32(&tasksNotCreated, batchSize)
					return
				}
				log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
					Debugf("Wrote %d tasks (%d:%d) for %v to Cassandra in %v", batchSize, start, end-1, id.GetValue(), time.Since(batchTimeStart))
				err = s.logTaskStateChanges(ctx, idsToTaskRuntimes)
				if err != nil {
					log.Errorf("Unable to log task state changes for job ID %v range(%d:%d), error = %v", jobID, start, end-1, err)
				}
			}
		}()
	}
	wg.Wait()

	if tasksNotCreated != 0 {
		s.metrics.TaskMetrics.TaskCreateFail.Inc(int64(tasksNotCreated))
		s.metrics.TaskMetrics.TaskCreate.Inc(int64(nTasks - tasksNotCreated))
		msg := fmt.Sprintf(
			"Wrote %d tasks for %v, and was unable to write %d tasks to Cassandra in %v",
			nTasks-tasksNotCreated,
			jobID,
			tasksNotCreated,
			time.Since(timeStart))
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}

	s.metrics.TaskMetrics.TaskCreate.Inc(int64(nTasks))

	log.WithField("duration_s", time.Since(timeStart).Seconds()).
		Infof("Wrote all %d tasks for %v to Cassandra in %v", nTasks, jobID, time.Since(timeStart))
	return nil

}

// addPodEvent upserts single pod state change for a Job -> Instance -> Run.
// Task state events are sorted by reverse chronological run_id and time of event.
func (s *Store) addPodEvent(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *task.RuntimeInfo) error {
	var runID, prevRunID int
	var podStatus []byte
	var err, errMessage error

	errLog := false
	if runID, err = util.ParseRunID(
		runtime.GetMesosTaskId().GetValue()); err != nil {
		errLog = true
		errMessage = err
	}
	if prevRunID, err = util.ParseRunID(
		runtime.GetPrevMesosTaskId().GetValue()); err != nil {
		errLog = true
		errMessage = err
	}
	if podStatus, err = proto.Marshal(runtime); err != nil {
		errLog = true
		errMessage = err
	}
	if errLog {
		log.WithFields(log.Fields{
			"job_id":                 jobID.GetValue(),
			"instance_id":            instanceID,
			"mesos_task_id":          runtime.GetMesosTaskId().GetValue(),
			"previous_mesos_task_id": runtime.GetPrevMesosTaskId().GetValue(),
			"actual_state":           runtime.GetState().String(),
			"goal_state":             runtime.GetGoalState().String(),
		}).WithError(errMessage).
			Info("pre-validation for upsert pod event failed")
		s.metrics.TaskMetrics.TaskLogStateFail.Inc(1)
		return errMessage
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(podEventsTable).
		Columns(
			"job_id",
			"instance_id",
			"run_id",
			"previous_run_id",
			"update_time",
			"actual_state",
			"goal_state",
			"hostname",
			"agent_id",
			"config_version",
			"desired_config_version",
			"volumeID",
			"message",
			"reason",
			"pod_status").
		Values(
			jobID.GetValue(),
			instanceID,
			runID,
			prevRunID,
			qb.UUID{UUID: gocql.UUIDFromTime(time.Now())},
			runtime.GetState().String(),
			runtime.GetGoalState().String(),
			runtime.GetHost(),
			runtime.GetAgentID().GetValue(),
			runtime.GetConfigVersion(),
			runtime.GetDesiredConfigVersion(),
			runtime.GetVolumeID().GetValue(),
			runtime.GetMessage(),
			runtime.GetReason(),
			podStatus).Into(podEventsTable)

	err = s.applyStatement(ctx, stmt, runtime.GetMesosTaskId().GetValue())
	if err != nil {
		log.WithFields(log.Fields{
			"job_id":          jobID.GetValue(),
			"instance_id":     instanceID,
			"run_id":          runtime.GetMesosTaskId().GetValue(),
			"previous_run_id": runtime.GetPrevMesosTaskId().GetValue(),
			"actual_state":    runtime.GetState().String(),
			"goal_state":      runtime.GetGoalState().String(),
		}).WithError(err).Error("adding a pod event failed")
		s.metrics.TaskMetrics.TaskLogStateFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.TaskLogState.Inc(1)
	return nil
}

// getPodEvents test method to read pod events for test validation
// TODO: update method to return pod events result set for CLI, UI & sandbox.
func (s *Store) getPodEvents(
	ctx context.Context,
	jobID string,
	instanceID int) (int, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(podEventsTable).
		Where(qb.Eq{"job_id": jobID, "instance_id": instanceID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Error(err)
		return -1, err
	}
	return len(allResults), nil
}

// logTaskStateChange logs the task state change events
func (s *Store) logTaskStateChange(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error {
	var stateChange = TaskStateChangeRecord{
		JobID:           jobID.GetValue(),
		InstanceID:      instanceID,
		MesosTaskID:     runtime.GetMesosTaskId().GetValue(),
		AgentID:         runtime.GetAgentID().GetValue(),
		TaskState:       runtime.GetState().String(),
		TaskHost:        runtime.GetHost(),
		EventTime:       time.Now().UTC().Format(time.RFC3339),
		Reason:          runtime.GetReason(),
		Healthy:         runtime.GetHealthy().String(),
		Message:         runtime.GetMessage(),
		PrevMesosTaskID: runtime.GetPrevMesosTaskId().GetValue(),
	}
	buffer, err := json.Marshal(stateChange)
	if err != nil {
		log.Errorf("Failed to marshal stateChange, error = %v", err)
		s.metrics.TaskMetrics.TaskLogStateFail.Inc(1)
		return err
	}
	stateChangePart := []string{string(buffer)}
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(taskStateChangesTable).
		Add("events", stateChangePart).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})
	result, err := s.executeWrite(ctx, stmt)
	if result != nil {
		defer result.Close()
	}
	if err != nil {
		log.Errorf("Fail to logTaskStateChange by jobID %v, instanceID %v %v, err=%v", jobID, instanceID, stateChangePart, err)
		s.metrics.TaskMetrics.TaskLogStateFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.TaskLogState.Inc(1)
	return nil
}

// logTaskStateChanges logs multiple task state change events in a batch operation (one RPC, separate statements)
// taskIDToTaskInfos is a map of task ID to task info
func (s *Store) logTaskStateChanges(ctx context.Context, taskIDToTaskRuntimes map[string]*task.RuntimeInfo) error {
	statements := []api.Statement{}
	nTasks := int64(0)
	for taskID, runtime := range taskIDToTaskRuntimes {
		jobID, instanceID, err := util.ParseTaskID(taskID)
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("Invalid task id")
			return err
		}
		stateChange := TaskStateChangeRecord{
			JobID:           jobID,
			InstanceID:      uint32(instanceID),
			TaskState:       runtime.GetState().String(),
			TaskHost:        runtime.GetHost(),
			EventTime:       time.Now().UTC().Format(time.RFC3339),
			MesosTaskID:     runtime.GetMesosTaskId().GetValue(),
			AgentID:         runtime.GetAgentID().GetValue(),
			Reason:          runtime.GetReason(),
			Healthy:         runtime.GetHealthy().String(),
			Message:         runtime.GetMessage(),
			PrevMesosTaskID: runtime.GetPrevMesosTaskId().GetValue(),
		}
		buffer, err := json.Marshal(stateChange)
		if err != nil {
			log.Errorf("Failed to marshal stateChange for task %v, error = %v", taskID, err)
			return err
		}
		stateChangePart := []string{string(buffer)}
		queryBuilder := s.DataStore.NewQuery()
		stmt := queryBuilder.Update(taskStateChangesTable).
			Add("events", stateChangePart).
			Where(qb.Eq{"job_id": jobID, "instance_id": instanceID})
		statements = append(statements, stmt)
		nTasks++
	}
	err := s.executeBatch(ctx, statements)
	if err != nil {
		log.Errorf("Fail to logTaskStateChanges for %d tasks, err=%v", len(taskIDToTaskRuntimes), err)
		s.metrics.TaskMetrics.TaskLogStateFail.Inc(nTasks)
		return err
	}
	s.metrics.TaskMetrics.TaskLogState.Inc(nTasks)
	return nil
}

// GetTaskStateChanges returns the state changes for a task
func (s *Store) GetTaskStateChanges(ctx context.Context, jobID *peloton.JobID, instanceID uint32) ([]*TaskStateChangeRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskStateChangesTable).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by jobID %v, instanceID %v, err=%v", jobID, instanceID, err)
		s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var stateChangeRecords TaskStateChangeRecords
		err = FillObject(value, &stateChangeRecords, reflect.TypeOf(stateChangeRecords))
		if err != nil {
			log.Errorf("Failed to Fill into TaskStateChangeRecords, val = %v err= %v", value, err)
			s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
			return nil, err
		}
		s.metrics.TaskMetrics.TaskGetLogState.Inc(1)
		return stateChangeRecords.GetStateChangeRecords()
	}
	s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
	return nil, fmt.Errorf("No state change records found for jobID %v, instanceID %v", jobID, instanceID)
}

// GetTaskEvents returns the events list for a task
func (s *Store) GetTaskEvents(ctx context.Context, jobID *peloton.JobID, instanceID uint32) ([]*task.TaskEvent, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskStateChangesTable).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("instance_id", instanceID).
			Error("Failed to get task state changes")
		s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
		return nil, err
	}

	var result []*task.TaskEvent
	for _, value := range allResults {
		var stateChangeRecords TaskStateChangeRecords
		err = FillObject(value, &stateChangeRecords, reflect.TypeOf(stateChangeRecords))
		if err != nil {
			log.WithError(err).
				WithField("value", value).
				Error("Failed to Fill into TaskStateChangeRecords")
			s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
			return nil, err
		}

		records, err := stateChangeRecords.GetStateChangeRecords()
		if err != nil {
			log.WithError(err).
				Error("Failed to get TaskStateChangeRecords")
			s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
			return nil, err
		}

		for _, record := range records {
			state, ok := task.TaskState_value[record.TaskState]
			if !ok {
				log.WithError(err).
					WithField("task_state", record.TaskState).
					Error("Unknown TaskState")
				state, _ = task.TaskState_value["UNKNOWN"]
			}
			rec := &task.TaskEvent{
				// TaskStateChangeRecords does not store event source, so don't set Source here
				State:     task.TaskState(state),
				Timestamp: record.EventTime,
				Message:   record.Message,
				Reason:    record.Reason,
				Healthy:   task.HealthState(task.HealthState_value[record.Healthy]),
				Hostname:  record.TaskHost,
				// TaskId here will contain Mesos TaskId
				TaskId: &peloton.TaskID{
					Value: record.MesosTaskID,
				},
				AgentId: record.AgentID,
				PrevTaskId: &peloton.TaskID{
					Value: record.PrevMesosTaskID,
				},
			}
			result = append(result, rec)
		}
		s.metrics.TaskMetrics.TaskGetLogState.Inc(1)
		return result, nil

	}
	s.metrics.TaskMetrics.TaskGetLogStateFail.Inc(1)
	return nil, fmt.Errorf("No state change records found for jobID %v, instanceID %v", jobID, instanceID)
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

// GetTaskConfig returns the task specific config
func (s *Store) GetTaskConfig(ctx context.Context, id *peloton.JobID,
	instanceID uint32, version uint64) (*task.TaskConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskConfigTable).
		Where(
			qb.Eq{
				"job_id":      id.GetValue(),
				"version":     version,
				"instance_id": []interface{}{instanceID, _defaultTaskConfigID},
			})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithField("instance_id", instanceID).
			WithField("version", version).
			WithError(err).
			Error("Fail to getTaskConfig")
		s.metrics.TaskMetrics.TaskGetConfigFail.Inc(1)
		return nil, err
	}
	taskID := fmt.Sprintf(taskIDFmt, id.GetValue(), int(instanceID))

	if len(allResults) == 0 {
		return nil, &storage.TaskNotFoundError{TaskID: taskID}
	}

	// Use last result (the most specific).
	value := allResults[len(allResults)-1]
	var record TaskConfigRecord
	if err := FillObject(value, &record, reflect.TypeOf(record)); err != nil {
		log.WithField("task_id", taskID).
			WithError(err).
			Error("Failed to Fill into TaskRecord")
		s.metrics.TaskMetrics.TaskGetConfigFail.Inc(1)
		return nil, err
	}

	s.metrics.TaskMetrics.TaskGetConfig.Inc(1)
	return record.GetTaskConfig()
}

// GetTaskConfigs returns the task configs for a list of instance IDs,
// job ID and config version.
func (s *Store) GetTaskConfigs(ctx context.Context, id *peloton.JobID,
	instanceIDs []uint32, version uint64) (map[uint32]*task.TaskConfig, error) {
	taskConfigMap := make(map[uint32]*task.TaskConfig)

	// add default instance ID to read the default config
	var dbInstanceIDs []int
	for _, instance := range instanceIDs {
		dbInstanceIDs = append(dbInstanceIDs, int(instance))
	}
	dbInstanceIDs = append(dbInstanceIDs, _defaultTaskConfigID)

	stmt := s.DataStore.NewQuery().Select("*").From(taskConfigTable).
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
		return taskConfigMap, err
	}

	if len(allResults) == 0 {
		log.Info("no results")
		return taskConfigMap, nil
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
			return nil, err
		}
		taskConfig, err := record.GetTaskConfig()
		if err != nil {
			return nil, err
		}
		if record.InstanceID == _defaultTaskConfigID {
			// get the default config
			defaultConfig = taskConfig
			continue
		}
		taskConfigMap[uint32(record.InstanceID)] = taskConfig
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
				return nil, fmt.Errorf("unable to read default task config")
			}
			taskConfigMap[instance] = defaultConfig
		}
	}
	s.metrics.TaskMetrics.TaskGetConfigs.Inc(1)
	return taskConfigMap, nil
}

func (s *Store) getTaskInfoFromRuntimeRecord(ctx context.Context, id *peloton.JobID, record *TaskRuntimeRecord) (*task.TaskInfo, error) {
	runtime, err := record.GetTaskRuntime()
	if err != nil {
		log.Errorf("Failed to parse task runtime from record, val = %v err= %v", record, err)
		return nil, err
	}

	config, err := s.GetTaskConfig(ctx, id, uint32(record.InstanceID),
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

//TODO GetTaskIDsForJobAndState and GetTasksForJobAndState have similar functionalities.
// They need to be merged with the common function only returning a task summary
// instead of full config and runtime.

// GetTaskIDsForJobAndState returns a list of instance-ids for a peloton job with certain state.
func (s *Store) GetTaskIDsForJobAndState(ctx context.Context, id *peloton.JobID, state string) ([]uint32, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("instance_id").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": state})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("task_state", state).
			Error("fail to fetch instance id list from db")
		s.metrics.TaskMetrics.TaskIDsGetForJobAndStateFail.Inc(1)
		return nil, err
	}

	var result []uint32
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID).
				WithField("task_state", state).
				Error("fail to fill task id into task record")
			s.metrics.TaskMetrics.TaskIDsGetForJobAndStateFail.Inc(1)
			return nil, err
		}
		result = append(result, uint32(record.InstanceID))
	}

	s.metrics.TaskMetrics.TaskIDsGetForJobAndState.Inc(1)
	return result, nil
}

// GetTasksForJobAndStates returns the tasks for a peloton job which are in one of the specified states.
// result map key is TaskID, value is TaskHost
func (s *Store) GetTasksForJobAndStates(ctx context.Context, id *peloton.JobID, states []task.TaskState) (map[uint32]*task.TaskInfo, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	var taskStates []string
	for _, state := range states {
		taskStates = append(taskStates, state.String())
	}
	stmt := queryBuilder.Select("instance_id").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": taskStates})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID).
			WithField("states", states).
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
		resultMap[uint32(record.InstanceID)], err = s.getTask(ctx, id.GetValue(), uint32(record.InstanceID))
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

// getTaskStateCount returns the task count for a peloton job with certain state
func (s *Store) getTaskStateCount(ctx context.Context, id *peloton.JobID, state string) (uint32, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("count (*)").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": state})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.
			WithField("job_id", jobID).
			WithField("state", state).
			WithError(err).
			Error("Fail to getTaskStateCount by jobId")
		return 0, err
	}

	log.Debugf("counts: %v", allResults)
	for _, value := range allResults {
		for _, count := range value {
			val := count.(int64)
			return uint32(val), nil
		}
	}
	return 0, nil
}

// GetTaskStateSummaryForJob returns the tasks count (runtime_config) for a peloton job with certain state
func (s *Store) GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error) {
	resultMap := make(map[string]uint32)
	for _, state := range task.TaskState_name {
		count, err := s.getTaskStateCount(ctx, id, state)
		if err != nil {
			s.metrics.TaskMetrics.TaskSummaryForJobFail.Inc(1)
			return nil, err
		}
		resultMap[state] = count
	}
	s.metrics.TaskMetrics.TaskSummaryForJob.Inc(1)
	return resultMap, nil
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
		Where(qb.Eq{"job_id": jobID}).
		Where("instance_id >= ?", instanceRange.From).
		Where("instance_id < ?", instanceRange.To)

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
		configs, err := s.GetTaskConfigs(ctx, id, instances,
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
	if jobType == job.JobType_BATCH {
		s.logTaskStateChange(ctx, jobID, instanceID, runtime)
	} else if jobType == job.JobType_SERVICE {
		s.addPodEvent(ctx, jobID, instanceID, runtime)
	}
	return nil
}

// UpdateTaskRuntimes updates task runtimes for the given task instance-ids
func (s *Store) UpdateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo) error {
	var instanceIDList []uint32

	// TODO the batching used in this function needs to be abstracted to a common
	// routine to be used by any storage API which needs to batch
	maxBatchSize := uint32(s.Conf.MaxBatchSize)
	maxParallelBatches := uint32(s.Conf.MaxParallelBatches)
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxUint32
	}
	jobID := id.GetValue()
	for instanceID := range runtimes {
		instanceIDList = append(instanceIDList, instanceID)
	}
	nTasks := uint32(len(runtimes))

	tasksNotUpdated := uint32(0)
	timeStart := time.Now()
	nBatches := nTasks / maxBatchSize
	if (nTasks % maxBatchSize) > 0 {
		nBatches = nBatches + 1
	}
	increment := uint32(0)
	if (nBatches % maxParallelBatches) > 0 {
		increment = 1
	}

	wg := new(sync.WaitGroup)
	prevEnd := uint32(0)
	log.WithField("batches", nBatches).
		WithField("tasks", nTasks).
		WithField("job_id", jobID).
		Debug("updating task runtimes")

	for i := uint32(0); i < maxParallelBatches; i++ {
		batchStart := prevEnd
		batchEnd := batchStart + (nBatches / maxParallelBatches) + increment
		if batchEnd > nTasks {
			batchEnd = nTasks
		}
		prevEnd = batchEnd
		if batchStart == batchEnd {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := batchStart; batch < batchEnd; batch++ {
				// do batching by rows, up to s.Conf.MaxBatchSize
				start := batch * maxBatchSize // the starting instance ID
				end := nTasks                 // the end bounds (noninclusive)
				if nTasks >= (batch+1)*maxBatchSize {
					end = (batch + 1) * maxBatchSize
				}
				batchSize := end - start // how many tasks in this batch
				if batchSize < 1 {
					// skip if it overflows
					continue
				}
				batchTimeStart := time.Now()
				insertStatements := []api.Statement{}
				idsToTaskRuntimes := map[string]*task.RuntimeInfo{}
				log.WithField("id", id.GetValue()).
					WithField("start", start).
					WithField("end", end).
					Debug("updating task runtimes")
				for k := start; k < end; k++ {
					instanceID := instanceIDList[k]
					runtime := runtimes[instanceID]
					if runtime == nil {
						continue
					}

					taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
					idsToTaskRuntimes[taskID] = runtime

					runtimeBuffer, err := proto.Marshal(runtime)
					if err != nil {
						log.WithField("job_id", id.GetValue()).
							WithField("instance_id", instanceID).
							WithError(err).
							Error("failed to update task runtime")
						atomic.AddUint32(&tasksNotUpdated, batchSize)
						return
					}

					queryBuilder := s.DataStore.NewQuery()
					stmt := queryBuilder.Update(taskRuntimeTable).
						Set("version", runtime.Revision.Version).
						Set("update_time", time.Now().UTC()).
						Set("state", runtime.GetState().String()).
						Set("runtime_info", runtimeBuffer).
						Where(qb.Eq{"job_id": id.GetValue(), "instance_id": instanceID})
					insertStatements = append(insertStatements, stmt)
				}
				err := s.applyStatements(ctx, insertStatements, jobID)
				if err != nil {
					log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
						Errorf("Updating %d task runtimes (%d:%d) for %v to Cassandra failed in %v with %v", batchSize, start, end-1, id.GetValue(), time.Since(batchTimeStart), err)
					atomic.AddUint32(&tasksNotUpdated, batchSize)
					return
				}
				log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
					Debugf("updated %d task runtimes (%d:%d) for %v to Cassandra in %v", batchSize, start, end-1, id.GetValue(), time.Since(batchTimeStart))
				err = s.logTaskStateChanges(ctx, idsToTaskRuntimes)
				if err != nil {
					log.Errorf("Unable to log task state changes for job ID %v range(%d:%d), error = %v", jobID, start, end-1, err)
				}
			}
		}()
	}
	wg.Wait()

	if tasksNotUpdated != 0 {
		s.metrics.TaskMetrics.TaskUpdateFail.Inc(int64(tasksNotUpdated))
		s.metrics.TaskMetrics.TaskUpdate.Inc(int64(nTasks - tasksNotUpdated))
		msg := fmt.Sprintf(
			"Updated %d task runtimes for %v, and was unable to write %d tasks to Cassandra in %v",
			nTasks-tasksNotUpdated,
			jobID,
			tasksNotUpdated,
			time.Since(timeStart))
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}

	s.metrics.TaskMetrics.TaskUpdate.Inc(int64(nTasks))

	log.WithField("duration_s", time.Since(timeStart).Seconds()).
		Debug("updated all %d task runtimes for %v to Cassandra in %v", nTasks, jobID, time.Since(timeStart))
	return nil

}

// GetTaskForJob returns a task by jobID and instanceID
func (s *Store) GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	taskID := fmt.Sprintf(taskIDFmt, id.GetValue(), int(instanceID))
	taskInfo, err := s.GetTaskByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	result := make(map[uint32]*task.TaskInfo)
	result[instanceID] = taskInfo
	return result, nil
}

// DeleteJob deletes a job and associated tasks, by job id.
// TODO: This implementation is not perfect, as if it's getting an transient
// error, the job or some tasks may not be fully deleted.
func (s *Store) DeleteJob(ctx context.Context, id *peloton.JobID) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(taskRuntimeTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	stmt = queryBuilder.Delete(taskConfigTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	// Delete all updates for the job
	updateIDs, err := s.GetUpdatesForJob(ctx, id)
	if err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	for _, id := range updateIDs {
		if err := s.deleteSingleUpdate(ctx, id); err != nil {
			return err
		}
	}

	stmt = queryBuilder.Delete(jobIndexTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	stmt = queryBuilder.Delete(jobConfigTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	stmt = queryBuilder.Delete(jobRuntimeTable).Where(qb.Eq{"job_id": id.GetValue()})
	err = s.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
	} else {
		s.metrics.JobMetrics.JobDelete.Inc(1)
	}
	return err
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
	return nil, &storage.TaskNotFoundError{TaskID: taskID}
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

func (s *Store) applyStatements(ctx context.Context, stmts []api.Statement, jobID string) error {
	err := s.executeBatch(ctx, stmts)
	if err != nil {
		log.Errorf("Fail to execute %d insert statements for job %v, err=%v", len(stmts), jobID, err)
		return err
	}
	return nil
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

// CreateResourcePool creates a resource pool with the resource pool id and the config value
func (s *Store) CreateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig, owner string) error {
	resourcePoolID := id.GetValue()
	configBuffer, err := json.Marshal(resPoolConfig)
	if err != nil {
		log.Errorf("error = %v", err)
		s.metrics.ResourcePoolMetrics.ResourcePoolCreateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(resPoolsTable).
		Columns("respool_id", "respool_config", "owner", "creation_time", "update_time").
		Values(resourcePoolID, string(configBuffer), owner, time.Now().UTC(), time.Now().UTC()).
		IfNotExist()

	err = s.applyStatement(ctx, stmt, resourcePoolID)
	if err != nil {
		s.metrics.ResourcePoolMetrics.ResourcePoolCreateFail.Inc(1)
		return err
	}
	s.metrics.ResourcePoolMetrics.ResourcePoolCreate.Inc(1)
	return nil
}

// GetResourcePool gets a resource pool info object
func (s *Store) GetResourcePool(ctx context.Context, id *peloton.ResourcePoolID) (*respool.ResourcePoolInfo, error) {
	return nil, errors.New("unimplemented")
}

// DeleteResourcePool Deletes the resource pool
func (s *Store) DeleteResourcePool(ctx context.Context, id *peloton.ResourcePoolID) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(resPoolsTable).Where(qb.Eq{"respool_id": id.GetValue()})
	err := s.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		s.metrics.ResourcePoolMetrics.ResourcePoolDeleteFail.Inc(1)
		return err
	}
	s.metrics.ResourcePoolMetrics.ResourcePoolDelete.Inc(1)
	return nil
}

// UpdateResourcePool Updates the resource pool config for a give resource pool
// ID
func (s *Store) UpdateResourcePool(ctx context.Context, id *peloton.ResourcePoolID,
	resPoolConfig *respool.ResourcePoolConfig) error {
	resourcePoolID := id.GetValue()
	configBuffer, err := json.Marshal(resPoolConfig)
	if err != nil {
		log.
			WithField("respool_ID", resourcePoolID).
			WithError(err).
			Error("Failed to unmarshal resource pool config")
		s.metrics.ResourcePoolMetrics.ResourcePoolUpdateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(resPoolsTable).
		Set("respool_config", configBuffer).
		Set("update_time", time.Now().UTC()).
		Where(qb.Eq{"respool_id": resourcePoolID}).
		IfOnly("EXISTS")

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithField("respool_ID", resourcePoolID).
			WithError(err).
			Error("Failed to update resource pool config")
		s.metrics.ResourcePoolMetrics.ResourcePoolUpdateFail.Inc(1)
		return err
	}
	s.metrics.ResourcePoolMetrics.ResourcePoolUpdate.Inc(1)
	return nil
}

// GetAllResourcePools Get all the resource pool configs
func (s *Store) GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("respool_id", "owner", "respool_config", "creation_time", "update_time").From(resPoolsTable)

	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetAllResourcePools, err=%v", err)
		s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*respool.ResourcePoolConfig)
	for _, value := range allResults {
		var record ResourcePoolRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into ResourcePoolRecord, err= %v", err)
			s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resourcePoolConfig, err := record.GetResourcePoolConfig()
		if err != nil {
			log.Errorf("Failed to get ResourceConfig from record, err= %v", err)
			s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resultMap[record.RespoolID] = resourcePoolConfig
		s.metrics.ResourcePoolMetrics.ResourcePoolGet.Inc(1)
	}
	return resultMap, nil
}

// GetResourcePoolsByOwner Get all the resource pool b owner
func (s *Store) GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("respool_id", "owner", "respool_config", "creation_time", "update_time").From(resPoolsOwnerView).
		Where(qb.Eq{"owner": owner})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetResourcePoolsByOwner %v, err=%v", owner, err)
		s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*respool.ResourcePoolConfig)

	for _, value := range allResults {
		var record ResourcePoolRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into ResourcePoolRecord, err= %v", err)
			s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resourcePoolConfig, err := record.GetResourcePoolConfig()
		if err != nil {
			log.Errorf("Failed to get ResourceConfig from record, err= %v", err)
			s.metrics.ResourcePoolMetrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resultMap[record.RespoolID] = resourcePoolConfig
		s.metrics.ResourcePoolMetrics.ResourcePoolGet.Inc(1)
	}
	return resultMap, nil
}

// GetJobRuntime returns the job runtime info
func (s *Store) GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobRuntimeTable).
		Where(qb.Eq{"job_id": id.GetValue()})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("GetJobRuntime failed")
		s.metrics.JobMetrics.JobGetRuntimeFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", id.GetValue()).
				WithField("value", value).
				Error("Failed to get JobRuntimeRecord from record")
			s.metrics.JobMetrics.JobGetRuntimeFail.Inc(1)
			return nil, err
		}
		runtime, err := record.GetJobRuntime()
		if err != nil {
			return nil, err
		}

		if runtime.GetRevision().GetVersion() < 1 {
			// Older job which does not have changelog.
			// TODO (zhixin): remove this after no more job in the system
			// does not have a changelog version.
			runtime.Revision = &peloton.ChangeLog{
				CreatedAt: uint64(time.Now().UnixNano()),
				UpdatedAt: uint64(time.Now().UnixNano()),
				Version:   1,
			}
		}
		return runtime, nil
	}
	s.metrics.JobMetrics.JobNotFound.Inc(1)
	return nil, fmt.Errorf("Cannot find job wth jobID %v", id.GetValue())
}

// updateJobIndex updates the job index table with job runtime
// and config (if not nil)
func (s *Store) updateJobIndex(
	ctx context.Context,
	id *peloton.JobID,
	runtime *job.RuntimeInfo,
	config *job.JobConfig) error {

	runtimeBuffer, err := json.Marshal(runtime)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Failed to marshal job runtime")
		s.metrics.JobMetrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	completeTime := time.Time{}
	if runtime.GetCompletionTime() != "" {
		completeTime, err = time.Parse(time.RFC3339Nano, runtime.GetCompletionTime())
		if err != nil {
			log.WithField("runtime", runtime).
				WithError(err).
				Warn("Fail to parse completeTime")
		}
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(jobIndexTable).
		Set("runtime_info", runtimeBuffer).
		Set("state", runtime.GetState().String()).
		Set("creation_time", parseTime(runtime.GetCreationTime())).
		Set("completion_time", completeTime).
		Set("update_time", time.Now()).
		Where(qb.Eq{"job_id": id.GetValue()})

	if config != nil {
		// Do not save the instance config with the job
		// configuration in the job_index table.
		instanceConfig := config.GetInstanceConfig()
		config.InstanceConfig = nil
		configBuffer, err := json.Marshal(config)
		config.InstanceConfig = instanceConfig
		if err != nil {
			log.Errorf("Failed to marshal jobConfig, error = %v", err)
			s.metrics.JobMetrics.JobUpdateConfigFail.Inc(1)
			return err
		}

		labelBuffer, err := json.Marshal(config.Labels)
		if err != nil {
			log.Errorf("Failed to marshal labels, error = %v", err)
			s.metrics.JobMetrics.JobUpdateConfigFail.Inc(1)
			return err
		}

		stmt = stmt.Set("config", configBuffer).
			Set("respool_id", config.GetRespoolID().GetValue()).
			Set("owner", config.GetOwningTeam()).
			Set("name", config.GetName()).
			Set("job_type", uint32(config.GetType())).
			Set("instance_count", uint32(config.GetInstanceCount())).
			Set("labels", labelBuffer)
	}

	err = s.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobMetrics.JobUpdateInfoFail.Inc(1)
		return err
	}
	s.metrics.JobMetrics.JobUpdateInfo.Inc(1)
	return nil
}

// UpdateJobRuntime updates the job runtime info
func (s *Store) UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error {
	return s.updateJobRuntimeWithConfig(ctx, id, runtime, nil)
}

// UpdateJobRuntimeWithConfig updates the job runtime info
// including the config (if not nil) in the job_index
func (s *Store) updateJobRuntimeWithConfig(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo, config *job.JobConfig) error {
	runtimeBuffer, err := proto.Marshal(runtime)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobMetrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(jobRuntimeTable).
		Set("state", runtime.GetState().String()).
		Set("update_time", time.Now().UTC()).
		Set("runtime_info", runtimeBuffer).
		Where(qb.Eq{"job_id": id.GetValue()})

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobMetrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	err = s.updateJobIndex(ctx, id, runtime, config)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("updateJobInfoWithJobRuntime failed")
		return err
	}
	s.metrics.JobMetrics.JobUpdateRuntime.Inc(1)
	return nil
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
		} else if property == stateField {
			// return result if not equal, otherwise goto next loop
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
			stateField:
			continue
		}
		return nil, 0, errors.New("Sort only supports fields: state, creation_time")
	}

	sort.Slice(sortedTasksResult, func(i, j int) bool {
		return Less(orderByList, sortedTasksResult[i], sortedTasksResult[j])
	})

	offset := spec.GetPagination().GetOffset()
	limit := _defaultQueryLimit
	if spec.GetPagination() != nil {
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

// DeletePersistentVolume delete persistent volume entry.
func (s *Store) DeletePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(volumeTable).Where(qb.Eq{"volume_id": volumeID.GetValue()})

	err := s.applyStatement(ctx, stmt, volumeID.GetValue())
	if err != nil {
		s.metrics.VolumeMetrics.VolumeDeleteFail.Inc(1)
		return err
	}

	s.metrics.VolumeMetrics.VolumeDelete.Inc(1)
	return nil
}

// CreateUpdate creates a new update entry in DB.
// If it already exists, the create will return an error.
func (s *Store) CreateUpdate(
	ctx context.Context,
	updateInfo *models.UpdateModel,
) error {
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
			"update_options",
			"update_state",
			"instances_total",
			"instances_done",
			"instances_current",
			"job_id",
			"job_config_version",
			"job_config_prev_version",
			"creation_time").
		Values(
			updateInfo.GetUpdateID().GetValue(),
			updateConfigBuffer,
			updateInfo.GetState().String(),
			updateInfo.GetInstancesTotal(),
			0,
			[]int{},
			updateInfo.GetJobID().GetValue(),
			updateInfo.GetJobConfigVersion(),
			updateInfo.GetPrevJobConfigVersion(),
			time.Now()).
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
		if err := s.cleanupPreviousUpdatesForJob(
			ctx,
			updateInfo.GetJobID()); err != nil {
			log.WithError(err).
				WithField("job_id", updateInfo.GetJobID().GetValue()).
				Info("failed to clean up previous updates")
		}
	}()

	s.metrics.UpdateMetrics.UpdateCreate.Inc(1)
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

	// first fetch the updates for the job
	updates, err := s.GetUpdatesForJob(ctx, jobID)
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
			return err
		}

		if len(allResults) <= s.Conf.MaxUpdatesPerJob {
			// nothing to clean up
			return nil
		}

		for _, value := range allResults {
			var record UpdateRecord
			if err := FillObject(value, &record,
				reflect.TypeOf(record)); err != nil {
				log.WithError(err).
					WithField("update_id", updateID.GetValue()).
					Info("failed to fill the update record")
				return err
			}

			// sort as per the job configuration version
			updateInfo := &SortUpdateInfo{
				updateID:         updateID,
				jobConfigVersion: uint64(record.JobConfigVersion),
			}
			updateList = append(updateList, updateInfo)
		}
	}

	sort.Sort(sort.Reverse(SortedUpdateList(updateList)))
	for _, u := range updateList[s.Conf.MaxUpdatesPerJob:] {
		// delete the old job and task configurations, and then the update
		s.DeleteUpdate(ctx, u.updateID, jobID, u.jobConfigVersion)
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
			InstancesTotal:       uint32(record.InstancesTotal),
			InstancesDone:        uint32(record.InstancesDone),
			InstancesCurrent:     record.GetProcessingInstances(),
			CreationTime:         record.CreationTime.Format(time.RFC3339Nano),
			UpdateTime:           record.UpdateTime.Format(time.RFC3339Nano),
		}
		s.metrics.UpdateMetrics.UpdateGet.Inc(1)
		return updateInfo, nil
	}

	s.metrics.UpdateMetrics.UpdateGetFail.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf("update not found")
}

// deleteSingleUpdate deletes a given update from the update_info table.
func (s *Store) deleteSingleUpdate(ctx context.Context, id *peloton.UpdateID) error {
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

	// first delete the task configurations
	stmt := queryBuilder.Delete(taskConfigTable).Where(qb.Eq{
		"job_id": jobID.GetValue(), "version": version})
	if err := s.applyStatement(ctx, stmt, jobID.GetValue()); err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			WithField("version", version).
			Info("failed to delete the task configurations")
		return err
	}

	// next delete the job configuration
	stmt = queryBuilder.Delete(jobConfigTable).Where(qb.Eq{
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

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(updatesTable).
		Set("update_state", updateInfo.GetState().String()).
		Set("instances_done", updateInfo.GetInstancesDone()).
		Set("instances_current", updateInfo.GetInstancesCurrent()).
		Set("update_time", time.Now().UTC()).
		Where(qb.Eq{"update_id": updateInfo.GetUpdateID().GetValue()})

	if err := s.applyStatement(
		ctx,
		stmt,
		updateInfo.GetUpdateID().GetValue()); err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"update_id":             updateInfo.GetUpdateID().GetValue(),
				"update_state":          updateInfo.GetState().String(),
				"update_instances_done": updateInfo.GetInstancesDone(),
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
			InstancesTotal:   uint32(record.InstancesTotal),
			InstancesDone:    uint32(record.InstancesDone),
			InstancesCurrent: record.GetProcessingInstances(),
			UpdateTime:       record.UpdateTime.Format(time.RFC3339Nano),
		}

		s.metrics.UpdateMetrics.UpdateGetProgess.Inc(1)
		return updateInfo, nil
	}

	s.metrics.UpdateMetrics.UpdateGetProgessFail.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf("update not found")
}

// GetUpdatesForJob returns the list of job updates created for a given job.
func (s *Store) GetUpdatesForJob(ctx context.Context,
	jobID *peloton.JobID) ([]*peloton.UpdateID, error) {
	var updateIDs []*peloton.UpdateID

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("update_id").From(updatesByJobView).
		Where(qb.Eq{"job_id": jobID.GetValue()})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", jobID.GetValue()).
			Info("failed to fetch updates for a given job")
		s.metrics.UpdateMetrics.UpdateGetForJobFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record UpdateRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", jobID.GetValue()).
				Info("failed to fill update record for the job")
			s.metrics.UpdateMetrics.UpdateGetForJobFail.Inc(1)
			return nil, err
		}

		updateIDs = append(updateIDs,
			&peloton.UpdateID{Value: record.UpdateID.String()})
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

func (s *Store) getJobSummaryFromLuceneResult(ctx context.Context, allResults []map[string]interface{}) ([]*job.JobSummary, error) {
	var summaryResults []*job.JobSummary
	for _, value := range allResults {
		summary := &job.JobSummary{}
		id, ok := value["job_id"].(qb.UUID)
		if !ok {
			log.WithField("job_id", value).
				Error("fail to find job_id in query result")
			return nil, fmt.Errorf("got invalid response from cassandra")
		}
		summary.Id = &peloton.JobID{
			Value: id.String(),
		}

		name, ok := value["name"].(string)
		if !ok {
			log.WithField("name", value["name"]).
				Info("failed to cast name to string")
		}
		if name == "" {
			// In case of an older job, the "name" column is not populated
			// in jobIndexTable. This is a rudimentary assumption,
			// unfortunately because jobIndexTable doesn't have versioning.
			// In this case, get summary from jobconfig
			// and move on to the next job entry.
			// TODO (adityacb): remove this code block when we
			// start archiving older jobs and no longer hit this case.
			log.WithField("job_id", id).
				Debug("empty name in job_index, get job summary from job config")
			summary, err := s.getJobSummaryFromConfig(ctx, summary.Id)
			if err != nil {
				log.WithError(err).
					WithField("job_id", id).
					Error("failed to get job summary from job config")
				return nil, fmt.Errorf("failed to get job summary from job config")
			}
			summaryResults = append(summaryResults, summary)
			continue
		} else {
			summary.Name = name
		}

		runtimeInfo, ok := value["runtime_info"].(string)
		if ok {
			err := json.Unmarshal([]byte(runtimeInfo), &summary.Runtime)
			if err != nil {
				log.WithError(err).
					WithField("runtime_info", runtimeInfo).
					Info("failed to unmarshal runtime info")
			}
		} else {
			log.WithField("runtime_info", value["runtime_info"]).
				Info("failed to cast runtime_info to string")
		}

		summary.OwningTeam, ok = value["owner"].(string)
		if !ok {
			log.WithField("owner", value["owner"]).
				Info("failed to cast owner to string")
		}
		summary.Owner = summary.OwningTeam

		instcnt, ok := value["instance_count"].(int)
		if !ok {
			log.WithField("instance_count", value["instance_count"]).
				Info("failed to cast instance_count to uint32")
		} else {
			summary.InstanceCount = uint32(instcnt)
		}

		jobType, ok := value["job_type"].(int)
		if !ok {
			log.WithField("job_type", value["job_type"]).
				Info("failed to cast job_type to uint32")
		} else {
			summary.Type = job.JobType(jobType)
		}

		respoolIDStr, ok := value["respool_id"].(string)
		if !ok {
			log.WithField("respool_id", value["respool_id"]).
				Info("failed to cast respool_id to string")
		} else {
			summary.RespoolID = &peloton.ResourcePoolID{
				Value: respoolIDStr,
			}
		}

		labelBuffer, ok := value["labels"].(string)
		if ok {
			err := json.Unmarshal([]byte(labelBuffer), &summary.Labels)
			if err != nil {
				log.WithError(err).
					WithField("labels", labelBuffer).
					Info("failed to unmarshal labels")
			}
		} else {
			log.WithField("labels", value["labels"]).
				Info("failed to cast labels to string")
		}

		summaryResults = append(summaryResults, summary)
	}
	return summaryResults, nil
}

func (s *Store) getJobSummaryFromConfig(ctx context.Context, id *peloton.JobID) (*job.JobSummary, error) {
	summary := &job.JobSummary{}
	jobConfig, err := s.GetJobConfig(ctx, id)
	if err != nil {
		log.WithError(err).
			Info("failed to get jobconfig")
		return nil, err
	}
	summary.Name = jobConfig.GetName()
	summary.Id = id
	summary.Type = jobConfig.GetType()
	summary.Owner = jobConfig.GetOwningTeam()
	summary.OwningTeam = jobConfig.GetOwningTeam()
	summary.Labels = jobConfig.GetLabels()
	summary.InstanceCount = jobConfig.GetInstanceCount()
	summary.RespoolID = jobConfig.GetRespoolID()
	summary.Runtime, err = s.GetJobRuntime(ctx, id)
	if err != nil {
		log.WithError(err).
			Info("failed to get job runtime")
		return nil, err
	}
	return summary, nil
}

// CreateSecret stores a secret in the secret_info table
// DO NOT LOG SECRETS in this function
func (s *Store) CreateSecret(
	ctx context.Context,
	secret *peloton.Secret,
	id *peloton.JobID,
) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(secretInfoTable).
		Columns(
			"secret_id",
			"job_id",
			"path",
			"data",
			"creation_time",
			"version",
			"valid").
		Values(
			secret.GetId().GetValue(),
			id.GetValue(),
			secret.GetPath(),
			secret.GetValue().GetData(),
			time.Now().UTC(),
			secretVersion0,
			secretValid,
		).
		IfNotExist()
	err := s.applyStatement(ctx, stmt, id.GetValue())
	if err != nil {
		log.WithError(err).
			WithFields(log.Fields{
				"job_id":    id.GetValue(),
				"secret_id": secret.GetId().GetValue(),
			}).
			Error("create secret failed")
		s.metrics.SecretMetrics.SecretCreateFail.Inc(1)
		return err
	}
	s.metrics.SecretMetrics.SecretCreate.Inc(1)
	return nil
}

// UpdateSecret updates a secret in the secret_info table
// DO NOT LOG SECRETS in this function
func (s *Store) UpdateSecret(
	ctx context.Context,
	secret *peloton.Secret,
) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Update(secretInfoTable).
		Set("data", secret.GetValue().GetData()).
		Set("path", secret.GetPath()).
		Where(qb.Eq{"secret_id": secret.GetId().GetValue(),
			"valid": secretValid})

	err := s.applyStatement(ctx, stmt, secret.GetId().GetValue())
	if err != nil {
		log.WithError(err).
			WithField("secret_id", secret.GetId().GetValue()).
			Error("create secret failed")
		s.metrics.SecretMetrics.SecretUpdateFail.Inc(1)
		return err
	}
	s.metrics.SecretMetrics.SecretUpdate.Inc(1)
	return nil
}

// GetSecret gets a secret from the secret_info table
// DO NOT LOG SECRETS in this function
func (s *Store) GetSecret(ctx context.Context, id *peloton.SecretID) (*peloton.Secret, error) {
	queryBuilder := s.DataStore.NewQuery()

	stmt := queryBuilder.Select(
		"path",
		"data").
		From(secretInfoTable).
		Where(qb.Eq{
			"secret_id": id.GetValue(),
			"valid":     secretValid,
		})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		s.metrics.SecretMetrics.SecretGetFail.Inc(1)
		return nil, err
	}
	return s.createSecretFromResults(id, allResults)
}

// createSecretFromResults will create a peloton secret
// proto message from the cql query results (from cql query
// to secret_info table)
func (s *Store) createSecretFromResults(
	id *peloton.SecretID,
	allResults []map[string]interface{},
) (*peloton.Secret, error) {
	// allResults contain the cql query results after querying
	// secret_info table.

	// One secret id should have only one row (one secret)
	// associated with it.
	if len(allResults) > 1 {
		s.metrics.SecretMetrics.SecretGetFail.Inc(1)
		return nil, yarpcerrors.AlreadyExistsErrorf(
			"found %d secrets for secret id %v",
			len(allResults), id.GetValue())
	}

	// loop through the results and construct peloton secret
	for _, value := range allResults {
		// key 'data' in the map should contain secret data
		dataStr, ok := value["data"].(string)
		if !ok {
			s.metrics.SecretMetrics.SecretGetFail.Inc(1)
			return nil, yarpcerrors.InternalErrorf(
				"could not retrieve secret data for id %v",
				id.GetValue())
		}
		// key 'path' in the map should contain secret path
		pathStr, ok := value["path"].(string)
		if !ok {
			s.metrics.SecretMetrics.SecretGetFail.Inc(1)
			return nil, yarpcerrors.InternalErrorf(
				"could not retrieve secret path for id %v",
				id.GetValue())
		}
		s.metrics.SecretMetrics.SecretGet.Inc(1)
		// construct the peloton secret proto message and return
		return &peloton.Secret{
			Id:   id,
			Path: pathStr,
			Value: &peloton.Secret_Value{
				Data: []byte(dataStr),
			},
		}, nil
	}
	// If allResults doesn't have any rows, it means
	// that the secret was not found. Send an error back.
	s.metrics.SecretMetrics.SecretNotFound.Inc(1)
	return nil, yarpcerrors.NotFoundErrorf(
		"Cannot find secret wth id %v",
		id.GetValue())
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
