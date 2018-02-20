package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/query"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/update"
	pb_volume "code.uber.internal/infra/peloton/.gen/peloton/api/volume"

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
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	taskIDFmt             = "%s-%d"
	jobConfigTable        = "job_config"
	jobRuntimeTable       = "job_runtime"
	jobIndexTable         = "job_index"
	taskConfigTable       = "task_config"
	taskRuntimeTable      = "task_runtime"
	taskStateChangesTable = "task_state_changes"
	updatesTable          = "update_info"
	frameworksTable       = "frameworks"
	taskJobStateView      = "mv_task_by_state"
	jobByStateView        = "mv_job_by_state"
	resPoolsTable         = "respools"
	resPoolsOwnerView     = "mv_respools_by_owner"
	volumeTable           = "persistent_volumes"

	_defaultQueryLimit    uint32 = 10
	_defaultQueryMaxLimit uint32 = 100

	// _defaultTaskConfigID is used for storing, and retrieving, the default
	// task configuration, when no specific is available.
	_defaultTaskConfigID = -1
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
}

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

// Store implements JobStore using a cassandra backend
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

func (s *Store) createJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, version int64, owner string) error {
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
	// TODO set correct version
	if jobConfig.GetDefaultConfig() != nil {
		if err := s.createTaskConfig(ctx, id, _defaultTaskConfigID, jobConfig.GetDefaultConfig(), 0); err != nil {
			return err
		}
	}

	for instanceID, cfg := range jobConfig.GetInstanceConfig() {
		merged := taskconfig.Merge(jobConfig.GetDefaultConfig(), cfg)
		// TODO set correct version
		if err := s.createTaskConfig(ctx, id, int64(instanceID), merged, 0); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) createTaskConfig(ctx context.Context, id *peloton.JobID, instanceID int64, taskConfig *task.TaskConfig, version int) error {
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

// CreateJob creates a job with the job id and the config value
func (s *Store) CreateJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, owner string) error {
	// Create version 0 of the job config.
	if err := s.createJobConfig(ctx, id, jobConfig, 0, owner); err != nil {
		return err
	}

	var goalState job.JobState
	switch jobConfig.Type {
	case job.JobType_BATCH:
		goalState = job.JobState_SUCCEEDED
	default:
		goalState = job.JobState_RUNNING
	}

	initialJobRuntime := job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: time.Now().UTC().Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		GoalState:    goalState,
	}
	// Init the task stats to reflect that all tasks are in initialized state
	initialJobRuntime.TaskStats[task.TaskState_INITIALIZED.String()] = jobConfig.InstanceCount

	// Create the initial job runtime record
	err := s.updateJobRuntimeWithConfig(ctx, id, &initialJobRuntime, jobConfig)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("CreateJobRuntime failed")
		s.metrics.JobMetrics.JobCreateRuntimeFail.Inc(1)
		return err
	}
	s.metrics.JobMetrics.JobCreateRuntime.Inc(1)
	return nil
}

func (s *Store) getMaxJobVersion(ctx context.Context, id *peloton.JobID) (int64, error) {
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
			return max.(int64), nil
		}
	}
	return 0, nil
}

// UpdateJobConfig updates a job with the job id and the config value
func (s *Store) UpdateJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	version, err := s.getMaxJobVersion(ctx, id)
	if err != nil {
		s.metrics.JobMetrics.JobCreateConfigFail.Inc(1)
		return err
	}

	// Increment version.
	version++

	if err := s.createJobConfig(ctx, id, jobConfig, version, "<missing owner>"); err != nil {
		return err
	}

	r, err := s.GetJobRuntime(ctx, id)
	if err != nil {
		return err
	}

	// Update to use new version.
	r.ConfigVersion = version

	return s.updateJobRuntimeWithConfig(ctx, id, r, jobConfig)
}

// GetJobConfig returns a job config given the job id
func (s *Store) GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error) {
	r, err := s.GetJobRuntime(ctx, id)
	if err != nil {
		return nil, err
	}

	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("config").From(jobConfigTable).
		Where(qb.Eq{"job_id": jobID, "version": r.ConfigVersion})
	stmtString, _, _ := stmt.ToSQL()
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to execute stmt %v, err=%v", stmtString, err)
		s.metrics.JobMetrics.JobGetFail.Inc(1)
		return nil, err
	}

	if len(allResults) > 1 {
		s.metrics.JobMetrics.JobGetFail.Inc(1)
		return nil, fmt.Errorf("found %d jobs %v for job id %v", len(allResults), allResults, jobID)
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
		return record.GetJobConfig()
	}
	s.metrics.JobMetrics.JobNotFound.Inc(1)
	return nil, fmt.Errorf("Cannot find job wth jobID %v", jobID)
}

// QueryJobs returns all jobs in the resource pool that matches the spec.
func (s *Store) QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec) ([]*job.JobInfo, []*job.JobSummary, uint32, error) {
	// Query is based on stratio lucene index on jobs.
	// See https://github.com/Stratio/cassandra-lucene-index
	// We are using "must" for the labels and only return the jobs that contains all
	// label values
	// TODO: investigate if there are any golang library that can build lucene query

	var clauses []string

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
	if len(spec.JobStates) > 0 {
		values := ""
		for i, s := range spec.JobStates {
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

	summaryResults, err := s.getJobSummaryFromLuceneResult(ctx, allResults)
	if err != nil {
		// Suppress this error until we get rid of JobInfo
		// Just log warning and increment query fail metric
		uql, args, _, _ := stmt.ToUql()
		log.WithField("uql", uql).
			WithField("args", args).
			WithField("labels", spec.GetLabels()).
			WithError(err).
			Warn("Failed to get JobSummary")
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
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

	stmt := queryBuilder.Select("job_id").From(jobByStateView).
		Where(qb.Eq{"state": jobStates})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.WithError(err).
			Error("GetJobsByStates failed")
		s.metrics.JobMetrics.JobGetByStatesFail.Inc(1)
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
			return nil, err
		}
		jobs = append(jobs, peloton.JobID{Value: record.JobID.String()})
	}
	s.metrics.JobMetrics.JobGetByStates.Inc(1)
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
// TODO: remove this in favor of CreateTaskRuntimes
func (s *Store) CreateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo, owner string) error {
	now := time.Now()
	runtime.Revision = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   0,
	}

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
			runtimeBuffer).
		IfNotExist()

	taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
	if err := s.applyStatement(ctx, stmt, taskID); err != nil {
		s.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return err
	}
	s.metrics.TaskMetrics.TaskCreate.Inc(1)
	// Track the task events
	err = s.logTaskStateChange(ctx, jobID, instanceID, runtime)
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

					now := time.Now()
					runtime.Revision = &peloton.ChangeLog{
						CreatedAt: uint64(now.UnixNano()),
						UpdatedAt: uint64(now.UnixNano()),
						Version:   0,
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

// logTaskStateChange logs the task state change events
func (s *Store) logTaskStateChange(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error {
	var stateChange = TaskStateChangeRecord{
		JobID:       jobID.GetValue(),
		InstanceID:  instanceID,
		MesosTaskID: runtime.GetMesosTaskId().GetValue(),
		TaskState:   runtime.GetState().String(),
		TaskHost:    runtime.GetHost(),
		EventTime:   time.Now().UTC(),
		Reason:      runtime.GetReason(),
		Message:     runtime.GetMessage(),
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
			JobID:       jobID,
			InstanceID:  uint32(instanceID),
			TaskState:   runtime.GetState().String(),
			TaskHost:    runtime.GetHost(),
			EventTime:   time.Now().UTC(),
			MesosTaskID: runtime.GetMesosTaskId().GetValue(),
			Reason:      runtime.GetReason(),
			Message:     runtime.GetMessage(),
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
				Timestamp: record.EventTime.String(),
				Message:   record.Message,
				Reason:    record.Reason,
				Hostname:  record.TaskHost,
				// TaskId here will contain Mesos TaskId
				TaskId: &peloton.TaskID{
					Value: record.MesosTaskID,
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
	instanceID uint32, version int64) (*task.TaskConfig, error) {
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
	instanceIDs []uint32, version int64) (map[uint32]*task.TaskConfig, error) {
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
		int64(runtime.ConfigVersion))
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

// GetTasksForJobAndState returns the tasks for a peloton job with certain state.
// result map key is TaskID, value is TaskHost
func (s *Store) GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	jobID := id.GetValue()
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("instance_id").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": state})
	allResults, err := s.executeRead(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobAndState by jobId %v state %v, err=%v", jobID, state, err)
		s.metrics.TaskMetrics.TaskGetForJobAndStateFail.Inc(1)
		return nil, err
	}
	resultMap := make(map[uint32]*task.TaskInfo)

	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskMetrics.TaskGetForJobAndStateFail.Inc(1)
			return nil, err
		}
		resultMap[uint32(record.InstanceID)], err = s.getTask(ctx, id.GetValue(), uint32(record.InstanceID))
		if err != nil {
			log.Errorf("Failed to get taskInfo from task, val = %v err= %v", value, err)
			s.metrics.TaskMetrics.TaskGetForJobAndStateFail.Inc(1)
			return nil, err
		}
		s.metrics.TaskMetrics.TaskGetForJobAndState.Inc(1)
	}
	s.metrics.TaskMetrics.TaskGetForJobAndState.Inc(1)
	return resultMap, nil
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
		Where(qb.Eq{"job_id": jobID}).
		Where("instance_id >= ?", instanceRange.From).
		Where("instance_id < ?", instanceRange.To)

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
			int64(configVersion))
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

					// Bump version of task.
					if runtime.Revision == nil {
						runtime.Revision = &peloton.ChangeLog{}
					}
					runtime.Revision.Version++
					runtime.Revision.UpdatedAt = uint64(time.Now().UnixNano())

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

					// TBD use of CAS

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

	stmt = queryBuilder.Delete(jobRuntimeTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		s.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	stmt = queryBuilder.Delete(jobConfigTable).Where(qb.Eq{"job_id": id.GetValue()})
	err := s.applyStatement(ctx, stmt, id.GetValue())
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
	log.Debugf("stmt=%v", stmtString)
	result, err := s.executeWrite(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to execute stmt for %v %v, err=%v", itemName, stmtString, err)
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

func getTaskID(taskInfo *task.TaskInfo) string {
	jobID := taskInfo.JobId.GetValue()
	return fmt.Sprintf(taskIDFmt, jobID, taskInfo.InstanceId)
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
		return record.GetJobRuntime()
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

// QueryTasks returns all tasks in the given offset..offset+limit range.
func (s *Store) QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error) {
	jobConfig, err := s.GetJobConfig(ctx, id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("Failed to get jobConfig")
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, 0, err
	}
	offset := spec.GetPagination().GetOffset()
	limit := _defaultQueryLimit
	if spec.GetPagination() != nil {
		limit = spec.GetPagination().GetLimit()
	}
	end := offset + limit
	if end > jobConfig.InstanceCount {
		end = jobConfig.InstanceCount
	}
	tasks, err := s.GetTasksForJobByRange(ctx, id, &task.InstanceRange{
		From: offset,
		To:   end,
	})
	if err != nil {
		s.metrics.JobMetrics.JobQueryFail.Inc(1)
		return nil, 0, err
	}

	var result []*task.TaskInfo
	for i := offset; i < end; i++ {
		result = append(result, tasks[i])
	}
	s.metrics.JobMetrics.JobQuery.Inc(1)
	return result, jobConfig.InstanceCount, nil
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

// CreateUpdate creates a new entry in Cassandra, if it doesn't already exist.
func (s *Store) CreateUpdate(ctx context.Context, id *update.UpdateID, jobID *peloton.JobID, jobConfig *job.JobConfig, updateConfig *update.UpdateConfig) error {
	updateConfigBuffer, err := json.Marshal(updateConfig)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			WithField("job_id", jobID.GetValue()).
			Error("failed to marshal update config")
		return err
	}

	jobConfigBuffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			WithField("job_id", jobID.GetValue()).
			Error("failed to marshal job config")
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(updatesTable).
		Columns(
			"update_id",
			"update_config",
			"state",
			"instances_total",
			"instances_done",
			"instances_current",
			"job_id",
			"job_config",
			"creation_time").
		Values(
			id.GetValue(),
			string(updateConfigBuffer),
			update.State_ROLLING_FORWARD,
			jobConfig.GetInstanceCount(),
			0,
			[]int{},
			jobID.GetValue(),
			jobConfigBuffer,
			time.Now()).
		IfNotExist()

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			WithField("job_id", jobID.GetValue()).
			Error("create update in DB failed")
		return err
	}

	return nil
}

// GetUpdateProgress returns the list of tasks being process as well as the
// overall progress of the update.
func (s *Store) GetUpdateProgress(ctx context.Context, id *update.UpdateID) ([]uint32, uint32, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("instances_current", "instances_done").From(updatesTable).Where(qb.Eq{"update_id": id.GetValue()})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			Error("get update progress from db failed")
		return nil, 0, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			WithField("update_id", id.GetValue()).
			Error("get update process all results failed")
		return nil, 0, err
	}

	for _, value := range allResults {
		var record UpdateRecord
		if err := FillObject(value, &record, reflect.TypeOf(record)); err != nil {
			return nil, 0, err
		}
		return record.GetProcessingInstances(), uint32(record.InstancesDone), nil
	}
	return nil, 0, fmt.Errorf("cannot find update with ID %v", id.GetValue())
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
