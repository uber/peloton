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
	"code.uber.internal/infra/peloton/.gen/peloton/api/upgrade"
	pb_volume "code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra/api"
	qb "code.uber.internal/infra/peloton/storage/querybuilder"

	"code.uber.internal/infra/peloton/storage/cassandra/impl"
	"code.uber.internal/infra/peloton/util"
	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
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
	upgradesTable         = "upgrades"
	frameworksTable       = "frameworks"
	taskJobStateView      = "mv_task_by_state"
	jobByStateView        = "mv_job_by_state"
	resPools              = "respools"
	resPoolsOwnerView     = "mv_respools_by_owner"
	volumeTable           = "persistent_volumes"

	_defaultQueryLimit uint32 = 10

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
	errors, ok := migrate.UpSync(connString, c.Migrations)
	log.Infof("UpSync complete")
	if !ok {
		log.Errorf("UpSync failed with errors: %v", errors)
		return errors
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
	connStr = strings.Replace(connStr, " ", "", -1)
	log.Infof("Cassandra migration string %v", connStr)
	return connStr
}

// Store implements JobStore using a cassandra backend
type Store struct {
	DataStore api.DataStore
	metrics   storage.Metrics
	Conf      *Config
}

// NewStore creates a Store
func NewStore(config *Config, scope tally.Scope) (*Store, error) {
	dataStore, err := impl.CreateStore(config.CassandraConn, config.StoreName, scope)
	if err != nil {
		log.Errorf("Failed to NewStore, err=%v", err)
		return nil, err
	}
	return &Store{
		DataStore: dataStore,
		metrics:   storage.NewMetrics(scope.SubScope("storage")),
		Conf:      config,
	}, nil
}

func (s *Store) createJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, version int64, owner string) error {
	jobID := id.Value
	configBuffer, err := proto.Marshal(jobConfig)
	if err != nil {
		log.Errorf("Failed to marshal jobConfig, error = %v", err)
		s.metrics.JobCreateFail.Inc(1)
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
			WithField("job_id", id.Value).
			Error("createJobConfig failed")
		s.metrics.JobCreateFail.Inc(1)
		return err
	}

	// TODO: Create TaskConfig here. This requires the task configs to be
	// flattened at this point, see jobmgr.

	return nil
}

func (s *Store) createTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version int, taskConfig *task.TaskConfig) error {
	configBuffer, err := proto.Marshal(taskConfig)
	if err != nil {
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
			WithField("job_id", id.Value).
			Error("createTaskConfig failed")
		s.metrics.JobCreateFail.Inc(1)
		return err
	}

	return nil
}

// CreateJob creates a job with the job id and the config value
func (s *Store) CreateJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, owner string) error {
	// Create version 0 of the job config.
	if err := s.createJobConfig(ctx, id, jobConfig, 0, owner); err != nil {
		return err
	}

	initialJobRuntime := job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: time.Now().UTC().Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
	}
	// Init the task stats to reflect that all tasks are in initialized state
	initialJobRuntime.TaskStats[task.TaskState_INITIALIZED.String()] = jobConfig.InstanceCount

	// Create the initial job runtime record
	err := s.updateJobRuntimeWithConfig(ctx, id, &initialJobRuntime, jobConfig)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("UpdateJobRuntime failed")
		s.metrics.JobCreateFail.Inc(1)
		return err
	}
	s.metrics.JobCreate.Inc(1)
	return nil
}

func (s *Store) getMaxJobVersion(ctx context.Context, id *peloton.JobID) (int64, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("MAX(version)").From(jobConfigTable).
		Where(qb.Eq{"job_id": id.GetValue()})

	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to get max version of job %v: %v", id.GetValue(), err)
		return 0, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
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

	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("config").From(jobConfigTable).
		Where(qb.Eq{"job_id": jobID, "version": r.ConfigVersion})
	stmtString, _, _ := stmt.ToSQL()
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to execute stmt %v, err=%v", stmtString, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to get all results for stmt %v, err=%v", stmtString, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	log.Debugf("all result = %v", allResults)
	if len(allResults) > 1 {
		s.metrics.JobGetFail.Inc(1)
		return nil, fmt.Errorf("found %d jobs %v for job id %v", len(allResults), allResults, jobID)
	}
	for _, value := range allResults {
		var record JobConfigRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into JobRecord, err= %v", err)
			s.metrics.JobGetFail.Inc(1)
			return nil, err
		}
		s.metrics.JobGet.Inc(1)
		return record.GetJobConfig()
	}
	s.metrics.JobNotFound.Inc(1)
	return nil, fmt.Errorf("Cannot find job wth jobID %v", jobID)
}

// QueryJobs returns all jobs in the resource pool that matches the spec.
func (s *Store) QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec) ([]*job.JobInfo, uint32, error) {
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
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"config", values:%s}`, strconv.Quote(word)))
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

	where := `expr(job_index_lucene, '{filter: [`
	for i, c := range clauses {
		if i > 0 {
			where += ", "
		}
		where += c
	}
	where += "]"

	// add sorter into the query in case orderby is specified in the query spec
	var orderBy = spec.GetPagination().GetOrderBy()
	if orderBy != nil && len(orderBy) > 0 {
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
		}
		where += "]"
	}
	where += "}')"

	log.WithField("where", where).Debug("query string")

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("job_id").From(jobIndexTable)

	if len(clauses) > 0 {
		stmt = stmt.Where(where)
	}

	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithField("labels", spec.GetLabels()).
			WithError(err).
			Error("Fail to Query jobs")
		s.metrics.JobQueryFail.Inc(1)
		return nil, 0, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithField("labels", spec.GetLabels()).
			WithError(err).
			Error("Fail to Query jobs")
		s.metrics.JobQueryFail.Inc(1)
		return nil, 0, err
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
		end = spec.GetPagination().GetLimit()
	}
	if end > uint32(len(allResults)) {
		end = uint32(len(allResults))
	}
	allResults = allResults[:end]

	var results []*job.JobInfo
	for _, value := range allResults {
		id, ok := value["job_id"].(qb.UUID)
		if !ok {
			log.WithField("labels", spec.GetLabels()).
				WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, 0, fmt.Errorf("got invalid response from cassandra")
		}

		jobID := &peloton.JobID{
			Value: id.String(),
		}

		jobRuntime, err := s.GetJobRuntime(ctx, jobID)
		if err != nil {
			log.WithError(err).WithField("job_id", id.String()).Warnf("no job runtime found when executing jobs query")
			continue
		}

		jobConfig, err := s.GetJobConfig(ctx, jobID)
		if err != nil {
			log.WithField("labels", spec.GetLabels()).
				WithError(err).
				Error("Fail to Query jobs")
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

	s.metrics.JobQuery.Inc(1)
	return results, total, nil
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
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			Error("GetJobsByStates failed")
		s.metrics.JobGetByStatesFail.Inc(1)
		return nil, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			Error("GetJobsByStates get all results failed")
		s.metrics.JobGetByStatesFail.Inc(1)
		return nil, err
	}

	var jobs []peloton.JobID
	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Failed to get JobRuntimeRecord from record")
			s.metrics.JobGetByStatesFail.Inc(1)
			return nil, err
		}
		jobs = append(jobs, peloton.JobID{Value: record.JobID.String()})
	}
	s.metrics.JobGetByStates.Inc(1)
	return jobs, nil
}

// GetAllJobs returns all jobs
// TODO: introduce jobstate and add GetJobsByState
func (s *Store) GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error) {
	// TODO: Get jobs from  all edge respools
	var resultMap = make(map[string]*job.RuntimeInfo)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobRuntimeTable)
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			Error("Fail to Query all jobs")
		s.metrics.JobQueryFail.Inc(1)
		return nil, err
	}
	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			Error("Fail to Query jobs")
		s.metrics.JobQueryFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, err
		}
		jobRuntime, err := record.GetJobRuntime()
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, err
		}
		resultMap[record.JobID.String()] = jobRuntime
	}
	s.metrics.JobQueryAll.Inc(1)
	return resultMap, nil
}

// CreateTask creates a task for a peloton job
// TODO: remove this in favor of CreateTasks
func (s *Store) CreateTask(ctx context.Context, id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, owner string) error {
	jobID := id.Value
	if taskInfo.InstanceId != instanceID || taskInfo.JobId.Value != jobID {
		errMsg := fmt.Sprintf("Task has instance id %v, different than the instanceID %d expected, jobID %v, task JobId %v",
			instanceID, taskInfo.InstanceId, jobID, taskInfo.JobId.Value)
		log.Errorf(errMsg)
		s.metrics.TaskCreateFail.Inc(1)
		return fmt.Errorf(errMsg)
	}

	// TODO: Set correct version.
	if err := s.createTaskConfig(ctx, id, instanceID, 0, taskInfo.GetConfig()); err != nil {
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}

	now := time.Now()
	taskInfo.Runtime.Revision = &peloton.ChangeLog{
		CreatedAt: uint64(now.UnixNano()),
		UpdatedAt: uint64(now.UnixNano()),
		Version:   0,
	}

	runtimeBuffer, err := proto.Marshal(taskInfo.Runtime)
	if err != nil {
		log.WithField("job_id", taskInfo.JobId.Value).
			WithField("instance_id", taskInfo.InstanceId).
			WithError(err).
			Error("Failed to create task runtime")
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}

	taskInfo.Runtime.State = task.TaskState_INITIALIZED
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
			taskInfo.GetRuntime().GetRevision().GetVersion(),
			time.Now().UTC(),
			taskInfo.GetRuntime().GetState().String(),
			runtimeBuffer).
		IfNotExist()

	taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
	if err := s.applyStatement(ctx, stmt, taskID); err != nil {
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}
	s.metrics.TaskCreate.Inc(1)
	// Track the task events
	err = s.logTaskStateChange(ctx, id, instanceID, taskInfo)
	if err != nil {
		log.Errorf("Unable to log task state changes for job ID %v instance %v, error = %v", jobID, instanceID, err)
		return err
	}
	return nil
}

// CreateTasks creates tasks for the given slice of task infos, instances 0..n
func (s *Store) CreateTasks(ctx context.Context, id *peloton.JobID, taskInfos []*task.TaskInfo, owner string) error {
	maxBatchSize := uint32(s.Conf.MaxBatchSize)
	maxParallelBatches := uint32(s.Conf.MaxParallelBatches)
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxUint32
	}
	jobID := id.Value
	nTasks := uint32(len(taskInfos))
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
				idsToTaskInfos := map[string]*task.TaskInfo{}
				log.WithField("id", id.Value).
					WithField("start", start).
					WithField("end", end).
					Debug("creating tasks")
				for i := start; i < end; i++ {
					t := taskInfos[i]

					// TODO: Write task config as part of writing job config..
					if err := s.createTaskConfig(ctx, id, t.InstanceId, 0, t.GetConfig()); err != nil {
						log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
							Errorf("Writing %d tasks (%d:%d) for %v to Cassandra failed in %v with %v", batchSize, start, end-1, id.Value, time.Since(batchTimeStart), err)
						s.metrics.TaskCreateFail.Inc(int64(nTasks))
						atomic.AddUint32(&tasksNotCreated, batchSize)
						return
					}

					now := time.Now()
					t.Runtime.Revision = &peloton.ChangeLog{
						CreatedAt: uint64(now.UnixNano()),
						UpdatedAt: uint64(now.UnixNano()),
						Version:   0,
					}

					t.Runtime.State = task.TaskState_INITIALIZED
					taskID := fmt.Sprintf(taskIDFmt, jobID, t.InstanceId)

					idsToTaskInfos[taskID] = t

					runtimeBuffer, err := proto.Marshal(t.Runtime)
					if err != nil {
						log.WithField("job_id", t.JobId.Value).
							WithField("instance_id", t.InstanceId).
							WithError(err).
							Error("Failed to create task runtime")
						s.metrics.TaskCreateFail.Inc(int64(nTasks))
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
							t.InstanceId,
							t.GetRuntime().GetRevision().GetVersion(),
							time.Now().UTC(),
							t.GetRuntime().GetState().String(),
							runtimeBuffer)

					// IfNotExist() will cause Writing 20 tasks (0:19) for TestJob2 to Cassandra failed in 8.756852ms with
					// Batch with conditions cannot span multiple partitions. For now, drop the IfNotExist()

					insertStatements = append(insertStatements, stmt)
				}
				err := s.applyStatements(ctx, insertStatements, jobID)
				if err != nil {
					log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
						Errorf("Writing %d tasks (%d:%d) for %v to Cassandra failed in %v with %v", batchSize, start, end-1, id.Value, time.Since(batchTimeStart), err)
					s.metrics.TaskCreateFail.Inc(int64(nTasks))
					atomic.AddUint32(&tasksNotCreated, batchSize)
					return
				}
				log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
					Debugf("Wrote %d tasks (%d:%d) for %v to Cassandra in %v", batchSize, start, end-1, id.Value, time.Since(batchTimeStart))
				s.metrics.TaskCreate.Inc(int64(nTasks))

				err = s.logTaskStateChanges(ctx, idsToTaskInfos)
				if err != nil {
					log.Errorf("Unable to log task state changes for job ID %v range(%d:%d), error = %v", jobID, start, end-1, err)
				}
			}
		}()
	}
	wg.Wait()

	if tasksNotCreated != 0 {
		msg := fmt.Sprintf(
			"Wrote %d tasks for %v, and was unable to write %d tasks to Cassandra in %v",
			nTasks-tasksNotCreated,
			id,
			tasksNotCreated,
			time.Since(timeStart))
		log.Errorf(msg)
		return fmt.Errorf(msg)
	}

	log.WithField("duration_s", time.Since(timeStart).Seconds()).
		Infof("Wrote all %d tasks for %v to Cassandra in %v", nTasks, id, time.Since(timeStart))
	return nil

}

// logTaskStateChange logs the task state change events
func (s *Store) logTaskStateChange(ctx context.Context, jobID *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo) error {
	var stateChange = TaskStateChangeRecord{
		JobID:      jobID.GetValue(),
		InstanceID: instanceID,
		TaskState:  taskInfo.Runtime.State.String(),
		TaskHost:   taskInfo.Runtime.Host,
		EventTime:  time.Now().UTC(),
	}
	buffer, err := json.Marshal(stateChange)
	if err != nil {
		log.Errorf("Failed to marshal stateChange, error = %v", err)
		return err
	}
	stateChangePart := []string{string(buffer)}
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(taskStateChangesTable).
		Add("events", stateChangePart).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if result != nil {
		defer result.Close()
	}
	if err != nil {
		log.Errorf("Fail to logTaskStateChange by jobID %v, instanceID %v %v, err=%v", jobID, instanceID, stateChangePart, err)
		return err
	}
	return nil
}

// logTaskStateChanges logs multiple task state change events in a batch operation (one RPC, separate statements)
// taskIDToTaskInfos is a map of task ID to task info
func (s *Store) logTaskStateChanges(ctx context.Context, taskIDToTaskInfos map[string]*task.TaskInfo) error {
	statements := []api.Statement{}
	for taskID, taskInfo := range taskIDToTaskInfos {
		jobID, instanceID, err := util.ParseTaskID(taskID)
		if err != nil {
			log.WithError(err).
				WithField("task_id", taskID).
				Error("Invalid task id")
			return err
		}
		runtime := taskInfo.GetRuntime()
		stateChange := TaskStateChangeRecord{
			JobID:       jobID,
			InstanceID:  uint32(instanceID),
			TaskState:   runtime.GetState().String(),
			TaskHost:    runtime.GetHost(),
			EventTime:   time.Now().UTC(),
			MesosTaskID: runtime.GetMesosTaskId().GetValue(),
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
	}
	err := s.DataStore.ExecuteBatch(ctx, statements)
	if err != nil {
		log.Errorf("Fail to logTaskStateChanges for %d tasks, err=%v", len(taskIDToTaskInfos), err)
		return err
	}
	return nil
}

// GetTaskStateChanges returns the state changes for a task
func (s *Store) GetTaskStateChanges(ctx context.Context, jobID *peloton.JobID, instanceID uint32) ([]*TaskStateChangeRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskStateChangesTable).
		Where(qb.Eq{"job_id": jobID.GetValue(), "instance_id": instanceID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by jobID %v, instanceID %v, err=%v", jobID, instanceID, err)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by jobID %v, instanceID %v, err=%v", jobID, instanceID, err)
		return nil, err
	}
	for _, value := range allResults {
		var stateChangeRecords TaskStateChangeRecords
		err = FillObject(value, &stateChangeRecords, reflect.TypeOf(stateChangeRecords))
		if err != nil {
			log.Errorf("Failed to Fill into TaskStateChangeRecords, val = %v err= %v", value, err)
			return nil, err
		}
		return stateChangeRecords.GetStateChangeRecords()
	}
	return nil, fmt.Errorf("No state change records found for jobID %v, instanceID %v", jobID, instanceID)
}

// GetTasksForJobResultSet returns the result set that can be used to iterate each task in a job
// Caller need to call result.Close()
func (s *Store) GetTasksForJobResultSet(ctx context.Context, id *peloton.JobID) (api.ResultSet, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobResultSet by jobId %v, err=%v", jobID, err)
		return nil, err
	}
	return result, nil
}

// GetTasksForJob returns all the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error) {
	result, err := s.GetTasksForJobResultSet(ctx, id)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Error("Fail to GetTasksForJob")
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[uint32]*task.TaskInfo)
	allResults, err := result.All(ctx)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithError(err).
			Errorf("Fail to get all results for GetTasksForJob")
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("value", value).
				WithError(err).
				Error("Failed to Fill into TaskRuntimeRecord")
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		runtime, err := record.GetTaskRuntime()
		if err != nil {
			log.WithField("record", record).
				WithError(err).
				Error("Failed to parse task runtime from record")
			s.metrics.TaskGetFail.Inc(1)
			continue
		}

		taskInfo := &task.TaskInfo{
			Runtime:    runtime,
			InstanceId: uint32(record.InstanceID),
			JobId:      id,
		}

		s.metrics.TaskGet.Inc(1)
		resultMap[taskInfo.InstanceId] = taskInfo
	}
	return resultMap, nil
}

func (s *Store) getTaskConfig(ctx context.Context, id *peloton.JobID, instanceID uint32, version int64) (*task.TaskConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskConfigTable).
		Where(
			qb.Eq{
				"job_id":      id.GetValue(),
				"version":     version,
				"instance_id": []interface{}{instanceID, _defaultTaskConfigID},
			})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithField("job_id", id.GetValue()).
			WithField("instance_id", instanceID).
			WithField("version", version).
			WithError(err).
			Error("Fail to getTaskConfig")
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	taskID := fmt.Sprintf(taskIDFmt, id.GetValue(), int(instanceID))
	allResults, err := result.All(ctx)
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
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}

	s.metrics.TaskGet.Inc(1)
	return record.GetTaskConfig()
}

func (s *Store) getTaskInfoFromRuntimeRecord(ctx context.Context, id *peloton.JobID, record *TaskRuntimeRecord) (*task.TaskInfo, error) {
	runtime, err := record.GetTaskRuntime()
	if err != nil {
		log.Errorf("Failed to parse task runtime from record, val = %v err= %v", record, err)
		return nil, err
	}

	config, err := s.getTaskConfig(ctx, id, uint32(record.InstanceID), int64(runtime.ConfigVersion))
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

// GetTasksForJobAndState returns the tasks for a peloton job with certain state.
// result map key is TaskID, value is TaskHost
func (s *Store) GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("instance_id").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": state})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobAndState by jobId %v state %v, err=%v", jobID, state, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[uint32]*task.TaskInfo)
	allResults, err := result.All(ctx)
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		resultMap[uint32(record.InstanceID)], err = s.GetTask(ctx, id.GetValue(), uint32(record.InstanceID))
		if err != nil {
			log.Errorf("Failed to get taskInfo from task, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		s.metrics.TaskGet.Inc(1)
	}
	return resultMap, nil
}

// GetTasksForJobAndState returns the task count for a peloton job with certain state
func (s *Store) getTaskStateCount(ctx context.Context, id *peloton.JobID, state string) (uint32, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("count (*)").From(taskJobStateView).
		Where(qb.Eq{"job_id": jobID, "state": state})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.
			WithField("job_id", jobID).
			WithField("state", state).
			WithError(err).
			Error("Fail to getTaskStateCount by jobId")
		return 0, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
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
			return nil, err
		}
		resultMap[state] = count
	}
	return resultMap, nil
}

// GetTasksForJobByRange returns the tasks (tasks.TaskInfo) for a peloton job given instance id range
func (s *Store) GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	jobID := id.Value
	result := make(map[uint32]*task.TaskInfo)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").
		From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID}).
		Where("instance_id >= ?", instanceRange.From).
		Where("instance_id < ?", instanceRange.To)

	resultSet, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobByRange by jobId %v range %v, err=%v", jobID, instanceRange, err)
		return nil, err
	}
	if resultSet != nil {
		defer resultSet.Close()
	}
	allResults, err := resultSet.All(ctx)
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("job_id", id.Value).
				WithError(err).
				Error("Failed to Fill into TaskRecord")
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		taskInfo, err := s.getTaskInfoFromRuntimeRecord(ctx, id, &record)
		result[taskInfo.InstanceId] = taskInfo
		s.metrics.TaskGet.Inc(1)
	}
	return result, nil
}

// UpdateTask updates a task for a peloton job
func (s *Store) UpdateTask(ctx context.Context, taskInfo *task.TaskInfo) error {
	// Bump version of task.
	if taskInfo.Runtime.Revision == nil {
		taskInfo.Runtime.Revision = &peloton.ChangeLog{}
	}
	currentVersion := taskInfo.Runtime.Revision.Version

	taskInfo.Runtime.Revision.Version++
	taskInfo.Runtime.Revision.UpdatedAt = uint64(time.Now().UnixNano())

	runtimeBuffer, err := proto.Marshal(taskInfo.Runtime)
	if err != nil {
		log.WithField("job_id", taskInfo.JobId.Value).
			WithField("instance_id", taskInfo.InstanceId).
			WithError(err).
			Error("Failed to update task runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	vc := strconv.FormatUint(currentVersion, 10)
	if currentVersion == 0 {
		vc += ", null"
	}
	// Comment out the condition as we are seeing perf issue with CAS writes in C* in pit-prod01
	// condition := "version IN (" + vc + ")"

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(taskRuntimeTable).
		Set("version", taskInfo.Runtime.Revision.Version).
		Set("update_time", time.Now().UTC()).
		Set("state", taskInfo.GetRuntime().GetState().String()).
		Set("runtime_info", runtimeBuffer).
		Where(qb.Eq{"job_id": taskInfo.GetJobId().GetValue(), "instance_id": taskInfo.InstanceId}) //.
		//IfOnly(condition)

	if err := s.applyStatement(ctx, stmt, getTaskID(taskInfo)); err != nil {
		s.metrics.TaskUpdateFail.Inc(1)
		return err
	}
	s.metrics.TaskUpdate.Inc(1)
	s.logTaskStateChange(ctx, taskInfo.GetJobId(), taskInfo.InstanceId, taskInfo)
	return nil
}

// GetTaskForJob returns a task by jobID and instanceID
func (s *Store) GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	taskID := fmt.Sprintf(taskIDFmt, id.Value, int(instanceID))
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
		return err
	}

	stmt = queryBuilder.Delete(taskConfigTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		return err
	}

	stmt = queryBuilder.Delete(jobRuntimeTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		return err
	}

	stmt = queryBuilder.Delete(jobConfigTable).Where(qb.Eq{"job_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		return err
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
	return s.GetTask(ctx, jobID, uint32(instanceID))
}

// GetTask returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTask(ctx context.Context, jobID string, instanceID uint32) (*task.TaskInfo, error) {
	taskID := fmt.Sprintf(taskIDFmt, jobID, int(instanceID))
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskRuntimeTable).
		Where(qb.Eq{"job_id": jobID, "instance_id": instanceID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithField("task_id", taskID).
			WithError(err).
			Error("Fail to GetTask")
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
	for _, value := range allResults {
		var record TaskRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("task_id", taskID).
				WithError(err).
				Error("Failed to Fill into TaskRecord")
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}

		s.metrics.TaskGet.Inc(1)
		return s.getTaskInfoFromRuntimeRecord(ctx, &peloton.JobID{Value: jobID}, &record)
	}
	s.metrics.TaskNotFound.Inc(1)
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
	return s.applyStatement(ctx, stmt, frameworksTable)
}

//GetMesosStreamID reads the mesos stream id for a framework name
func (s *Store) GetMesosStreamID(ctx context.Context, frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(ctx, frameworkName)
	if err != nil {
		s.metrics.StreamIDGetFail.Inc(1)
		return "", err
	}

	s.metrics.StreamIDGet.Inc(1)
	return frameworkInfoRecord.MesosStreamID, nil
}

//GetFrameworkID reads the framework id for a framework name
func (s *Store) GetFrameworkID(ctx context.Context, frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(ctx, frameworkName)
	if err != nil {
		s.metrics.FrameworkIDGetFail.Inc(1)
		return "", err
	}

	s.metrics.FrameworkIDGet.Inc(1)
	return frameworkInfoRecord.FrameworkID, nil
}

func (s *Store) getFrameworkInfo(ctx context.Context, frameworkName string) (*FrameworkInfoRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(frameworksTable).
		Where(qb.Eq{"framework_name": frameworkName})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to getFrameworkInfo by frameworkName %v, err=%v", frameworkName, err)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
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
	err := s.DataStore.ExecuteBatch(ctx, stmts)
	if err != nil {
		log.Errorf("Fail to execute %d insert statements for job %v, err=%v", len(stmts), jobID, err)
		return err
	}
	return nil
}

func (s *Store) applyStatement(ctx context.Context, stmt api.Statement, itemName string) error {
	stmtString, _, _ := stmt.ToSQL()
	log.Debugf("stmt=%v", stmtString)
	result, err := s.DataStore.Execute(ctx, stmt)
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
		log.Error(errMsg)
		return yarpcerrors.AlreadyExistsErrorf(errMsg)
	}
	return nil
}

// CreateResourcePool creates a resource pool with the resource pool id and the config value
func (s *Store) CreateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig, owner string) error {
	resourcePoolID := id.Value
	configBuffer, err := json.Marshal(resPoolConfig)
	if err != nil {
		log.Errorf("error = %v", err)
		s.metrics.ResourcePoolCreateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(resPools).
		Columns("respool_id", "respool_config", "owner", "creation_time", "update_time").
		Values(resourcePoolID, string(configBuffer), owner, time.Now().UTC(), time.Now().UTC()).
		IfNotExist()

	err = s.applyStatement(ctx, stmt, resourcePoolID)
	if err != nil {
		s.metrics.ResourcePoolCreateFail.Inc(1)
		return err
	}
	s.metrics.ResourcePoolCreate.Inc(1)
	return nil
}

// GetResourcePool gets a resource pool info object
func (s *Store) GetResourcePool(ctx context.Context, id *peloton.ResourcePoolID) (*respool.ResourcePoolInfo, error) {
	return nil, errors.New("unimplemented")
}

// DeleteResourcePool Deletes the resource pool
func (s *Store) DeleteResourcePool(ctx context.Context, id *peloton.ResourcePoolID) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(resPools).Where(qb.Eq{"respool_id": id.GetValue()})
	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		return err
	}
	return nil
}

// UpdateResourcePool Update the resource pool
func (s *Store) UpdateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, Config *respool.ResourcePoolConfig) error {
	return errors.New("unimplemented")
}

// GetAllResourcePools Get all the resource pool configs
func (s *Store) GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("respool_id", "owner", "respool_config", "creation_time", "update_time").From(resPools)
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetAllResourcePools, err=%v", err)
		s.metrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*respool.ResourcePoolConfig)
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to get all results for GetAllResourcePools, err=%v", err)
		s.metrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var record ResourcePoolRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into ResourcePoolRecord, err= %v", err)
			s.metrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resourcePoolConfig, err := record.GetResourcePoolConfig()
		if err != nil {
			log.Errorf("Failed to get ResourceConfig from record, err= %v", err)
			s.metrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resultMap[record.RespoolID] = resourcePoolConfig
		s.metrics.ResourcePoolGet.Inc(1)
	}
	return resultMap, nil
}

// GetResourcePoolsByOwner Get all the resource pool b owner
func (s *Store) GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("respool_id", "owner", "respool_config", "creation_time", "update_time").From(resPoolsOwnerView).
		Where(qb.Eq{"owner": owner})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetResourcePoolsByOwner %v, err=%v", owner, err)
		s.metrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*respool.ResourcePoolConfig)
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to get all results for GetResourcePoolsByOwner %v, err=%v", owner, err)
		s.metrics.ResourcePoolGetFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record ResourcePoolRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into ResourcePoolRecord, err= %v", err)
			s.metrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resourcePoolConfig, err := record.GetResourcePoolConfig()
		if err != nil {
			log.Errorf("Failed to get ResourceConfig from record, err= %v", err)
			s.metrics.ResourcePoolGetFail.Inc(1)
			return nil, err
		}
		resultMap[record.RespoolID] = resourcePoolConfig
		s.metrics.ResourcePoolGet.Inc(1)
	}
	return resultMap, nil
}

func getTaskID(taskInfo *task.TaskInfo) string {
	jobID := taskInfo.JobId.Value
	return fmt.Sprintf(taskIDFmt, jobID, taskInfo.InstanceId)
}

// GetJobRuntime returns the job runtime info
func (s *Store) GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(jobRuntimeTable).
		Where(qb.Eq{"job_id": id.Value})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("GetJobRuntime failed")
		s.metrics.JobGetRuntimeFail.Inc(1)
		return nil, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("GetJobRuntime Get all results failed")
		s.metrics.JobGetRuntimeFail.Inc(1)
		return nil, err
	}

	for _, value := range allResults {
		var record JobRuntimeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("job_id", id.Value).
				WithField("value", value).
				Error("Failed to get JobRuntimeRecord from record")
			s.metrics.JobGetRuntimeFail.Inc(1)
			return nil, err
		}
		return record.GetJobRuntime()
	}
	s.metrics.JobNotFound.Inc(1)
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
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to marshal job runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
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
		configBuffer, err := json.Marshal(config)
		if err != nil {
			log.Errorf("Failed to marshal jobConfig, error = %v", err)
			s.metrics.JobCreateFail.Inc(1)
			return err
		}

		labelBuffer, err := json.Marshal(config.Labels)
		if err != nil {
			log.Errorf("Failed to marshal labels, error = %v", err)
			s.metrics.JobCreateFail.Inc(1)
			return err
		}

		stmt = stmt.Set("config", configBuffer).
			Set("respool_id", config.GetRespoolID().GetValue()).
			Set("owner", config.GetOwningTeam()).
			Set("labels", labelBuffer)
	}

	err = s.applyStatement(ctx, stmt, id.Value)
	if err != nil {
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobUpdateInfoFail.Inc(1)
		return err
	}
	s.metrics.JobUpdateInfo.Inc(1)
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
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(jobRuntimeTable).
		Set("state", runtime.GetState().String()).
		Set("update_time", time.Now().UTC()).
		Set("runtime_info", runtimeBuffer).
		Where(qb.Eq{"job_id": id.GetValue()})

	if err := s.applyStatement(ctx, stmt, id.Value); err != nil {
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	err = s.updateJobIndex(ctx, id, runtime, config)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("updateJobInfoWithJobRuntime failed")
		return err
	}
	s.metrics.JobUpdateRuntime.Inc(1)
	return nil
}

// QueryTasks returns all tasks in the given offset..offset+limit range.
func (s *Store) QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error) {
	jobConfig, err := s.GetJobConfig(ctx, id)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.GetValue()).
			Error("Failed to get jobConfig")
		s.metrics.JobQueryFail.Inc(1)
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
		return nil, 0, err
	}

	var result []*task.TaskInfo
	for i := offset; i < end; i++ {
		result = append(result, tasks[i])
	}
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
		s.metrics.VolumeCreateFail.Inc(1)
		return err
	}

	s.metrics.VolumeCreate.Inc(1)
	return nil
}

// UpdatePersistentVolume update state for a persistent volume.
func (s *Store) UpdatePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID, state pb_volume.VolumeState) error {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Update(volumeTable).
		Set("state", state.String()).
		Set("update_time", time.Now().UTC()).
		Where(qb.Eq{"volume_id": volumeID.GetValue()})

	err := s.applyStatement(ctx, stmt, volumeID.GetValue())
	if err != nil {
		s.metrics.VolumeUpdateFail.Inc(1)
		return err
	}

	s.metrics.VolumeUpdate.Inc(1)
	return nil
}

// GetPersistentVolume gets the persistent volume object.
func (s *Store) GetPersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) (*pb_volume.PersistentVolumeInfo, error) {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Select("*").
		From(volumeTable).
		Where(qb.Eq{"volume_id": volumeID.GetValue()})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("volume_id", volumeID).
			Error("Fail to GetPersistentVolume by volumeID.")
		s.metrics.VolumeGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}

	allResults, err := result.All(ctx)
	for _, value := range allResults {
		var record PersistentVolumeRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("raw_volume_value", value).
				Error("Failed to Fill into PersistentVolumeRecord.")
			s.metrics.VolumeGetFail.Inc(1)
			return nil, err
		}
		s.metrics.VolumeGet.Inc(1)
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
	return nil, &storage.VolumeNotFoundError{VolumeID: volumeID}
}

// DeletePersistentVolume delete persistent volume entry.
func (s *Store) DeletePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(volumeTable).Where(qb.Eq{"volume_id": volumeID.GetValue()})

	err := s.applyStatement(ctx, stmt, volumeID.GetValue())
	if err != nil {
		s.metrics.VolumeDeleteFail.Inc(1)
		return err
	}

	s.metrics.VolumeDelete.Inc(1)
	return nil
}

// CreateUpgrade creates a new entry in Cassandra, if it doesn't already exist.
func (s *Store) CreateUpgrade(ctx context.Context, id *upgrade.WorkflowID, spec *upgrade.UpgradeSpec) error {
	optionsBuffer, err := json.Marshal(spec.GetOptions())
	if err != nil {
		log.Errorf("Failed to marshal options, error = %v", err)
		return err
	}

	configBuffer, err := json.Marshal(spec.GetJobConfig())
	if err != nil {
		log.Errorf("Failed to marshal jobConfig, error = %v", err)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(upgradesTable).
		Columns(
			"upgrade_id",
			"options",
			"state",
			"progress",
			"instances",
			"job_id",
			"job_config").
		Values(
			id.GetValue(),
			string(optionsBuffer),
			upgrade.State_ROLLING_FORWARD,
			0,
			[]int{},
			spec.GetJobId().GetValue(),
			configBuffer).
		IfNotExist()

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithError(err).
			WithField("workflow_id", id.GetValue()).
			Error("CreateUpgrade failed")
		return err
	}

	return nil
}

// AddTaskToProcessing adds instanceID to the set of processing instances, and
// incrementing the progress by one. It's an error if instanceID is not the
// next instance in line to be upgraded.
func (s *Store) AddTaskToProcessing(ctx context.Context, id *upgrade.WorkflowID, instanceID uint32) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(upgradesTable).
		Add("instances", []uint32{instanceID}).
		Set("progress", instanceID+1).
		Where(qb.Eq{"upgrade_id": id.GetValue()}).
		IfOnly(fmt.Sprintf("progress = %d", instanceID))

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithError(err).
			WithField("workflow_id", id.GetValue()).
			Error("StartTaskUpgrade failed")
		return err
	}

	return nil
}

// RemoveTaskFromProcessing removes the instanceID from the set of processing
// instances. This function is a no-op if the instanceID is not in the list
// of processing tasks.
func (s *Store) RemoveTaskFromProcessing(ctx context.Context, id *upgrade.WorkflowID, instanceID uint32) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(upgradesTable).
		Remove("instances", []uint32{instanceID}).
		Where(qb.Eq{"upgrade_id": id.GetValue()})

	if err := s.applyStatement(ctx, stmt, id.GetValue()); err != nil {
		log.WithError(err).
			WithField("workflow_id", id.GetValue()).
			Error("CompleteTaskUpgrade failed")
		return err
	}

	return nil
}

// GetWorkflowProgress returns the list of tasks being process as well as the
// overall progress of the upgrade.
func (s *Store) GetWorkflowProgress(ctx context.Context, id *upgrade.WorkflowID) ([]uint32, uint32, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("instances", "progress").From(upgradesTable).Where(qb.Eq{"upgrade_id": id.GetValue()})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("GetJobRuntime failed")
		return nil, 0, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("GetJobRuntime Get all results failed")
		return nil, 0, err
	}

	for _, value := range allResults {
		var record UpgradeRecord
		if err := FillObject(value, &record, reflect.TypeOf(record)); err != nil {
			return nil, 0, err
		}
		return record.GetProcessingInstances(), uint32(record.Progress), nil
	}
	return nil, 0, fmt.Errorf("Cannot find workflow with ID %v", id.Value)
}

func parseTime(v string) time.Time {
	r, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}
	}
	return r
}
