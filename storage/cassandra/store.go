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
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	pb_volume "code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/cassandra/api"
	qb "code.uber.internal/infra/peloton/storage/querybuilder"

	"code.uber.internal/infra/peloton/storage/cassandra/impl"
	log "github.com/Sirupsen/logrus"
	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
)

const (
	taskIDFmt             = "%s-%d"
	jobsTable             = "jobs"
	jobRuntimeTable       = "job_runtime"
	tasksTable            = "tasks"
	taskStateChangesTable = "task_state_changes"
	frameworksTable       = "frameworks"
	jobOwnerView          = "mv_jobs_by_owner"
	taskJobStateView      = "mv_task_by_job_state"
	jobByStateView        = "mv_jobs_by_state"
	taskHostView          = "mv_task_by_host"
	resPools              = "respools"
	resPoolsOwnerView     = "mv_respools_by_owner"
	volumeTable           = "persistent_volumes"
	jobsByRespoolView     = "mv_jobs_by_respool"

	_defaultQueryLimit uint32 = 100
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

// CreateJob creates a job with the job id and the config value
func (s *Store) CreateJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, owner string) error {
	jobID := id.Value
	configBuffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.Errorf("Failed to marshal jobConfig, error = %v", err)
		s.metrics.JobCreateFail.Inc(1)
		return err
	}
	labels := jobConfig.Labels
	var labelBuffer []byte
	labelBuffer, err = json.Marshal(labels)
	if err != nil {
		log.Errorf("Failed to marshal labels, error = %v", err)
		s.metrics.JobCreateFail.Inc(1)
		return err
	}
	initialJobRuntime := job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: time.Now().Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(jobsTable).
		Columns(
			"JobID",
			"JobConfig",
			"Owner",
			"Labels",
			"CreateTime",
			"RespoolID").
		Values(
			jobID,
			string(configBuffer),
			owner,
			string(labelBuffer),
			time.Now(),
			jobConfig.GetRespoolID().GetValue()).
		IfNotExist()

	err = s.applyStatement(ctx, stmt, jobID)
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("CreateJob failed")
		s.metrics.JobCreateFail.Inc(1)
		return err
	}

	// Create the initial job runtime record
	err = s.UpdateJobRuntime(ctx, id, &initialJobRuntime)
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

// UpdateJobConfig updates a job with the job id and the config value
func (s *Store) UpdateJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	jobID := id.Value
	configBuffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.Errorf("Failed to marshal jobConfig, error = %v", err)
		s.metrics.JobUpdateFail.Inc(1)
		return err
	}
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(jobsTable).
		Set("JobConfig", string(configBuffer)).
		Where(qb.Eq{"JobID": jobID})
	err = s.applyStatement(ctx, stmt, id.Value)
	if err != nil {
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to update job config")
		s.metrics.JobUpdateFail.Inc(1)
		return err
	}
	s.metrics.JobUpdate.Inc(1)
	return nil
}

// GetJobConfig returns a job config given the job id
func (s *Store) GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobConfig").From(jobsTable).
		Where(qb.Eq{"JobID": jobID})
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
		var record JobRecord
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
func (s *Store) QueryJobs(ctx context.Context, respoolID *respool.ResourcePoolID, spec *job.QuerySpec) ([]*job.JobInfo, uint32, error) {
	// Query is based on stratio lucene index on jobs.
	// See https://github.com/Stratio/cassandra-lucene-index
	// We are using "must" for the labels and only return the jobs that contains all
	// label values
	// TODO: investigate if there are any golang library that can build lucene query
	// TODO: Apply sort order.

	var clauses []string

	// Labels field must contain value of the specified labels
	for _, label := range spec.GetLabels() {
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"labels", values:%s}`, strconv.Quote(label.Value)))
	}

	// jobconfig field must contain all specified keywords
	for _, word := range spec.GetKeywords() {
		clauses = append(clauses, fmt.Sprintf(`{type: "contains", field:"jobconfig", values:%s}`, strconv.Quote(word)))
	}

	where := `expr(jobs_index, '{query: {type: "boolean", must: [`
	for i, c := range clauses {
		if i > 0 {
			where += ", "
		}
		where += c
	}
	where += "]}}')"

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobID", "JobConfig").From(jobsTable)
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
		var record JobRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithField("labels", spec.GetLabels()).
				WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, 0, err
		}
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.WithField("labels", spec.GetLabels()).
				WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, 0, err
		}

		jobID := &peloton.JobID{
			Value: record.JobID,
		}

		jobRuntime, err := s.GetJobRuntime(ctx, jobID)
		if err != nil {
			log.Warnf("no job runtime found for job `%v` when executing jobs query", jobID)
		}

		// TODO: Add to view and move to cassandra where clause.
		if respoolID != nil && jobConfig.GetRespoolID().GetValue() != respoolID.GetValue() {
			continue
		}

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

	stmt := queryBuilder.Select("JobID").From(jobByStateView).
		Where(qb.Eq{"JobState": jobStates})
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
		var record JobRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Failed to get JobRecord from record")
			s.metrics.JobGetByStatesFail.Inc(1)
			return nil, err
		}
		jobs = append(jobs, peloton.JobID{Value: record.JobID})
	}
	s.metrics.JobGetByStates.Inc(1)
	return jobs, nil
}

// GetJobsByOwner returns jobs by owner
func (s *Store) GetJobsByOwner(ctx context.Context, owner string) (map[string]*job.JobConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobID", "JobConfig").From(jobOwnerView).
		Where(qb.Eq{"Owner": owner})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetJobsByOwner %v, err=%v", owner, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*job.JobConfig)
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to get all results for GetJobsByOwner %v, err=%v", owner, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var record JobRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into JobRecord, err= %v", err)
			s.metrics.JobGetFail.Inc(1)
			return nil, err
		}
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.Errorf("Failed to get jobConfig from record, err= %v", err)
			s.metrics.JobGetFail.Inc(1)
			return nil, err
		}
		resultMap[record.JobID] = jobConfig
		s.metrics.JobGet.Inc(1)
	}
	return resultMap, nil
}

// GetAllJobs returns all jobs
// TODO: introduce jobstate and add GetJobsByState
func (s *Store) GetAllJobs(ctx context.Context) (map[string]*job.JobConfig, error) {
	// TODO: Get jobs from  all edge respools
	var resultMap = make(map[string]*job.JobConfig)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobID", "JobConfig").From(jobsTable)
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
		var record JobRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, err
		}
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.WithError(err).
				Error("Fail to Query jobs")
			s.metrics.JobQueryFail.Inc(1)
			return nil, err
		}
		resultMap[record.JobID] = jobConfig
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
	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("Failed to marshal taskInfo, error = %v", err)
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}
	taskInfo.Runtime.State = task.TaskState_INITIALIZED
	taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(tasksTable). // TODO: runtime conf and task conf
							Columns("TaskID", "JobID", "TaskState", "CreateTime", "TaskInfo").
							Values(taskID, jobID, taskInfo.Runtime.State.String(), time.Now(), string(buffer)).
							IfNotExist()

	err = s.applyStatement(ctx, stmt, taskID)
	if err != nil {
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}
	s.metrics.TaskCreate.Inc(1)
	// Track the task events
	err = s.logTaskStateChange(ctx, taskID, taskInfo)
	if err != nil {
		log.Errorf("Unable to log task state changes for job ID %v instance %v, error = %v", jobID, taskID, err)
		return err
	}
	return nil
}

// CreateTasks creates tasks for the given slice of task infos, instances 0..n
func (s *Store) CreateTasks(ctx context.Context, id *peloton.JobID, taskInfos []*task.TaskInfo, owner string) error {
	maxBatchSize := int64(s.Conf.MaxBatchSize)
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxInt64
	}
	jobID := id.Value
	nTasks := int64(len(taskInfos))
	tasksNotCreated := int64(0)
	timeStart := time.Now()
	nBatches := nTasks/maxBatchSize + 1
	wg := new(sync.WaitGroup)
	log.WithField("batches", nBatches).
		WithField("tasks", nTasks).
		Debug("Creating tasks")
	for batch := int64(0); batch < nBatches; batch++ {
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
		wg.Add(1)
		go func() {
			batchTimeStart := time.Now()
			insertStatements := []api.Statement{}
			idsToTaskInfos := map[string]*task.TaskInfo{}
			defer wg.Done()
			log.WithField("id", id.Value).
				WithField("start", start).
				WithField("end", end).
				Debug("creating tasks")
			for i := start; i < end; i++ {
				t := taskInfos[i]
				buffer, err := json.Marshal(t)
				if err != nil {
					log.Errorf("Failed to marshal taskInfo for job ID %v and instance %d, error = %v", jobID, t.InstanceId, err)
					s.metrics.TaskCreateFail.Inc(nTasks)
					atomic.AddInt64(&tasksNotCreated, batchSize)
					return
				}

				t.Runtime.State = task.TaskState_INITIALIZED
				taskID := fmt.Sprintf(taskIDFmt, jobID, t.InstanceId)

				idsToTaskInfos[taskID] = t

				queryBuilder := s.DataStore.NewQuery()
				stmt := queryBuilder.Insert(tasksTable).
					Columns("TaskID", "JobID", "TaskState", "CreateTime", "TaskInfo").
					Values(taskID, jobID, t.Runtime.State.String(), time.Now(), string(buffer))

				// IfNotExist() will cause Writing 20 tasks (0:19) for TestJob2 to Cassandra failed in 8.756852ms with
				// Batch with conditions cannot span multiple partitions. For now, drop the IfNotExist()

				insertStatements = append(insertStatements, stmt)
			}
			err := s.applyStatements(ctx, insertStatements, jobID)
			if err != nil {
				log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
					Errorf("Writing %d tasks (%d:%d) for %v to Cassandra failed in %v with %v", batchSize, start, end-1, id.Value, time.Since(batchTimeStart), err)
				s.metrics.TaskCreateFail.Inc(nTasks)
				atomic.AddInt64(&tasksNotCreated, batchSize)
				return
			}
			log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
				Debugf("Wrote %d tasks (%d:%d) for %v to Cassandra in %v", batchSize, start, end-1, id.Value, time.Since(batchTimeStart))
			s.metrics.TaskCreate.Inc(nTasks)

			err = s.logTaskStateChanges(ctx, idsToTaskInfos)
			if err != nil {
				log.Errorf("Unable to log task state changes for job ID %v range(%d:%d), error = %v", jobID, start, end-1, err)
			}
		}()
	}
	wg.Wait()
	if tasksNotCreated != 0 {
		// TODO: should we propogate this error up the stack? Should we fire logTaskStateChanges before doing so?
		log.Errorf("Wrote %d tasks for %v, and was unable to write %d tasks to Cassandra in %v", nTasks-tasksNotCreated, id, tasksNotCreated, time.Since(timeStart))
	} else {
		log.WithField("duration_s", time.Since(timeStart).Seconds()).
			Infof("Wrote all %d tasks for %v to Cassandra in %v", nTasks, id, time.Since(timeStart))
	}
	return nil

}

// logTaskStateChange logs the task state change events
func (s *Store) logTaskStateChange(ctx context.Context, taskID string, taskInfo *task.TaskInfo) error {
	var stateChange = TaskStateChangeRecord{
		TaskID:    taskID,
		TaskState: taskInfo.Runtime.State.String(),
		TaskHost:  taskInfo.Runtime.Host,
		EventTime: time.Now(),
	}
	buffer, err := json.Marshal(stateChange)
	if err != nil {
		log.Errorf("Failed to marshal stateChange, error = %v", err)
		return err
	}
	stateChangePart := []string{string(buffer)}
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Update(taskStateChangesTable).
		Add("Events", stateChangePart).
		Where(qb.Eq{"TaskID": taskID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if result != nil {
		defer result.Close()
	}
	if err != nil {
		log.Errorf("Fail to logTaskStateChange by taskID %v %v, err=%v", taskID, stateChangePart, err)
		return err
	}
	return nil
}

// logTaskStateChanges logs multiple task state change events in a batch operation (one RPC, separate statements)
// taskIDToTaskInfos is a map of task ID to task info
func (s *Store) logTaskStateChanges(ctx context.Context, taskIDToTaskInfos map[string]*task.TaskInfo) error {
	statements := []api.Statement{}
	for taskID, taskInfo := range taskIDToTaskInfos {
		var stateChange = TaskStateChangeRecord{
			TaskID:      taskID,
			TaskState:   taskInfo.Runtime.State.String(),
			TaskHost:    taskInfo.Runtime.Host,
			EventTime:   time.Now(),
			MesosTaskID: taskInfo.Runtime.TaskId.GetValue(),
		}
		buffer, err := json.Marshal(stateChange)
		if err != nil {
			log.Errorf("Failed to marshal stateChange for task %v, error = %v", taskID, err)
			return err
		}
		stateChangePart := []string{string(buffer)}
		queryBuilder := s.DataStore.NewQuery()
		stmt := queryBuilder.Update(taskStateChangesTable).
			Add("Events", stateChangePart).
			Where(qb.Eq{"TaskID": taskID})
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
func (s *Store) GetTaskStateChanges(ctx context.Context, taskID string) ([]*TaskStateChangeRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskStateChangesTable).
		Where(qb.Eq{"TaskID": taskID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by taskID %v, err=%v", taskID, err)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by taskID %v, err=%v", taskID, err)
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
	return nil, fmt.Errorf("No state change records found for taskID %v", taskID)
}

// GetTasksForJobResultSet returns the result set that can be used to iterate each task in a job
// Caller need to call result.Close()
func (s *Store) GetTasksForJobResultSet(ctx context.Context, id *peloton.JobID) (api.ResultSet, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID})
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
		log.Errorf("Fail to GetTasksForJob by jobId %v, err=%v", id.Value, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[uint32]*task.TaskInfo)
	allResults, err := result.All(ctx)
	if err != nil {
		log.Errorf("Fail to get all results for GetTasksForJob by jobId %v, err=%v", id.Value, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	for _, value := range allResults {
		var record TaskRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		taskInfo, err := record.GetTaskInfo()
		if err != nil {
			log.Errorf("Failed to parse task Info from record, val = %v err= %v", taskInfo, err)
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		s.metrics.TaskGet.Inc(1)
		resultMap[taskInfo.InstanceId] = taskInfo
	}
	return resultMap, nil
}

// GetTasksForJobAndState returns the tasks for a peloton job with certain state.
// result map key is TaskID, value is TaskHost
func (s *Store) GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("TaskID", "TaskInfo").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID, "TaskState": state})
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
		var record TaskRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		var taskInfo *task.TaskInfo
		taskInfo, err = record.GetTaskInfo()
		if err != nil {
			log.Errorf("Failed to parse taskInfo from TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		resultMap[taskInfo.InstanceId] = taskInfo
		s.metrics.TaskGet.Inc(1)
	}
	return resultMap, nil
}

// GetTasksOnHost returns the tasks running on a host,
// result map key is taskID, value is taskState
func (s *Store) GetTasksOnHost(ctx context.Context, host string) (map[string]string, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskHostView).
		Where(qb.Eq{"TaskHost": host})

	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksOnHost by host %v, err=%v", host, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[string]string)
	allResults, err := result.All(ctx)
	for _, value := range allResults {
		var record TaskRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		resultMap[record.TaskID] = record.TaskState
		s.metrics.TaskGet.Inc(1)
	}
	return resultMap, nil
}

// GetTasksForJobAndState returns the task count for a peloton job with certain state
func (s *Store) getTaskStateCount(ctx context.Context, id *peloton.JobID, state string) (int, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("count (*)").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID, "TaskState": state})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to getTaskStateCount by jobId %v state %v, err=%v", jobID, state, err)
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
			return int(val), nil
		}
	}
	return 0, nil
}

// GetTaskStateSummaryForJob returns the tasks count (runtime_config) for a peloton job with certain state
func (s *Store) GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]int, error) {
	resultMap := make(map[string]int)
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
	var i uint32
	for i = instanceRange.From; i < instanceRange.To; i++ {
		taskID := fmt.Sprintf(taskIDFmt, jobID, i)
		task, err := s.GetTaskByID(ctx, taskID)
		if err != nil {
			log.Errorf("Failed to retrieve job %v instance %d, err= %v", jobID, i, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		s.metrics.TaskGet.Inc(1)
		result[i] = task
	}
	return result, nil
}

// UpdateTask updates a task for a peloton job
func (s *Store) UpdateTask(ctx context.Context, taskInfo *task.TaskInfo) error {
	taskID := getTaskID(taskInfo)
	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("error = %v", err)
		s.metrics.TaskUpdateFail.Inc(1)
		return err
	}
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(tasksTable). // TODO: runtime conf and task conf
							Columns("TaskID", "JobID", "TaskState", "TaskHost", "CreateTime", "TaskInfo").
							Values(taskID, taskInfo.JobId.Value, taskInfo.GetRuntime().State.String(), taskInfo.GetRuntime().Host, time.Now(), string(buffer))
	err = s.applyStatement(ctx, stmt, taskID)
	if err != nil {
		s.metrics.TaskUpdateFail.Inc(1)
		return err
	}
	s.metrics.TaskUpdate.Inc(1)
	s.logTaskStateChange(ctx, taskID, taskInfo)
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

// DeleteJob deletes a job by id
// TODO: decide if DeleteJob() should be removed from API
func (s *Store) DeleteJob(ctx context.Context, id *peloton.JobID) error {
	return nil
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(tasksTable).
		Where(qb.Eq{"TaskID": taskID})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskByID by taskID %v, err=%v", taskID, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(ctx)
	for _, value := range allResults {
		var record TaskRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		s.metrics.TaskGet.Inc(1)
		return record.GetTaskInfo()
	}
	s.metrics.TaskNotFound.Inc(1)
	return nil, &storage.TaskNotFoundError{TaskID: taskID}
}

//SetMesosStreamID stores the mesos framework id for a framework name
func (s *Store) SetMesosStreamID(ctx context.Context, frameworkName string, mesosStreamID string) error {
	return s.updateFrameworkTable(ctx, map[string]interface{}{"FrameworkName": frameworkName, "MesosStreamID": mesosStreamID})
}

//SetMesosFrameworkID stores the mesos framework id for a framework name
func (s *Store) SetMesosFrameworkID(ctx context.Context, frameworkName string, frameworkID string) error {
	return s.updateFrameworkTable(ctx, map[string]interface{}{"FrameworkName": frameworkName, "FrameworkID": frameworkID})
}

func (s *Store) updateFrameworkTable(ctx context.Context, content map[string]interface{}) error {
	hostName, err := os.Hostname()
	if err != nil {
		return err
	}
	queryBuilder := s.DataStore.NewQuery()
	content["UpdateHost"] = hostName
	content["UpdateTime"] = time.Now()

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
		Where(qb.Eq{"FrameworkName": frameworkName})
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
		return fmt.Errorf(errMsg)
	}
	return nil
}

// CreateResourcePool creates a resource pool with the resource pool id and the config value
func (s *Store) CreateResourcePool(ctx context.Context, id *respool.ResourcePoolID, resPoolConfig *respool.ResourcePoolConfig, owner string) error {
	resourcePoolID := id.Value
	configBuffer, err := json.Marshal(resPoolConfig)
	if err != nil {
		log.Errorf("error = %v", err)
		s.metrics.ResourcePoolCreateFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(resPools).
		Columns("ID", "ResourcePoolConfig", "Owner", "CreateTime", "UpdateTime").
		Values(resourcePoolID, string(configBuffer), owner, time.Now(), time.Now()).
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
func (s *Store) GetResourcePool(ctx context.Context, id *respool.ResourcePoolID) (*respool.ResourcePoolInfo, error) {
	return nil, errors.New("unimplemented")
}

// DeleteResourcePool Deletes the resource pool
func (s *Store) DeleteResourcePool(ctx context.Context, id *respool.ResourcePoolID) error {
	return errors.New("unimplemented")
}

// UpdateResourcePool Update the resource pool
func (s *Store) UpdateResourcePool(ctx context.Context, id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig) error {
	return errors.New("unimplemented")
}

// GetAllResourcePools Get all the resource pool configs
func (s *Store) GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("ID", "Owner", "ResourcePoolConfig", "CreateTime", "UpdateTime").From(resPools)
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
		resultMap[record.ID] = resourcePoolConfig
		s.metrics.ResourcePoolGet.Inc(1)
	}
	return resultMap, nil
}

// GetResourcePoolsByOwner Get all the resource pool b owner
func (s *Store) GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("ID", "Owner", "ResourcePoolConfig", "CreateTime", "UpdateTime").From(resPoolsOwnerView).
		Where(qb.Eq{"Owner": owner})
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
		resultMap[record.ID] = resourcePoolConfig
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
	stmt := queryBuilder.Select("JobRuntime").From(jobRuntimeTable).
		Where(qb.Eq{"JobID": id.Value})
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
		s.metrics.JobGetRuntime.Inc(1)
		return record.GetJobRuntime()
	}
	s.metrics.JobNotFound.Inc(1)
	return nil, fmt.Errorf("Cannot find job wth jobID %v", id.Value)
}

// UpdateJobRuntime updates the job runtime info
func (s *Store) UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error {
	buffer, err := json.Marshal(runtime)
	if err != nil {
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to marshal job runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(jobRuntimeTable).
		Columns("JobID", "JobState", "UpdateTime", "JobRuntime").
		Values(id.Value, runtime.State.String(), time.Now(), string(buffer))
	err = s.applyStatement(ctx, stmt, id.Value)
	if err != nil {
		log.WithField("job_id", id.Value).
			WithError(err).
			Error("Failed to update job runtime")
		s.metrics.JobUpdateRuntimeFail.Inc(1)
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
	if offset >= jobConfig.InstanceCount {
		return nil, 0, errors.New("offset larger than job instances")
	}
	limit := _defaultQueryLimit
	if spec.GetPagination() != nil {
		limit = spec.GetPagination().GetLimit()
	}
	end := offset + limit - 1
	if end > jobConfig.InstanceCount-1 {
		end = jobConfig.InstanceCount - 1
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
	return result, uint32(len(result)), nil
}

// CreatePersistentVolume creates a persistent volume entry.
func (s *Store) CreatePersistentVolume(ctx context.Context, volume *pb_volume.PersistentVolumeInfo) error {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(volumeTable).
		Columns("ID", "State", "GoalState", "JobID", "InstanceID", "Hostname", "SizeMb", "ContainerPath", "CreateTime", "UpdateTime").
		Values(
			volume.GetId().GetValue(),
			volume.State.String(),
			volume.GoalState.String(),
			volume.GetJobId().GetValue(),
			volume.InstanceId,
			volume.Hostname,
			volume.SizeMB,
			volume.ContainerPath,
			time.Now(),
			time.Now()).
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
func (s *Store) UpdatePersistentVolume(ctx context.Context, volumeID string, state pb_volume.VolumeState) error {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Update(volumeTable).
		Set("State", state.String()).
		Set("UpdateTime", time.Now()).
		Where(qb.Eq{"ID": volumeID})

	err := s.applyStatement(ctx, stmt, volumeID)
	if err != nil {
		s.metrics.VolumeUpdateFail.Inc(1)
		return err
	}

	s.metrics.VolumeUpdate.Inc(1)
	return nil
}

// GetPersistentVolume gets the persistent volume object.
func (s *Store) GetPersistentVolume(ctx context.Context, volumeID string) (*pb_volume.PersistentVolumeInfo, error) {

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.
		Select("*").
		From(volumeTable).
		Where(qb.Eq{"ID": volumeID})
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
				Value: record.ID,
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
	return nil, fmt.Errorf("PersistentVolume not found for ID %s", volumeID)
}

// DeletePersistentVolume delete persistent volume entry.
func (s *Store) DeletePersistentVolume(ctx context.Context, volumeID string) error {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Delete(volumeTable).Where(qb.Eq{"ID": volumeID})

	err := s.applyStatement(ctx, stmt, volumeID)
	if err != nil {
		s.metrics.VolumeDeleteFail.Inc(1)
		return err
	}

	s.metrics.VolumeDelete.Inc(1)
	return nil
}

// GetJobsByRespoolID returns jobIDs in a respool
func (s *Store) GetJobsByRespoolID(ctx context.Context, respoolID *respool.ResourcePoolID) (map[string]*job.JobConfig, error) {
	if respoolID == nil || respoolID.Value == "" {
		return nil, errors.New("respoolID is null / empty")
	}
	respoolIDVal := respoolID.Value

	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobID", "JobConfig").From(jobsByRespoolView).
		Where(qb.Eq{"RespoolID": respoolID.Value})
	result, err := s.DataStore.Execute(ctx, stmt)
	if err != nil {
		log.WithError(err).
			WithField("respool_id", respoolIDVal).
			Error("GetJobIDsByRespoolID failed")
		s.metrics.JobGetByRespoolIDFail.Inc(1)
		return nil, err
	}

	allResults, err := result.All(ctx)
	if err != nil {
		log.WithError(err).
			WithField("respool_id", respoolIDVal).
			Error("GetJobIDsByRespoolID get all results failed")
		s.metrics.JobGetByRespoolIDFail.Inc(1)
		return nil, err
	}

	results := make(map[string]*job.JobConfig)
	for _, value := range allResults {
		var record JobRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.WithError(err).
				WithField("respool_id", respoolIDVal).
				Error("Failed to get JobRecord from record")
			s.metrics.JobGetByRespoolIDFail.Inc(1)
			return nil, err
		}
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.WithError(err).
				WithField("respool_id", respoolIDVal).
				Error("Failed to get jobConfig from record")
			s.metrics.JobGetByRespoolIDFail.Inc(1)
			return nil, err
		}
		results[record.JobID] = jobConfig
	}
	s.metrics.JobGetByRespoolID.Inc(1)
	return results, nil
}
