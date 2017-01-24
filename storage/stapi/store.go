package stapi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"peloton/job"
	"peloton/task"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mesos "code.uber.internal/infra/peloton/pbgen/src/mesos/v1"
	peloton_storage "code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/stapi-go.git"
	"code.uber.internal/infra/stapi-go.git/api"
	sc "code.uber.internal/infra/stapi-go.git/config"
	qb "code.uber.internal/infra/stapi-go.git/querybuilder"
	log "github.com/Sirupsen/logrus"
	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
)

const (
	taskIDFmt             = "%s-%d"
	jobsTable             = "jobs"
	tasksTable            = "tasks"
	taskStateChangesTable = "task_state_changes"
	frameworksTable       = "frameworks"
	jobOwnerView          = "mv_jobs_by_owner"
	taskJobStateView      = "mv_task_by_job_state"
	taskHostView          = "mv_task_by_host"
)

// Config is the config for STAPIStore
type Config struct {
	Stapi      sc.Configuration `yaml:"stapi"`
	StoreName  string           `yaml:"store_name"`
	Migrations string           `yaml:"migrations"`
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
		c.Stapi.Cassandra.ContactPoints[0],
		c.Stapi.Cassandra.Port,
		c.StoreName)
	connStr = strings.Replace(connStr, " ", "", -1)
	log.Infof("Cassandra migration string %v", connStr)
	return connStr
}

// Store implements JobStore using a cassandra backend
type Store struct {
	DataStore api.DataStore
	metrics   peloton_storage.Metrics
	Conf      *Config
}

// NewStore creates a Store
func NewStore(config *Config, metricScope tally.Scope) (*Store, error) {
	storage.Initialize(storage.Options{
		Cfg:    config.Stapi,
		AppID:  "peloton",
		Logger: bark.NewLoggerFromLogrus(log.StandardLogger()),
	})
	dataStore, err := storage.OpenDataStore(config.StoreName)
	if err != nil {
		log.Errorf("Failed to NewSTAPIStore, err=%v", err)
		return nil, err
	}
	return &Store{
		DataStore: dataStore,
		metrics:   peloton_storage.NewMetrics(metricScope),
		Conf:      config,
	}, nil
}

// CreateJob creates a job with the job id and the config value
func (s *Store) CreateJob(id *job.JobID, jobConfig *job.JobConfig, owner string) error {
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
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(jobsTable).
		Columns("JobID", "JobConfig", "Owner", "Labels", "JobState", "CreateTime").
		Values(jobID, string(configBuffer), owner, string(labelBuffer), "Init", time.Now()).
		IfNotExist()

	err = s.applyStatement(stmt, jobID)
	if err != nil {
		s.metrics.JobCreateFail.Inc(1)
		return err
	}
	s.metrics.JobCreate.Inc(1)
	return nil
}

// GetJob returns a job config given the job id
func (s *Store) GetJob(id *job.JobID) (*job.JobConfig, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobConfig").From(jobsTable).
		Where(qb.Eq{"JobID": jobID})
	stmtString, _, _ := stmt.ToSql()
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to execute stmt %v, err=%v", stmtString, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	allResults, err := result.All(context.Background())
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

// Query returns all jobs that contains the Labels.
func (s *Store) Query(Labels *mesos.Labels) (map[string]*job.JobConfig, error) {
	return nil, nil
}

// GetJobsByOwner returns jobs by owner
func (s *Store) GetJobsByOwner(owner string) (map[string]*job.JobConfig, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("JobID", "JobConfig").From(jobOwnerView).
		Where(qb.Eq{"Owner": owner})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetJobsByOwner %v, err=%v", owner, err)
		s.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	var resultMap = make(map[string]*job.JobConfig)
	allResults, err := result.All(context.Background())
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
func (s *Store) GetAllJobs() (map[string]*job.JobConfig, error) {
	return nil, nil
}

// CreateTask creates a task for a peloton job
// TODO: remove this in favor of CreateTasks
func (s *Store) CreateTask(id *job.JobID, instanceID int, taskInfo *task.TaskInfo, owner string) error {
	jobID := id.Value
	if taskInfo.InstanceId != uint32(instanceID) || taskInfo.JobId.Value != jobID {
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
	taskInfo.Runtime.State = task.RuntimeInfo_INITIALIZED
	taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Insert(tasksTable). // TODO: runtime conf and task conf
							Columns("TaskID", "JobID", "TaskState", "CreateTime", "TaskInfo").
							Values(taskID, jobID, taskInfo.Runtime.State.String(), time.Now(), string(buffer)).
							IfNotExist()

	err = s.applyStatement(stmt, taskID)
	if err != nil {
		s.metrics.TaskCreateFail.Inc(1)
		return err
	}
	s.metrics.TaskCreate.Inc(1)
	// Track the task events
	err = s.logTaskStateChange(taskID, taskInfo)
	if err != nil {
		log.Errorf("Unable to log task state changes for job ID %v instance %v, error = %v", jobID, taskID, err)
		return err
	}
	return nil
}

// CreateTasks creates tasks for the given slice of task infos, instances 0..n
func (s *Store) CreateTasks(id *job.JobID, taskInfos []*task.TaskInfo, owner string) error {
	maxBatchSize := int64(s.Conf.MaxBatchSize)
	if maxBatchSize == 0 {
		maxBatchSize = math.MaxInt64
	}
	jobID := id.Value
	nTasks := int64(len(taskInfos))
	tasksNotCreated := int64(0)
	writeLock := sync.Mutex{} // protect these datastructs against concurrent modification
	idsToTaskInfos := map[string]*task.TaskInfo{}
	timeStart := time.Now()
	wg := new(sync.WaitGroup)

	for batch := int64(0); batch <= nTasks/maxBatchSize; batch++ {
		// do batching by rows, up to s.Conf.MaxBatchSize
		start := batch * maxBatchSize // the starting instance ID
		end := nTasks                 // the end bounds (noninclusive)
		if nTasks >= (batch+1)*maxBatchSize {
			end = (batch + 1) * maxBatchSize
		}
		batchSize := end - start // how many tasks in this batch
		wg.Add(1)
		go func() {
			batchTimeStart := time.Now()
			insertStatements := []api.Statement{}
			defer wg.Done()
			for instanceID := start; instanceID < end; instanceID++ {
				t := taskInfos[instanceID]
				// abort if the tasks dont have the expected instance IDs and job IDs
				if t.InstanceId != uint32(instanceID) || t.JobId.Value != jobID {
					errMsg := fmt.Sprintf("Task should have id %v, different than instance ID %d in task info, jobID %v, task JobId %v",
						instanceID, t.InstanceId, jobID, t.JobId.Value)
					log.Errorf(errMsg)
					s.metrics.TaskCreateFail.Inc(nTasks)
					atomic.AddInt64(&tasksNotCreated, batchSize)
					return
				}
				buffer, err := json.Marshal(t)
				if err != nil {
					log.Errorf("Failed to marshal taskInfo for job ID %v and instance %d, error = %v", jobID, instanceID, err)
					s.metrics.TaskCreateFail.Inc(nTasks)
					atomic.AddInt64(&tasksNotCreated, batchSize)
					return
				}

				t.Runtime.State = task.RuntimeInfo_INITIALIZED
				taskID := fmt.Sprintf(taskIDFmt, jobID, instanceID)

				writeLock.Lock()
				idsToTaskInfos[taskID] = t
				writeLock.Unlock()

				queryBuilder := s.DataStore.NewQuery()
				stmt := queryBuilder.Insert(tasksTable).
					Columns("TaskID", "JobID", "TaskState", "CreateTime", "TaskInfo").
					Values(taskID, jobID, t.Runtime.State.String(), time.Now(), string(buffer))

					// IfNotExist() will cause Writing 20 tasks (0:19) for TestJob2 to Cassandra failed in 8.756852ms with
					// Batch with conditions cannot span multiple partitions. For now, drop the IfNotExist()

				writeLock.Lock()
				insertStatements = append(insertStatements, stmt)
				writeLock.Unlock()
			}
			err := s.applyStatements(insertStatements, jobID)
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

	// Track the task events
	err := s.logTaskStateChanges(idsToTaskInfos)
	if err != nil {
		log.Errorf("Unable to log %d task state changes for job ID %v, error = %v", nTasks, jobID, err)
		return err
	}

	return nil

}

// logTaskStateChange logs the task state change events
func (s *Store) logTaskStateChange(taskID string, taskInfo *task.TaskInfo) error {
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
	result, err := s.DataStore.Execute(context.Background(), stmt)
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
func (s *Store) logTaskStateChanges(taskIDToTaskInfos map[string]*task.TaskInfo) error {
	statements := []api.Statement{}
	for taskID, taskInfo := range taskIDToTaskInfos {
		var stateChange = TaskStateChangeRecord{
			TaskID:    taskID,
			TaskState: taskInfo.Runtime.State.String(),
			TaskHost:  taskInfo.Runtime.Host,
			EventTime: time.Now(),
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
	err := s.DataStore.ExecuteBatch(context.Background(), statements)
	if err != nil {
		log.Errorf("Fail to logTaskStateChanges for %d tasks, err=%v", len(taskIDToTaskInfos), err)
		return err
	}
	return nil
}

// GetTaskStateChanges returns the state changes for a task
func (s *Store) GetTaskStateChanges(taskID string) ([]*TaskStateChangeRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskStateChangesTable).
		Where(qb.Eq{"TaskID": taskID})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskStateChanges by taskID %v, err=%v", taskID, err)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(context.Background())
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
func (s *Store) GetTasksForJobResultSet(id *job.JobID) (api.ResultSet, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobResultSet by jobId %v, err=%v", jobID, err)
		return nil, err
	}
	return result, nil
}

// GetTasksForJob returns all the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTasksForJob(id *job.JobID) (map[uint32]*task.TaskInfo, error) {
	result, err := s.GetTasksForJobResultSet(id)
	if err != nil {
		log.Errorf("Fail to GetTasksForJob by jobId %v, err=%v", id.Value, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[uint32]*task.TaskInfo)
	allResults, err := result.All(context.Background())
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
func (s *Store) GetTasksForJobAndState(id *job.JobID, state string) (map[string]string, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("TaskID", "TaskHost").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID, "TaskState": state})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksForJobAndState by jobId %v state %v, err=%v", jobID, state, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[string]string)
	allResults, err := result.All(context.Background())
	for _, value := range allResults {
		var record TaskRecord
		err := FillObject(value, &record, reflect.TypeOf(record))
		if err != nil {
			log.Errorf("Failed to Fill into TaskRecord, val = %v err= %v", value, err)
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		resultMap[record.TaskID] = record.TaskHost
		s.metrics.TaskGet.Inc(1)
	}
	return resultMap, nil
}

// GetTasksOnHost returns the tasks running on a host,
// result map key is taskID, value is taskState
func (s *Store) GetTasksOnHost(host string) (map[string]string, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(taskHostView).
		Where(qb.Eq{"TaskHost": host})

	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetTasksOnHost by host %v, err=%v", host, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	resultMap := make(map[string]string)
	allResults, err := result.All(context.Background())
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
func (s *Store) getTaskStateCount(id *job.JobID, state string) (int, error) {
	jobID := id.Value
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("count (*)").From(taskJobStateView).
		Where(qb.Eq{"JobID": jobID, "TaskState": state})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to getTaskStateCount by jobId %v state %v, err=%v", jobID, state, err)
		return 0, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(context.Background())
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
func (s *Store) GetTaskStateSummaryForJob(id *job.JobID) (map[string]int, error) {
	resultMap := make(map[string]int)
	for _, state := range task.RuntimeInfo_TaskState_name {
		count, err := s.getTaskStateCount(id, state)
		if err != nil {
			return nil, err
		}
		resultMap[state] = count
	}
	return resultMap, nil
}

// GetTasksForJobByRange returns the tasks (tasks.TaskInfo) for a peloton job given instance id range
func (s *Store) GetTasksForJobByRange(id *job.JobID, instanceRange *task.InstanceRange) map[uint32]*task.TaskInfo {
	jobID := id.Value
	result := make(map[uint32]*task.TaskInfo)
	var i uint32
	for i = instanceRange.From; i <= instanceRange.To; i++ {
		taskID := fmt.Sprintf(taskIDFmt, jobID, i)
		task, err := s.GetTaskByID(taskID)
		if err != nil {
			log.Errorf("Failed to retrieve job %v instance %d, err= %v", jobID, i, err)
			s.metrics.TaskGetFail.Inc(1)
			continue
		}
		s.metrics.TaskGet.Inc(1)
		result[i] = task
	}
	return result
}

// UpdateTask updates a task for a peloton job
func (s *Store) UpdateTask(taskInfo *task.TaskInfo) error {
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
	err = s.applyStatement(stmt, taskID)
	if err != nil {
		s.metrics.TaskUpdateFail.Inc(1)
		return err
	}
	s.metrics.TaskUpdate.Inc(1)
	s.logTaskStateChange(taskID, taskInfo)
	return nil
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (s *Store) GetTaskByID(taskID string) (*task.TaskInfo, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(tasksTable).
		Where(qb.Eq{"TaskID": taskID})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to GetTaskByID by taskID %v, err=%v", taskID, err)
		s.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(context.Background())
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
	return nil, fmt.Errorf("Task id %v not found", taskID)
}

//SetMesosStreamID stores the mesos framework id for a framework name
func (s *Store) SetMesosStreamID(frameworkName string, mesosStreamID string) error {
	return s.updateFrameworkTable(map[string]interface{}{"FrameworkName": frameworkName, "MesosStreamID": mesosStreamID})
}

//SetMesosFrameworkID stores the mesos framework id for a framework name
func (s *Store) SetMesosFrameworkID(frameworkName string, frameworkID string) error {
	return s.updateFrameworkTable(map[string]interface{}{"FrameworkName": frameworkName, "FrameworkID": frameworkID})
}

func (s *Store) updateFrameworkTable(content map[string]interface{}) error {
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
	return s.applyStatement(stmt, frameworksTable)
}

//GetMesosStreamID reads the mesos stream id for a framework name
func (s *Store) GetMesosStreamID(frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(frameworkName)
	if err != nil {
		return "", err
	}
	return frameworkInfoRecord.MesosStreamID, nil
}

//GetFrameworkID reads the framework id for a framework name
func (s *Store) GetFrameworkID(frameworkName string) (string, error) {
	frameworkInfoRecord, err := s.getFrameworkInfo(frameworkName)
	if err != nil {
		return "", err
	}
	return frameworkInfoRecord.FrameworkID, nil
}

func (s *Store) getFrameworkInfo(frameworkName string) (*FrameworkInfoRecord, error) {
	queryBuilder := s.DataStore.NewQuery()
	stmt := queryBuilder.Select("*").From(frameworksTable).
		Where(qb.Eq{"FrameworkName": frameworkName})
	result, err := s.DataStore.Execute(context.Background(), stmt)
	if err != nil {
		log.Errorf("Fail to getFrameworkInfo by frameworkName %v, err=%v", frameworkName, err)
		return nil, err
	}
	if result != nil {
		defer result.Close()
	}
	allResults, err := result.All(context.Background())
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

func (s *Store) applyStatements(stmts []api.Statement, jobID string) error {
	err := s.DataStore.ExecuteBatch(context.Background(), stmts)
	if err != nil {
		log.Errorf("Fail to execute %d insert statements for job %v, err=%v", len(stmts), jobID, err)
		return err
	}
	return nil
}

func (s *Store) applyStatement(stmt api.Statement, itemName string) error {
	stmtString, _, _ := stmt.ToSql()
	log.Debugf("stmt=%v", stmtString)
	result, err := s.DataStore.Execute(context.Background(), stmt)
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

func getTaskID(taskInfo *task.TaskInfo) string {
	jobID := taskInfo.JobId.Value
	return fmt.Sprintf(taskIDFmt, jobID, taskInfo.InstanceId)
}
