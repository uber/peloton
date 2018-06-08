package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/gemnasium/migrate/driver/mysql" // Pull in MySQL driver for migrate
	"github.com/gemnasium/migrate/migrate"
	_ "github.com/go-sql-driver/mysql" // Pull in MySQL driver for sqlx
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/yarpcerrors"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/update"
	pb_volume "code.uber.internal/infra/peloton/.gen/peloton/api/volume"

	"code.uber.internal/infra/peloton/storage"
)

const (
	// Table names
	jobsTable         = "jobs"
	jobRuntimeTable   = "job_runtime"
	tasksTable        = "tasks"
	frameworksTable   = "frameworks"
	resourcePoolTable = "respools"

	// MaxDeadlockRetries is how many times a statement will be retried when a deadlock is detected before failing
	MaxDeadlockRetries = 4
	// DeadlockBackoffDuration is how long to sleep between deadlock detection and attempting to commit again
	DeadlockBackoffDuration = 100 * time.Millisecond

	// values for the col_key
	colBaseInfo      = "base_info"
	colJobConfig     = "job_config"
	colResPoolConfig = "respool_config"

	// Various statement templates for job_config
	insertJobStmt        = `INSERT INTO jobs (row_key, col_key, ref_key, body, created_by) values (?, ?, ?, ?, ?)`
	updateJobConfig      = `UPDATE jobs SET body = ? where row_key = ?`
	upsertJobRuntimeStmt = `INSERT INTO job_runtime (row_key, runtime) values (?, ?) ON DUPLICATE KEY UPDATE runtime = ?`
	// TODO: discuss on supporting soft delete.
	deleteJobStmt         = `DELETE from jobs where row_key = ?`
	getJobStmt            = `SELECT * from jobs where col_key = '` + colJobConfig + `' and row_key = ?`
	queryJobsForLabelStmt = `SELECT * from jobs where col_key='` + colJobConfig + `' and match(labels_summary) against (? IN BOOLEAN MODE)`
	getJobsbyOwnerStmt    = `SELECT * from jobs where col_key='` + colJobConfig + `' and owning_team = ?`
	getJobRuntimeStmt     = `SELECT runtime from job_runtime where row_key = ?`
	getJobByState         = `SELECT row_key from job_runtime where job_state = ?`

	// Various statement templates for task runtime_info
	insertTaskStmt            = `INSERT INTO tasks (row_key, col_key, ref_key, body, created_by) values (?, ?, ?, ?, ?)`
	insertTaskBatchStmt       = `INSERT INTO tasks (row_key, col_key, ref_key, body, created_by) VALUES `
	updateTaskStmt            = `UPDATE tasks SET body = ? where row_key = ?`
	getTasksForJobStmt        = `SELECT * from tasks where job_id = ?`
	getTasksCountForJobStmt   = `SELECT COUNT(*) from tasks where job_id = ?`
	getMesosFrameworkInfoStmt = `SELECT * from frameworks where framework_name = ?`
	setMesosStreamIDStmt      = `INSERT INTO frameworks (framework_name, mesos_stream_id, update_host) values (?, ?, ?) ON DUPLICATE KEY UPDATE mesos_stream_id = ?`
	setMesosFrameworkIDStmt   = `INSERT INTO frameworks (framework_name, framework_id, update_host) values (?, ?, ?) ON DUPLICATE KEY UPDATE framework_id = ?`

	// Statements for resource Manager
	insertResPoolStmt = `INSERT INTO respools (row_key, col_key, ref_key, body, created_by) values (?, ?, ?, ?, ?)`
	selectAllResPools = `SELECT * FROM respools`
)

// Filters represents a set of filters for a MySQL query, a filter entry
// consists of a field (the key) and a list of values (the value).
// These will be combined into the where-cause of the query. Each key will be
// required to have one of the values (using logic 'or') in the list for the
// key. These key clauses will then be required to all be true (using
// logic 'and').
type Filters map[string][]interface{}

// Config is the container for database configs
type Config struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Database     string `yaml:"database" validate:"nonzero"`
	Migrations   string `yaml:"migrations"`
	ReadOnly     bool
	Conn         *sqlx.DB
	ConnLifeTime time.Duration `yaml:"conn_lifetime"`
	// MaxBatchSize controls how many updates or inserts are batched together in a single statement
	// this maps to the number of rows updated or inserted at once. This is measured in rows.
	MaxBatchSize int `yaml:"max_batch_size_rows"`
}

// String returns the connection string for the DB
func (d *Config) String() string {
	return fmt.Sprintf(
		"%s:%s@(%s:%d)/%s?parseTime=true",
		d.User,
		d.Password,
		d.Host,
		d.Port,
		d.Database,
	)
}

// IsReadOnly checks if db is a slave
func (d *Config) IsReadOnly() bool {
	// check if it's a slave db
	err := d.Conn.Get(&d.ReadOnly, "SELECT @@global.read_only")
	if err != nil {
		log.Errorf("Failed to check db readonly, err=%v", err)
		return true
	}

	if d.ReadOnly {
		log.Infof("Database %v is read-only.", d.Database)
	} else {
		log.Infof("Database %v is read-write.", d.Database)
	}
	return d.ReadOnly
}

// AutoMigrate brings the schema up to date
func (d *Config) AutoMigrate() []error {
	// If it's read-only, do not run migration
	if d.IsReadOnly() {
		log.Infof("Skipping migration since the database is read-only.")
		return nil
	}

	connString := d.MigrateString()
	errors, ok := migrate.UpSync(connString, d.Migrations)
	if !ok {
		return errors
	}
	return nil
}

// MigrateString returns the db string required for database migration
func (d *Config) MigrateString() string {
	return fmt.Sprintf("mysql://%s", d.String())
}

// Connect is *not* goroutine safe, and should only be called by the
// main app to initialize a connection
func (d *Config) Connect() error {
	dbString := d.String()
	log.Debugf("Connecting to database %s:", dbString)
	db, err := sqlx.Open("mysql", dbString)
	if err != nil {
		return err
	}
	d.Conn = db
	log.Infof("Connected to database %s:", dbString)
	return err
}

// Store implements JobStore / TaskStore / FrameworkStore / respoolStore interfaces using a mysql backend
type Store struct {
	DB      *sqlx.DB
	metrics *storage.Metrics
	Conf    Config
}

// NewStore creates a MysqlJobStore, from a properly initialized config that has
// already established a connection to the DB
func NewStore(config Config, scope tally.Scope) *Store {
	return &Store{
		DB:      config.Conn,
		metrics: storage.NewMetrics(scope.SubScope("storage")),
		Conf:    config,
	}
}

// CreateJob creates a job with the job id and the config value
// TODO: break CreateJob into CreateJobConfig and CreateJobRuntimeWithConfigs
func (m *Store) CreateJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, createdBy string) error {
	buffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.WithError(err).Error("Marshal job config failed")
		m.metrics.JobMetrics.JobCreateFail.Inc(1)
		return err
	}

	now := time.Now().UTC()
	initialJobRuntime := &job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: time.Now().Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
		Revision: &peloton.ChangeLog{
			CreatedAt: uint64(now.UnixNano()),
			UpdatedAt: uint64(now.UnixNano()),
			Version:   1,
		},
	}
	// TODO: make the two following statements in one transaction
	// 0 is for "ref_key" field which is not used as of now
	_, err = m.DB.Exec(insertJobStmt, id.Value, colJobConfig, 0, string(buffer), createdBy)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("CreateJob failed")
		m.metrics.JobMetrics.JobCreateFail.Inc(1)
		return err
	}
	err = m.UpdateJobRuntime(ctx, id, initialJobRuntime)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("Create initial runtime failed")
		m.metrics.JobMetrics.JobCreateFail.Inc(1)
		return err
	}
	m.metrics.JobMetrics.JobCreate.Inc(1)
	return nil
}

// CreateJobRuntimeWithConfig is not yet implemented
func (m *Store) CreateJobRuntimeWithConfig(ctx context.Context, id *peloton.JobID, initialRuntime *job.RuntimeInfo, config *job.JobConfig) error {
	return yarpcerrors.UnimplementedErrorf("CreateJobRuntimeWithConfig is not implemented")
}

// CreateJobConfig is not yet implemented
func (m *Store) CreateJobConfig(ctx context.Context, id *peloton.JobID, config *job.JobConfig, version uint64, createBy string) error {
	return yarpcerrors.UnimplementedErrorf("CreateJobConfig is not implemented")
}

// UpdateJobConfig updates the job config for a given job id
func (m *Store) UpdateJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	buffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.WithError(err).Error("Marshal job config failed")
		m.metrics.JobMetrics.JobUpdateFail.Inc(1)
		return err
	}
	_, err = m.DB.Exec(updateJobConfig, string(buffer), id.Value)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("UpdateJobConfig failed")
		m.metrics.JobMetrics.JobUpdateFail.Inc(1)
		return err
	}

	m.metrics.JobMetrics.JobUpdate.Inc(1)
	return nil
}

// GetJobConfig returns a job config given the job id
func (m *Store) GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error) {
	jobs, err := m.getJobs(Filters{"row_key=": {id.Value}, "col_key=": {colJobConfig}})
	if err != nil {
		return nil, err
	}
	if len(jobs) > 1 {
		return nil, fmt.Errorf("found %d jobs %v for job id %v", len(jobs), jobs, id.Value)
	}
	for _, jobConfig := range jobs {
		return jobConfig, nil
	}
	return nil, fmt.Errorf("GetJobConfig cannot find JobConfig for jobID %v", id.Value)
}

// QueryJobs returns all jobs that contains the Labels.
//
// In the tasks table, the "Labels" field are compacted (all whitespaces and " are removed for each label),
// then stored as the "labels_summary" row. Mysql fulltext index are also set on this field.
// When a query comes, the query labels are compacted in the same way then queried against the fulltext index.
func (m *Store) QueryJobs(ctx context.Context, respoolID *peloton.ResourcePoolID, spec *job.QuerySpec, summaryOnly bool) ([]*job.JobInfo, []*job.JobSummary, uint32, error) {
	return nil, nil, 0, fmt.Errorf("unsupported storage backend")
}

// DeleteJob deletes a job by id
func (m *Store) DeleteJob(ctx context.Context, id *peloton.JobID) error {
	// Check if there are any task left for the job. If there is any, abort the deletion
	// TODO: slu -- discussion on if the task state matter here
	tasks, err := m.GetTasksForJob(ctx, id)
	if err != nil {
		log.Errorf("GetTasksForJob for job id %v failed with error %v", id.Value, err)
		m.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}
	if len(tasks) > 0 {
		err = fmt.Errorf("job id %v still have task runtime records, cannot delete %v", id.Value, tasks)
		m.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(deleteJobStmt, id.Value)
	if err != nil {
		log.Errorf("Delete job id %v failed with error %v", id.Value, err)
		m.metrics.JobMetrics.JobDeleteFail.Inc(1)
		return err
	}
	m.metrics.JobMetrics.JobDelete.Inc(1)
	return nil
}

// GetJobsByOwner returns jobs by owner
func (m *Store) GetJobsByOwner(ctx context.Context, owner string) (map[string]*job.JobConfig, error) {
	return m.getJobs(Filters{"owning_team=": {owner}, "col_key=": {colJobConfig}})
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error) {
	return m.getTasksByInstanceID(Filters{"job_id=": {id.Value}})
}

// GetTaskRuntimesForJobByRange returns the Task RuntimeInfo for batch jobs by
// instance ID range.
func (m *Store) GetTaskRuntimesForJobByRange(ctx context.Context,
	id *peloton.JobID, instanceRange *task.InstanceRange) (map[uint32]*task.RuntimeInfo, error) {
	return nil, errors.New("Not implemented")
}

// GetTasksForJobByRange returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return m.getTasksByInstanceID(Filters{"job_id=": {id.Value}, "instance_id >=": {Range.From}, "instance_id <": {Range.To}})
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error) {
	result, err := m.getTasksByInstanceID(Filters{"row_key=": {taskID}})
	if err != nil {
		return nil, err
	}

	if len(result) > 1 {
		log.Warnf("Found %v records for taskId %v", len(result), taskID)
	}
	// Return the first result
	for _, task := range result {
		return task, nil
	}
	// No record found
	log.WithField("task_id", taskID).Warn("Task not found")
	return nil, &storage.TaskNotFoundError{TaskID: taskID}
}

// GetTaskForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTaskForJob(ctx context.Context, id *peloton.JobID, instanceID uint32) (map[uint32]*task.TaskInfo, error) {
	return m.getTasksByInstanceID(Filters{"job_id=": {id.Value}, "instance_id=": {instanceID}})
}

// CreateTaskRuntime creates a task runtime for a peloton job
// TODO: remove this in favor of CreateTaskRuntimes
func (m *Store) CreateTaskRuntime(ctx context.Context, id *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo, createdBy string) error {
	// TODO: discuss on whether taskID should be part of the taskInfo instead of runtime
	taskInfo := &task.TaskInfo{
		InstanceId: instanceID,
		JobId:      id,
		Runtime:    runtime,
	}
	rowKey := fmt.Sprintf("%s-%d", id.Value, instanceID)
	if taskInfo.InstanceId != instanceID {
		errMsg := fmt.Sprintf("Task %v has instance id %v, different than the instanceID %d expected", rowKey, instanceID, taskInfo.InstanceId)
		log.Errorf(errMsg)
		m.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return fmt.Errorf(errMsg)
	}

	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("error = %v", err)
		m.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return err
	}

	// TODO: adjust when taskInfo pb change to introduce static config and runtime config
	_, err = m.DB.Exec(insertTaskStmt, rowKey, colBaseInfo, 0, string(buffer), createdBy)
	if err != nil {
		log.Errorf("Create task for job %v instance %d failed with error %v", id.Value, instanceID, err)
		m.metrics.TaskMetrics.TaskCreateFail.Inc(1)
		return err
	}
	m.metrics.TaskMetrics.TaskCreate.Inc(1)
	return nil
}

// CreateTaskRuntimes creates rows for a slice of Task runtimes, numbered 0..n
func (m *Store) CreateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo, createdBy string) error {
	taskInfos := []*task.TaskInfo{}
	for instanceID, runtime := range runtimes {
		taskinfo := &task.TaskInfo{
			InstanceId: uint32(instanceID),
			JobId:      id,
			Runtime:    runtime,
		}
		taskInfos = append(taskInfos, taskinfo)
	}
	timeStart := time.Now()
	maxBatchSize := int64(m.Conf.MaxBatchSize)
	if maxBatchSize == 0 {
		// TODO(gabe) move this into config parsing to ensure valid default values?
		maxBatchSize = math.MaxInt64
	}
	nTasks := int64(len(taskInfos))
	tasksNotCreated := int64(0)
	wg := new(sync.WaitGroup)
	nBatches := nTasks/maxBatchSize + 1
	// use MaxBatchSize to batch updates in smaller chunks, rather than blasting all
	// tasks into DB in single insert
	for batch := int64(0); batch < nBatches; batch++ {
		// do batching by rows, up to m.Conf.MaxBatchSize
		params := []string{}          // the list of parameters for the insert function
		values := []interface{}{}     // the list of parameters for the insert statement
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
			defer wg.Done()
			for i := start; i < end; i++ {
				t := taskInfos[i]
				rowKey := fmt.Sprintf("%s-%d", id.Value, t.InstanceId)
				params = append(params, `(?, ?, ?, ?, ?)`)
				buffer, err := json.Marshal(t)
				if err != nil {
					log.Errorf("Unable to marshal task %v error = %v", rowKey, err)
					m.metrics.TaskMetrics.TaskCreateFail.Inc(batchSize)
					atomic.AddInt64(&tasksNotCreated, batchSize)
					return
				}
				values = append(values, rowKey, colBaseInfo, 0, string(buffer), createdBy)
			}

			insertStatement := insertTaskBatchStmt + strings.Join(params, ", ")
			var err error
			for retries := 1; retries <= MaxDeadlockRetries; retries++ {
				_, err = m.DB.Exec(insertStatement, values...)
				if err != nil {
					if match, _ := regexp.MatchString(".*Deadlock found.*", err.Error()); match {
						// attempt to detect and reexecute deadlock txs
						log.WithField("jobid", id.Value).
							WithField("error", err.Error()).
							WithField("task_range_start", start).
							WithField("task_range_end", end-1).
							WithField("tasks", batchSize).
							WithField("retry", retries).
							WithField("retry_max", MaxDeadlockRetries).
							Warnf("Deadlock detected creating task batch in DB, retrying in %v", DeadlockBackoffDuration)
						time.Sleep(DeadlockBackoffDuration)
						continue
					}
				}
				break
			}
			if err != nil {
				log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
					WithField("jobid", id.Value).
					WithField("error", err.Error()).
					WithField("task_range_start", start).
					WithField("task_range_end", end-1).
					WithField("tasks", batchSize).
					Errorf("Writing task batch to DB failed in %v", time.Since(batchTimeStart))
				m.metrics.TaskMetrics.TaskCreateFail.Inc(batchSize)
				atomic.AddInt64(&tasksNotCreated, batchSize)
				return
			}
			log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
				WithField("jobid", id.Value).
				WithField("task_range_from", start).
				WithField("task_range_end", end-1).
				WithField("tasks", batchSize).
				Debugf("Wrote tasks to DB in %v", time.Since(batchTimeStart))
			m.metrics.TaskMetrics.TaskCreate.Inc(batchSize)
		}()
	}
	wg.Wait()
	if tasksNotCreated != 0 {
		log.WithField("duration_s", time.Since(timeStart).Seconds()).
			WithField("jobid", id.Value).
			WithField("tasks_created", nTasks-tasksNotCreated).
			WithField("tasks_not_created", tasksNotCreated).
			Errorf("Unable to write some tasks to DB in %v", time.Since(timeStart))
		return fmt.Errorf("Only %d of %d tasks for %v were written successfully in DB", tasksNotCreated, nTasks, id.Value)
	}
	log.WithField("duration_s", time.Since(timeStart).Seconds()).
		WithField("tasks", nTasks).
		WithField("jobid", id.Value).
		Infof("Wrote all tasks to DB in %v", time.Since(timeStart))
	return nil
}

// GetTasksForHosts returns all tasks for a list of hosts.
func (m *Store) GetTasksForHosts(ctx context.Context, hosts []string) (map[string][]*task.TaskInfo, error) {
	hostsList := make([]interface{}, 0, len(hosts))
	for _, h := range hosts {
		hostsList = append(hostsList, h)
	}
	tasks, err := m.getTasks(Filters{"task_host=": hostsList})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]*task.TaskInfo, len(tasks))
	for _, t := range tasks {
		if _, exists := result[t.Runtime.Host]; !exists {
			result[t.Runtime.Host] = []*task.TaskInfo{}
		}
		result[t.Runtime.Host] = append(result[t.Runtime.Host], t)
	}
	return result, nil
}

// GetTaskStateSummaryForJob returns the tasks count (runtime_config) for a peloton job with certain state.
func (m *Store) GetTaskStateSummaryForJob(ctx context.Context, id *peloton.JobID) (map[string]uint32, error) {
	return nil, fmt.Errorf("unimplemented GetTaskStateSummaryForJob")
}

// GetTaskConfig returns the task specific config
func (m *Store) GetTaskConfig(ctx context.Context, id *peloton.JobID,
	instanceID uint32, version uint64) (*task.TaskConfig, error) {
	return nil, fmt.Errorf("unimplemented GetTaskConfig")
}

// GetTaskConfigs returns the task specific config for a list of instances
func (m *Store) GetTaskConfigs(ctx context.Context, id *peloton.JobID,
	instanceIDs []uint32, version uint64) (map[uint32]*task.TaskConfig, error) {
	return nil, fmt.Errorf("unimplemented GetTaskConfigs")
}

// CreateTaskConfigs creates task configurations.
func (m *Store) CreateTaskConfigs(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	return fmt.Errorf("unimplemented CreateTaskConfigs")
}

// GetTasksForJobAndStates returns the tasks (runtime_config) for a peloton job with certain states
func (m *Store) GetTasksForJobAndStates(ctx context.Context, id *peloton.JobID, states []task.TaskState) (map[uint32]*task.TaskInfo, error) {
	var taskStates []string
	for _, state := range states {
		taskStates = append(taskStates, state.String())
	}
	return m.getTasksByInstanceID(Filters{"job_id=": {id.Value}, "task_states=": {taskStates}})
}

// GetTaskIDsForJobAndState returns a list of instance-ids for a peloton job with certain state.
func (m *Store) GetTaskIDsForJobAndState(ctx context.Context, id *peloton.JobID, state string) ([]uint32, error) {
	return nil, fmt.Errorf("unimplemented GetTaskIDsForJobAndState")
}

// GetTaskRuntime returns the task runtime peloton task
func (m *Store) GetTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32) (*task.RuntimeInfo, error) {
	return nil, fmt.Errorf("unimplemented GetTaskRuntime")
}

// UpdateTaskRuntime updates a task for a peloton job
func (m *Store) UpdateTaskRuntime(ctx context.Context, jobID *peloton.JobID, instanceID uint32, runtime *task.RuntimeInfo) error {
	rowKey := fmt.Sprintf("%s-%d", jobID.GetValue(), instanceID)
	taskinfo := &task.TaskInfo{
		InstanceId: instanceID,
		JobId:      jobID,
		Runtime:    runtime,
	}
	buffer, err := json.Marshal(taskinfo)

	if err != nil {
		log.Errorf("error = %v", err)
		m.metrics.TaskMetrics.TaskUpdateFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(updateTaskStmt, string(buffer), rowKey)
	if err != nil {
		log.Errorf("Update task for job %v instance %d failed with error %v", jobID.GetValue(), instanceID, err)
		m.metrics.TaskMetrics.TaskUpdateFail.Inc(1)
		return err
	}

	m.metrics.TaskMetrics.TaskUpdate.Inc(1)
	return nil
}

// getQueryAndArgs returns the SQL query along with the bind args
func getQueryAndArgs(table string, filters Filters, fields []string) (string, []interface{}) {
	var args []interface{}
	q := "SELECT"
	for _, field := range fields {
		q = q + " " + field
	}
	q = q + " FROM " + table
	filterKeys := make([]string, 0, len(filters))
	for key := range filters {
		filterKeys = append(filterKeys, key)
	}
	sort.Strings(filterKeys)
	if len(filters) > 0 {
		q = q + " WHERE "
		for i, field := range filterKeys {
			values := filters[field]
			q = q + "("
			for j, value := range values {
				q = q + field + " ?"
				args = append(args, value)
				if j < len(values)-1 {
					q = q + " or "
				}
			}
			q = q + ")"
			if i < len(filters)-1 {
				q = q + " and "
			}
		}
	}
	return q, args
}

func (m *Store) getJobRecords(filters Filters) ([]JobRecord, error) {
	var records = []JobRecord{}
	q, args := getQueryAndArgs(jobsTable, filters, []string{"*"})
	log.Debugf("DB query -- %v %v", q, args)
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getJobs for filters %v returns no rows", filters)
		m.metrics.JobMetrics.JobGetFail.Inc(1)
		return records, nil
	}

	if err != nil {
		log.Errorf("getJobs for filter %v failed with error %v", filters, err)
		m.metrics.JobMetrics.JobGetFail.Inc(1)
		return nil, err
	}
	return records, nil
}

func (m *Store) getJobs(filters Filters) (map[string]*job.JobConfig, error) {
	var result = make(map[string]*job.JobConfig)
	records, err := m.getJobRecords(filters)
	if err != nil {
		return nil, err
	}

	for _, jobRecord := range records {
		jobConfig, err := jobRecord.GetJobConfig()
		if err != nil {
			log.Errorf("jobRecord %v GetJobConfig failed, err=%v", jobRecord, err)
			m.metrics.JobMetrics.JobGetFail.Inc(1)
			return nil, err
		}
		result[jobRecord.RowKey] = jobConfig
	}
	m.metrics.JobMetrics.JobGet.Inc(1)
	return result, nil
}

func (m *Store) getTasks(filters Filters) ([]*task.TaskInfo, error) {
	var records = []TaskRecord{}
	var result []*task.TaskInfo
	q, args := getQueryAndArgs(tasksTable, filters, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getTasksByInstanceID for filters %v returns no rows", filters)
		m.metrics.TaskMetrics.TaskGetFail.Inc(1)
		return result, nil
	}
	if err != nil {
		log.Errorf("getTasksByInstanceID for filter %v failed with error %v", filters, err)
		m.metrics.TaskMetrics.TaskGetFail.Inc(1)
		return nil, err
	}
	for _, taskRecord := range records {
		taskInfo, err := taskRecord.GetTaskInfo()
		if err != nil {
			log.Errorf("taskRecord %v GetTaskInfo failed, err=%v", taskRecord, err)
			m.metrics.TaskMetrics.TaskGetFail.Inc(1)
			return nil, err
		}
		result = append(result, taskInfo)
	}
	m.metrics.TaskMetrics.TaskGet.Inc(1)
	return result, nil
}

func (m *Store) getTasksByInstanceID(filters Filters) (map[uint32]*task.TaskInfo, error) {
	var result = make(map[uint32]*task.TaskInfo)
	tasks, err := m.getTasks(filters)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		result[task.InstanceId] = task
	}
	return result, nil
}

//SetMesosStreamID stores the mesos stream id for a framework name
func (m *Store) SetMesosStreamID(ctx context.Context, frameworkName string, mesosStreamID string) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("os.HostName() failed with err=%v", err)
	}
	_, err = m.DB.Exec(setMesosStreamIDStmt, frameworkName, mesosStreamID, hostname, mesosStreamID)
	if err != nil {
		log.Errorf("SetMesosStreamId failed with framework named %v error = %v", frameworkName, err)
		return err
	}
	return nil
}

//SetMesosFrameworkID stores the mesos framework id for a framework name
func (m *Store) SetMesosFrameworkID(ctx context.Context, frameworkName string, frameworkID string) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("os.HostName() failed with err=%v", err)
	}
	_, err = m.DB.Exec(setMesosFrameworkIDStmt, frameworkName, frameworkID, hostname, frameworkID)
	if err != nil {
		log.Errorf("SetMesosFrameworkId failed with id %v error = %v", frameworkName, err)
		return err
	}
	return nil
}

//GetMesosStreamID reads the mesos stream id for a framework name
func (m *Store) GetMesosStreamID(ctx context.Context, frameworkName string) (string, error) {
	var records = []MesosFrameworkInfo{}
	q, args := getQueryAndArgs(frameworksTable, Filters{"framework_name=": {frameworkName}}, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetMesosStreamId for frmeworkName %v returns no rows", frameworkName)
		return "", nil
	}
	for _, frameworkInfo := range records {
		return frameworkInfo.MesosStreamID.String, nil
	}
	return "", nil
}

// GetFrameworkID reads the framework id for a framework name
func (m *Store) GetFrameworkID(ctx context.Context, frameworkName string) (string, error) {
	var records = []MesosFrameworkInfo{}
	q, args := getQueryAndArgs(frameworksTable, Filters{"framework_name=": {frameworkName}}, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetFrameworkId for frameworkName %v returns no rows", frameworkName)
		return "", nil
	}
	for _, frameworkInfo := range records {
		return frameworkInfo.FrameworkID.String, nil
	}
	return "", nil
}

// GetAllJobs returns all jobs
func (m *Store) GetAllJobs(ctx context.Context) (map[string]*job.RuntimeInfo, error) {
	return nil, errors.New("unimplemented")
}

// GetTaskEvents returns the events list for a task
func (m *Store) GetTaskEvents(ctx context.Context, jobID *peloton.JobID, instanceID uint32) ([]*task.TaskEvent, error) {
	return nil, errors.New("unimplemented")
}

// CreateResourcePool creates a resource pool with the resource pool id and the config value
// TODO: Need to create test case
func (m *Store) CreateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, respoolConfig *respool.ResourcePoolConfig, createdBy string) error {
	buffer, err := json.Marshal(respoolConfig)
	if err != nil {
		log.Errorf("error = %v", err)
		// Need to add metrics for respool creation fail
		return err
	}

	// TODO: Add check for Parent checking
	_, err = m.DB.Exec(insertResPoolStmt, id.Value, colResPoolConfig, 0, string(buffer), createdBy)
	if err != nil {
		log.WithFields(log.Fields{
			"ID":    id.Value,
			"Error": err,
		}).Error("Create Resource Pool failed")
		// Need to add metrics for respool creation fail
		return err
	}
	// Need to add metrics for respool creation succeded
	return nil
}

// GetResourcePool gets a resource pool info object
func (m *Store) GetResourcePool(ctx context.Context, id *peloton.ResourcePoolID) (*respool.ResourcePoolInfo, error) {
	return nil, errors.New("unimplemented")
}

// DeleteResourcePool Deletes the resource pool
func (m *Store) DeleteResourcePool(ctx context.Context, id *peloton.ResourcePoolID) error {
	return errors.New("unimplemented")
}

// UpdateResourcePool Update the resource pool
func (m *Store) UpdateResourcePool(ctx context.Context, id *peloton.ResourcePoolID, Config *respool.ResourcePoolConfig) error {
	return errors.New("unimplemented")
}

// GetResourcePoolsByOwner gets resource pool(s) by owner
func (m *Store) GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error) {
	var records = []ResourcePoolRecord{}
	var result = make(map[string]*respool.ResourcePoolConfig)
	q, args := getQueryAndArgs(resourcePoolTable, Filters{"created_by=": {owner}}, []string{"*"})
	log.WithFields(log.Fields{
		"Query": q,
		"Args":  args,
	}).Debug("DB query")
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetResourcePoolsByOwner returns no rows")
		// TODO: Adding metrics
		return result, nil
	}
	if err != nil {
		log.WithField("Error", err).Error("GetResourcePoolsByOwner failed")
		return nil, err
	}
	for _, resPoolRecord := range records {
		resPoolConfig, err := resPoolRecord.GetResPoolConfig()
		if err != nil {
			log.WithFields(log.Fields{
				"resPoolRecord": resPoolRecord,
				"Error ":        err,
			}).Error("GetResPoolConfig failed")
			// TODO: Adding metrics
			return nil, err
		}
		result[resPoolRecord.RowKey] = resPoolConfig
	}
	// TODO: Adding metrics
	return result, nil
}

// GetAllResourcePools Get all the resource pool
func (m *Store) GetAllResourcePools(ctx context.Context) (map[string]*respool.ResourcePoolConfig, error) {
	var records = []ResourcePoolRecord{}
	var result = make(map[string]*respool.ResourcePoolConfig)
	q, args := getQueryAndArgs(resourcePoolTable, Filters{}, []string{"*"})
	log.WithFields(log.Fields{
		"Query": q,
		"Args":  args,
	}).Debug("DB query")
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetAllResourcePools returns no rows")
		// TODO: Adding metrics
		return result, nil
	}
	if err != nil {
		log.WithField("Error", err).Error("GetAllResourcePools failed")
		return nil, err
	}
	for _, resPoolRecord := range records {
		resPoolConfig, err := resPoolRecord.GetResPoolConfig()
		if err != nil {
			log.WithFields(log.Fields{
				"resPoolRecord": resPoolRecord,
				"Error ":        err,
			}).Error("GetResPoolConfig failed")
			// TODO: Adding metrics
			return nil, err
		}
		result[resPoolRecord.RowKey] = resPoolConfig
	}
	// TODO: Adding metrics
	return result, nil
}

// GetJobRuntime returns the job runtime info
func (m *Store) GetJobRuntime(ctx context.Context, id *peloton.JobID) (*job.RuntimeInfo, error) {
	var records = []JobRuntimeRecord{}

	q, args := getQueryAndArgs(jobRuntimeTable, Filters{"row_key=": {id.Value}}, []string{"runtime"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetJobRuntime returns no rows")
		return nil, nil
	} else if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("Failed to GetJobRuntime")
		m.metrics.JobMetrics.JobGetRuntimeFail.Inc(1)
		return nil, err
	}

	if len(records) > 1 {
		m.metrics.JobMetrics.JobGetRuntimeFail.Inc(1)
		return nil, fmt.Errorf("found %d jobs %v for job id %v", len(records), records, id.Value)
	}
	for _, record := range records {
		runtime, err := record.GetJobRuntime()
		if err != nil {
			m.metrics.JobMetrics.JobGetRuntimeFail.Inc(1)
			return nil, err
		}
		m.metrics.JobMetrics.JobGetRuntime.Inc(1)
		return runtime, nil
	}
	return nil, fmt.Errorf("GetJobRuntime cannot find runtime for jobID %v", id.Value)
}

// GetJobsByStates returns the jobIDs by job states
func (m *Store) GetJobsByStates(ctx context.Context, state []job.JobState) ([]peloton.JobID, error) {
	return nil, nil
}

// GetJobsByState returns the jobID by job state
func (m *Store) GetJobsByState(ctx context.Context, state job.JobState) ([]peloton.JobID, error) {
	var records = []JobRuntimeRecord{}
	var result []peloton.JobID

	q, args := getQueryAndArgs(jobRuntimeTable, Filters{"job_state=": {state}}, []string{"row_key"})

	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.WithField("job_state", state).Warn("GetJobsByState returns no rows")
		return result, nil
	} else if err != nil {
		log.WithError(err).WithField("job_state", state).Error("Failed to GetJobsByState")
		m.metrics.JobMetrics.JobGetByStatesFail.Inc(1)
		return nil, err
	}

	for _, record := range records {
		result = append(result, peloton.JobID{
			Value: record.RowKey,
		})
	}
	m.metrics.JobMetrics.JobGetByStates.Inc(1)
	return result, nil
}

// UpdateJobRuntime updates the job runtime info
func (m *Store) UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error {
	buffer, err := json.Marshal(runtime)
	if err != nil {
		log.WithError(err).Error("Failed to marshal job runtime")
		m.metrics.JobMetrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(upsertJobRuntimeStmt, id.Value, string(buffer), string(buffer))
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("Failed to update job runtime")
		m.metrics.JobMetrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}
	m.metrics.JobMetrics.JobUpdateRuntime.Inc(1)
	return nil
}

// QueryTasks returns all tasks in the given [offset...offset+limit) range.
func (m *Store) QueryTasks(ctx context.Context, id *peloton.JobID, spec *task.QuerySpec) ([]*task.TaskInfo, uint32, error) {
	return nil, 0, fmt.Errorf("unsupported storage backend")
}

// CreatePersistentVolume creates a persistent volume entry.
func (m *Store) CreatePersistentVolume(ctx context.Context, volume *pb_volume.PersistentVolumeInfo) error {
	return errors.New("Not implemented")
}

// UpdateTaskRuntimes updates task runtimes for the given slice of task runtimes, instances 0..n
func (m *Store) UpdateTaskRuntimes(ctx context.Context, id *peloton.JobID, runtimes map[uint32]*task.RuntimeInfo) error {
	return errors.New("Not implemented")
}

// UpdatePersistentVolume update state for a persistent volume.
func (m *Store) UpdatePersistentVolume(ctx context.Context, volume *pb_volume.PersistentVolumeInfo) error {
	return errors.New("Not implemented")
}

// GetPersistentVolume gets the persistent volume object.
func (m *Store) GetPersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) (*pb_volume.PersistentVolumeInfo, error) {
	return nil, errors.New("Not implemented")
}

// DeletePersistentVolume delete persistent volume entry.
func (m *Store) DeletePersistentVolume(ctx context.Context, volumeID *peloton.VolumeID) error {
	return errors.New("Not implemented")
}

// GetJobsByRespoolID returns jobIDs in a respool
func (m *Store) GetJobsByRespoolID(ctx context.Context, respoolID *peloton.ResourcePoolID) (map[string]*job.JobConfig, error) {
	return nil, errors.New("Not implemented")
}

// CreateUpdate by creating a new update in the storage. It's an error
// if the update already exists.
func (m *Store) CreateUpdate(ctx context.Context, id *update.UpdateID, jobID *peloton.JobID, jobConfig *job.JobConfig, updateConfig *update.UpdateConfig) error {
	return errors.New("Not implemented")
}

// GetUpdateProgress returns the progress of the update.
func (m *Store) GetUpdateProgress(ctx context.Context, id *update.UpdateID) (processing []uint32, progress uint32, err error) {
	return nil, 0, errors.New("Not implemented")
}

// CreateSecret stores a secret in the secret_info table
func (m *Store) CreateSecret(ctx context.Context, secret *peloton.Secret, id *peloton.JobID) error {
	return errors.New("Not implemented")
}

// GetSecret gets a secret from the secret_info table
func (m *Store) GetSecret(ctx context.Context, id *peloton.SecretID) (*peloton.Secret, error) {
	return nil, errors.New("Not implemented")
}

// GetMaxJobConfigVersion is not yet implemented
func (m *Store) GetMaxJobConfigVersion(
	ctx context.Context,
	id *peloton.JobID,
) (uint64, error) {
	return 0, yarpcerrors.UnimplementedErrorf("GetMaxJobConfigVersion is not implemented")
}
