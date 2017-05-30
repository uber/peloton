package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	_ "github.com/gemnasium/migrate/driver/mysql" // Pull in MySQL driver for migrate
	"github.com/gemnasium/migrate/migrate"
	_ "github.com/go-sql-driver/mysql" // Pull in MySQL driver for sqlx
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"

	mesos_v1 "code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
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
	metrics storage.Metrics
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
func (m *Store) CreateJob(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig, createdBy string) error {
	buffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.WithError(err).Error("Marshal job config failed")
		m.metrics.JobCreateFail.Inc(1)
		return err
	}

	initialJobRuntime := &job.RuntimeInfo{
		State:        job.JobState_INITIALIZED,
		CreationTime: time.Now().Format(time.RFC3339Nano),
		TaskStats:    make(map[string]uint32),
	}
	// TODO: make the two following statements in one transaction
	// 0 is for "ref_key" field which is not used as of now
	_, err = m.DB.Exec(insertJobStmt, id.Value, colJobConfig, 0, string(buffer), createdBy)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("CreateJob failed")
		m.metrics.JobCreateFail.Inc(1)
		return err
	}
	err = m.UpdateJobRuntime(ctx, id, initialJobRuntime)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("Create initial runtime failed")
		m.metrics.JobCreateFail.Inc(1)
		return err
	}
	m.metrics.JobCreate.Inc(1)
	return nil
}

// UpdateJobConfig updates the job config for a given job id
func (m *Store) UpdateJobConfig(ctx context.Context, id *peloton.JobID, jobConfig *job.JobConfig) error {
	buffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.WithError(err).Error("Marshal job config failed")
		m.metrics.JobUpdateFail.Inc(1)
		return err
	}
	_, err = m.DB.Exec(updateJobConfig, string(buffer), id.Value)
	if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("UpdateJobConfig failed")
		m.metrics.JobUpdateFail.Inc(1)
		return err
	}

	m.metrics.JobUpdate.Inc(1)
	return nil
}

// GetJobConfig returns a job config given the job id
func (m *Store) GetJobConfig(ctx context.Context, id *peloton.JobID) (*job.JobConfig, error) {
	jobs, err := m.getJobs(map[string]interface{}{"row_key=": id.Value, "col_key=": colJobConfig})
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

// Query returns all jobs that contains the Labels.
//
// In the tasks table, the "Labels" field are compacted (all whitespaces and " are removed for each label),
// then stored as the "labels_summary" row. Mysql fulltext index are also set on this field.
// When a query comes, the query labels are compacted in the same way then queried against the fulltext index.
func (m *Store) Query(ctx context.Context, Labels *mesos_v1.Labels, keywords []string) (map[string]*job.JobConfig, error) {
	if Labels == nil || len(Labels.Labels) == 0 {
		log.Debug("Labels is empty, return all jobs")
		return m.GetAllJobs(ctx)
	}

	var queryLabels = ""
	records := []JobRecord{}
	var result = make(map[string]*job.JobConfig)
	for _, label := range Labels.Labels {
		buffer, err := json.Marshal(label)
		if err != nil {
			log.Errorf("%v error %v", label, err)
			return nil, err
		}
		// Remove all the " and spaces. This will help when searching by subset of labels
		text := strings.Replace(strings.Replace(string(buffer), "\"", "", -1), " ", "", -1)
		queryLabels = queryLabels + "+\"" + text + "\""
	}

	log.Debugf("Querying using labels %v, text (%v)", Labels, queryLabels)
	err := m.DB.Select(&records, queryJobsForLabelStmt, queryLabels)
	if err == sql.ErrNoRows {
		log.Warnf("Query for Label %v returns no rows", Labels)
		return nil, nil
	}
	if err != nil {
		log.Errorf("Query for labels %v failed with error %v", queryLabels, err)
		return nil, err
	}
	for _, record := range records {
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.Errorf("Query jobs %v failed with error %v", queryLabels, err)
			continue
		}
		result[record.RowKey] = jobConfig
	}
	return result, nil
}

// DeleteJob deletes a job by id
func (m *Store) DeleteJob(ctx context.Context, id *peloton.JobID) error {
	// Check if there are any task left for the job. If there is any, abort the deletion
	// TODO: slu -- discussion on if the task state matter here
	tasks, err := m.GetTasksForJob(ctx, id)
	if err != nil {
		log.Errorf("GetTasksForJob for job id %v failed with error %v", id.Value, err)
		m.metrics.JobDeleteFail.Inc(1)
		return err
	}
	if len(tasks) > 0 {
		err = fmt.Errorf("job id %v still have task runtime records, cannot delete %v", id.Value, tasks)
		m.metrics.JobDeleteFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(deleteJobStmt, id.Value)
	if err != nil {
		log.Errorf("Delete job id %v failed with error %v", id.Value, err)
		m.metrics.JobDeleteFail.Inc(1)
		return err
	}
	m.metrics.JobDelete.Inc(1)
	return nil
}

// GetJobsByOwner returns jobs by owner
func (m *Store) GetJobsByOwner(ctx context.Context, owner string) (map[string]*job.JobConfig, error) {
	return m.getJobs(map[string]interface{}{"owning_team=": owner, "col_key=": colJobConfig})
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTasksForJob(ctx context.Context, id *peloton.JobID) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value})
}

// GetTasksForJobByRange returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTasksForJobByRange(ctx context.Context, id *peloton.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "instance_id >=": Range.From, "instance_id <": Range.To})
}

// GetTaskByID returns the tasks (tasks.TaskInfo) for a peloton job
func (m *Store) GetTaskByID(ctx context.Context, taskID string) (*task.TaskInfo, error) {
	result, err := m.getTasks(map[string]interface{}{"row_key=": taskID})
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
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "instance_id=": instanceID})
}

// CreateTask creates a task for a peloton job
// TODO: remove this in favor of CreateTasks
func (m *Store) CreateTask(ctx context.Context, id *peloton.JobID, instanceID uint32, taskInfo *task.TaskInfo, createdBy string) error {
	// TODO: discuss on whether taskID should be part of the taskInfo instead of runtime
	rowKey := fmt.Sprintf("%s-%d", id.Value, instanceID)
	if taskInfo.InstanceId != instanceID {
		errMsg := fmt.Sprintf("Task %v has instance id %v, different than the instanceID %d expected", rowKey, instanceID, taskInfo.InstanceId)
		log.Errorf(errMsg)
		m.metrics.TaskCreateFail.Inc(1)
		return fmt.Errorf(errMsg)
	}

	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("error = %v", err)
		m.metrics.TaskCreateFail.Inc(1)
		return err
	}

	// TODO: adjust when taskInfo pb change to introduce static config and runtime config
	_, err = m.DB.Exec(insertTaskStmt, rowKey, colBaseInfo, 0, string(buffer), createdBy)
	if err != nil {
		log.Errorf("Create task for job %v instance %d failed with error %v", id.Value, instanceID, err)
		m.metrics.TaskCreateFail.Inc(1)
		return err
	}
	m.metrics.TaskCreate.Inc(1)
	return nil
}

// CreateTasks creates rows for a slice of Tasks, numbered 0..n
func (m *Store) CreateTasks(ctx context.Context, id *peloton.JobID, taskInfos []*task.TaskInfo, createdBy string) error {
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
					m.metrics.TaskCreateFail.Inc(batchSize)
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
				m.metrics.TaskCreateFail.Inc(batchSize)
				atomic.AddInt64(&tasksNotCreated, batchSize)
				return
			}
			log.WithField("duration_s", time.Since(batchTimeStart).Seconds()).
				WithField("jobid", id.Value).
				WithField("task_range_from", start).
				WithField("task_range_end", end-1).
				WithField("tasks", batchSize).
				Debugf("Wrote tasks to DB in %v", time.Since(batchTimeStart))
			m.metrics.TaskCreate.Inc(batchSize)
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

// GetTasksForJobAndState returns the tasks (runtime_config) for a peloton job with certain state
func (m *Store) GetTasksForJobAndState(ctx context.Context, id *peloton.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "task_state=": state})
}

// UpdateTask updates a task for a peloton job
func (m *Store) UpdateTask(ctx context.Context, taskInfo *task.TaskInfo) error {
	rowKey := fmt.Sprintf("%s-%d", taskInfo.JobId.Value, taskInfo.InstanceId)
	buffer, err := json.Marshal(taskInfo)

	if err != nil {
		log.Errorf("error = %v", err)
		m.metrics.TaskUpdateFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(updateTaskStmt, string(buffer), rowKey)
	if err != nil {
		log.Errorf("Update task for job %v instance %d failed with error %v", taskInfo.JobId.Value, taskInfo.InstanceId, err)
		m.metrics.TaskUpdateFail.Inc(1)
		return err
	}

	m.metrics.TaskUpdate.Inc(1)
	return nil
}

// getQueryAndArgs returns the SQL query along with the bind args
func getQueryAndArgs(table string, filters map[string]interface{}, fields []string) (string, []interface{}) {
	var args []interface{}
	q := "SELECT"
	for _, field := range fields {
		q = q + " " + field
	}
	q = q + " FROM " + table
	if len(filters) > 0 {
		q = q + " where "
		var i = 0
		for field, value := range filters {
			q = q + field + " ? "
			args = append(args, value)
			if i < len(filters)-1 {
				q = q + "and "
			}
			i++
		}
	}
	return q, args
}

func (m *Store) getJobRecords(filters map[string]interface{}) ([]JobRecord, error) {
	var records = []JobRecord{}
	q, args := getQueryAndArgs(jobsTable, filters, []string{"*"})
	log.Debugf("DB query -- %v %v", q, args)
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getJobs for filters %v returns no rows", filters)
		m.metrics.JobGetFail.Inc(1)
		return records, nil
	}

	if err != nil {
		log.Errorf("getJobs for filter %v failed with error %v", filters, err)
		m.metrics.JobGetFail.Inc(1)
		return nil, err
	}
	return records, nil
}

func (m *Store) getJobs(filters map[string]interface{}) (map[string]*job.JobConfig, error) {
	var result = make(map[string]*job.JobConfig)
	records, err := m.getJobRecords(filters)
	if err != nil {
		return nil, err
	}

	for _, jobRecord := range records {
		jobConfig, err := jobRecord.GetJobConfig()
		if err != nil {
			log.Errorf("jobRecord %v GetJobConfig failed, err=%v", jobRecord, err)
			m.metrics.JobGetFail.Inc(1)
			return nil, err
		}
		result[jobRecord.RowKey] = jobConfig
	}
	m.metrics.JobGet.Inc(1)
	return result, nil
}

func (m *Store) getTasks(filters map[string]interface{}) (map[uint32]*task.TaskInfo, error) {
	var records = []TaskRecord{}
	var result = make(map[uint32]*task.TaskInfo)
	q, args := getQueryAndArgs(tasksTable, filters, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getTasks for filters %v returns no rows", filters)
		m.metrics.TaskGetFail.Inc(1)
		return result, nil
	}
	if err != nil {
		log.Errorf("getTasks for filter %v failed with error %v", filters, err)
		m.metrics.TaskGetFail.Inc(1)
		return nil, err
	}
	for _, taskRecord := range records {
		taskInfo, err := taskRecord.GetTaskInfo()
		if err != nil {
			log.Errorf("taskRecord %v GetTaskInfo failed, err=%v", taskRecord, err)
			m.metrics.TaskGetFail.Inc(1)
			return nil, err
		}
		result[uint32(taskRecord.InstanceID)] = taskInfo
	}
	m.metrics.TaskGet.Inc(1)
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
	q, args := getQueryAndArgs(frameworksTable, map[string]interface{}{"framework_name=": frameworkName}, []string{"*"})
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
	q, args := getQueryAndArgs(frameworksTable, map[string]interface{}{"framework_name=": frameworkName}, []string{"*"})
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
func (m *Store) GetAllJobs(ctx context.Context) (map[string]*job.JobConfig, error) {
	return m.getJobs(map[string]interface{}{})
}

// CreateResourcePool creates a resource pool with the resource pool id and the config value
// TODO: Need to create test case
func (m *Store) CreateResourcePool(ctx context.Context, id *respool.ResourcePoolID, respoolConfig *respool.ResourcePoolConfig, createdBy string) error {
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
func (m *Store) GetResourcePool(ctx context.Context, id *respool.ResourcePoolID) (*respool.ResourcePoolInfo, error) {
	return nil, errors.New("unimplemented")
}

// DeleteResourcePool Deletes the resource pool
func (m *Store) DeleteResourcePool(ctx context.Context, id *respool.ResourcePoolID) error {
	return errors.New("unimplemented")
}

// UpdateResourcePool Update the resource pool
func (m *Store) UpdateResourcePool(ctx context.Context, id *respool.ResourcePoolID, Config *respool.ResourcePoolConfig) error {
	return errors.New("unimplemented")
}

// GetResourcePoolsByOwner gets resource pool(s) by owner
func (m *Store) GetResourcePoolsByOwner(ctx context.Context, owner string) (map[string]*respool.ResourcePoolConfig, error) {
	var records = []ResourcePoolRecord{}
	var result = make(map[string]*respool.ResourcePoolConfig)
	q, args := getQueryAndArgs(resourcePoolTable, map[string]interface{}{"created_by=": owner}, []string{"*"})
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
	q, args := getQueryAndArgs(resourcePoolTable, map[string]interface{}{}, []string{"*"})
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

	q, args := getQueryAndArgs(jobRuntimeTable, map[string]interface{}{"row_key=": id.Value}, []string{"runtime"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetJobRuntime returns no rows")
		return nil, nil
	} else if err != nil {
		log.WithError(err).WithField("job_id", id.Value).Error("Failed to GetJobRuntime")
		m.metrics.JobGetRuntimeFail.Inc(1)
		return nil, err
	}

	if len(records) > 1 {
		m.metrics.JobGetRuntimeFail.Inc(1)
		return nil, fmt.Errorf("found %d jobs %v for job id %v", len(records), records, id.Value)
	}
	for _, record := range records {
		runtime, err := record.GetJobRuntime()
		if err != nil {
			m.metrics.JobGetRuntimeFail.Inc(1)
			return nil, err
		}
		m.metrics.JobGetRuntime.Inc(1)
		return runtime, nil
	}
	return nil, fmt.Errorf("GetJobRuntime cannot find runtime for jobID %v", id.Value)
}

// GetJobsByState returns the jobID by job state
func (m *Store) GetJobsByState(ctx context.Context, state job.JobState) ([]peloton.JobID, error) {
	var records = []JobRuntimeRecord{}
	var result []peloton.JobID

	q, args := getQueryAndArgs(jobRuntimeTable, map[string]interface{}{"job_state=": state}, []string{"row_key"})

	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.WithField("job_state", state).Warn("GetJobsByState returns no rows")
		return result, nil
	} else if err != nil {
		log.WithError(err).WithField("job_state", state).Error("Failed to GetJobsByState")
		m.metrics.JobGetByStateFail.Inc(1)
		return nil, err
	}

	for _, record := range records {
		result = append(result, peloton.JobID{
			Value: record.RowKey,
		})
	}
	m.metrics.JobGetByState.Inc(1)
	return result, nil
}

// UpdateJobRuntime updates the job runtime info
func (m *Store) UpdateJobRuntime(ctx context.Context, id *peloton.JobID, runtime *job.RuntimeInfo) error {
	buffer, err := json.Marshal(runtime)
	if err != nil {
		log.WithError(err).Error("Failed to marshal job runtime")
		m.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}

	_, err = m.DB.Exec(upsertJobRuntimeStmt, id.Value, string(buffer), string(buffer))
	if err != nil {
		log.WithError(err).
			WithField("job_id", id.Value).
			Error("Failed to update job runtime")
		m.metrics.JobUpdateRuntimeFail.Inc(1)
		return err
	}
	m.metrics.JobUpdateRuntime.Inc(1)
	return nil
}

// QueryTasks returns all tasks in the given [offset...offset+limit) range.
func (m *Store) QueryTasks(ctx context.Context, id *peloton.JobID, offset uint32, limit uint32) ([]*task.TaskInfo, uint32, error) {
	// First fetch total count.
	var total uint32
	if err := m.DB.QueryRow(getTasksCountForJobStmt, id.Value).Scan(&total); err != nil {
		return nil, 0, err
	}

	// Now fetch the requested range.
	q := getTasksForJobStmt + " LIMIT ?, ?"
	rows, err := m.DB.Queryx(q, id.Value, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var tasks []*task.TaskInfo
	for rows.Next() {
		var taskRecord TaskRecord
		if err := rows.StructScan(&taskRecord); err != nil {
			return nil, 0, err
		}

		taskInfo, err := taskRecord.GetTaskInfo()
		if err != nil {
			log.Errorf("taskRecord %v GetTaskInfo failed, err=%v", taskRecord, err)
			m.metrics.TaskGetFail.Inc(1)
			return nil, 0, err
		}

		tasks = append(tasks, taskInfo)
	}

	return tasks, total, rows.Err()
}

// CreatePersistentVolume creates a persistent volume entry.
func (m *Store) CreatePersistentVolume(ctx context.Context, volume *pb_volume.PersistentVolumeInfo) error {
	return errors.New("Not implemented")
}

// UpdatePersistentVolume update state for a persistent volume.
func (m *Store) UpdatePersistentVolume(ctx context.Context, volumeID string, state pb_volume.VolumeState) error {
	return errors.New("Not implemented")
}

// GetPersistentVolume gets the persistent volume object.
func (m *Store) GetPersistentVolume(ctx context.Context, volumeID string) (*pb_volume.PersistentVolumeInfo, error) {
	return nil, errors.New("Not implemented")
}

// DeletePersistentVolume delete persistent volume entry.
func (m *Store) DeletePersistentVolume(ctx context.Context, volumeID string) error {
	return errors.New("Not implemented")
}

// GetJobsByRespoolID returns jobIDs in a respool
func (m *Store) GetJobsByRespoolID(ctx context.Context, respoolID *respool.ResourcePoolID) (map[string]*job.JobConfig, error) {
	return nil, errors.New("Not implemented")
}
