package mysql

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/storage"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // Pull in MySQL driver for sqlx
	"github.com/jmoiron/sqlx"
	_ "github.com/mattes/migrate/driver/mysql" // Pull in MySQL driver for migrate
	"github.com/mattes/migrate/migrate"
	mesos_v1 "mesos/v1"
	"os"
	"peloton/job"
	"peloton/task"
	"strings"
)

const (
	// Table names
	jobsTable       = "jobs"
	tasksTable      = "tasks"
	frameworksTable = "frameworks"

	// values for the col_key
	colRuntimeInfo = "runtime_info"
	colBaseInfo    = "base_info"
	colJobConfig   = "job_config"

	// Various statement templates for job_config
	insertJobStmt = `INSERT INTO jobs (row_key, col_key, ref_key, body, created_by) values (?, ?, ?, ?, ?)`
	// TODO: discuss on supporting soft delete.
	deleteJobStmt         = `DELETE from jobs where row_key = ?`
	getJobStmt            = `SELECT * from jobs where col_key = '` + colJobConfig + `' and row_key = ?`
	queryJobsForLabelStmt = `SELECT * from jobs where col_key='` + colJobConfig + `' and match(labels_summary) against (? IN BOOLEAN MODE)`
	getJobsbyOwnerStmt    = `SELECT * from jobs where col_key='` + colJobConfig + `' and owning_team = ?`

	// Various statement templates for task runtime_info
	insertTaskStmt             = `INSERT INTO tasks (row_key, col_key, ref_key, body, created_by) values (?, ?, ?, ?, ?)`
	updateTaskStmt             = `UPDATE tasks SET body = ? where row_key = ?`
	getTasksForJobStmt         = `SELECT * from tasks where col_key='` + colRuntimeInfo + `' and job_id = ?`
	getTasksForJobAndStateStmt = `SELECT * from tasks where col_key='` + colRuntimeInfo + `' and job_id = ? and task_state = ?`
	getMesosFrameworkInfoStmt  = `SELECT * from frameworks where framework_name = ?`
	setMesosStreamIdStmt       = `INSERT INTO frameworks (framework_name, mesos_stream_id, update_host) values (?, ?, ?) ON DUPLICATE KEY UPDATE mesos_stream_id = ?`
	setMesosFrameworkIdStmt    = `INSERT INTO frameworks (framework_name, framework_id, update_host) values (?, ?, ?) ON DUPLICATE KEY UPDATE framework_id = ?`
)

// Container for database configs
type Config struct {
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Database   string `yaml:"database",validate:"nonzero"`
	Migrations string `yaml:"migrations"`
	ReadOnly   bool
	Conn       *sqlx.DB
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

// MysqlJobStore implements JobStore using a mysql backend
type MysqlJobStore struct {
	DB *sqlx.DB
}

// NewMysqlJobStore creates a MysqlJobStore
func NewMysqlJobStore(db *sqlx.DB) *MysqlJobStore {
	return &MysqlJobStore{DB: db}
}

// CreateJob creates a job with the job id and the config value
func (m *MysqlJobStore) CreateJob(id *job.JobID, jobConfig *job.JobConfig, created_by string) error {
	buffer, err := json.Marshal(jobConfig)
	if err != nil {
		log.Errorf("error = %v", err)
		return err
	}
	_, err = m.DB.Exec(insertJobStmt, id.Value, colJobConfig, 0, string(buffer), created_by)
	if err != nil {
		log.Errorf("CreateJob failed with id %v error = %v", id.Value, err)
		return err
	}
	return err
}

// GetJob returns a job config given the job id
func (m *MysqlJobStore) GetJob(id *job.JobID) (*job.JobConfig, error) {
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
	return nil, nil
}

// Query returns all jobs that contains the Labels.
//
// In the tasks table, the "Labels" field are compacted (all whitespaces and " are removed for each label),
// then stored as the "labels_summary" row. Mysql fulltext index are also set on this field.
// When a query comes, the query labels are compacted in the same way then queried against the fulltext index.
func (m *MysqlJobStore) Query(Labels *mesos_v1.Labels) (map[string]*job.JobConfig, error) {
	var queryLabels = ""
	records := []storage.JobRecord{}
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
func (m *MysqlJobStore) DeleteJob(id *job.JobID) error {
	// Check if there are any task left for the job. If there is any, abort the deletion
	// TODO: slu -- discussion on if the task state matter here
	tasks, err := m.GetTasksForJob(id)
	if err != nil {
		log.Errorf("GetTasksForJob for job id %v failed with error %v", id.Value, err)
		return err
	}
	if len(tasks) > 0 {
		err = fmt.Errorf("job id %v still have task runtime records, cannot delete %v", id.Value, tasks)
		return err
	}

	_, err = m.DB.Exec(deleteJobStmt, id.Value)
	if err != nil {
		log.Errorf("Delete job id %v failed with error %v", id.Value, err)
	}
	return err
}

// GetJobsByOwner returns jobs by owner
func (m *MysqlJobStore) GetJobsByOwner(owner string) (map[string]*job.JobConfig, error) {
	return m.getJobs(map[string]interface{}{"owning_team=": owner, "col_key=": colJobConfig})
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *MysqlJobStore) GetTasksForJob(id *job.JobID) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value})
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *MysqlJobStore) GetTasksForJobByRange(id *job.JobID, Range *task.InstanceRange) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "instance_id >=": Range.From, "instance_id <=": Range.To})
}

// GetTaskById returns the tasks (tasks.TaskInfo) for a peloton job
func (m *MysqlJobStore) GetTaskById(taskId string) (*task.TaskInfo, error) {
	result, err := m.getTasks(map[string]interface{}{"task_id=": taskId})
	if err != nil {
		return nil, err
	}

	if len(result) > 1 {
		log.Warnf("Found %v records for taskId %v", len(result), taskId)
	}
	// Return the first result
	for _, task := range result {
		return task, nil
	}
	// No record found
	log.Warnf("No task records found for taskId %v", taskId)
	return nil, nil
}

// GetTasksForJob returns the tasks (tasks.TaskInfo) for a peloton job
func (m *MysqlJobStore) GetTaskForJob(id *job.JobID, instanceId uint32) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "instance_id=": instanceId})
}

// CreateTask creates a task for a peloton job
func (m *MysqlJobStore) CreateTask(id *job.JobID, instanceId int, taskInfo *task.TaskInfo, created_by string) error {
	// TODO: discuss on whether taskId should be part of the taskInfo instead of runtime
	row_key := fmt.Sprintf("%s-%d", id.Value, instanceId)
	if taskInfo.InstanceId != uint32(instanceId) {
		errMsg := fmt.Sprintf("Task %v has instance id %v, different than the instanceId %d expected", row_key, instanceId, taskInfo.InstanceId)
		log.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("error = %v", err)
		return err
	}
	// TODO: adjust when taskInfo pb change to introduce static config and runtime config
	_, err = m.DB.Exec(insertTaskStmt, row_key, colBaseInfo, 0, string(buffer), created_by)
	return err
}

// GetTasksForJob returns the tasks (runtime_config) for a peloton job with certain state
func (m *MysqlJobStore) GetTasksForJobAndState(id *job.JobID, state string) (map[uint32]*task.TaskInfo, error) {
	return m.getTasks(map[string]interface{}{"job_id=": id.Value, "task_state=": state})
}

// UpdateTask updates a task for a peloton job
func (m *MysqlJobStore) UpdateTask(taskInfo *task.TaskInfo) error {
	row_key := fmt.Sprintf("%s-%d", taskInfo.JobId.Value, taskInfo.InstanceId)
	buffer, err := json.Marshal(taskInfo)
	if err != nil {
		log.Errorf("error = %v", err)
		return err
	}
	_, err = m.DB.Exec(updateTaskStmt, string(buffer), row_key)
	return err
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

func (m *MysqlJobStore) getJobs(filters map[string]interface{}) (map[string]*job.JobConfig, error) {
	var records = []storage.JobRecord{}
	var result = make(map[string]*job.JobConfig)
	q, args := getQueryAndArgs(jobsTable, filters, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getJobs for filters %v returns no rows", filters)
		return result, nil
	}
	if err != nil {
		log.Errorf("getJobs for filter %v failed with error %v", filters, err)
		return nil, err
	}
	for _, jobRecord := range records {
		jobConfig, err := jobRecord.GetJobConfig()
		if err != nil {
			log.Errorf("jobRecord %v GetJobConfig failed, err=%v", jobRecord, err)
			return nil, err
		}
		result[jobRecord.RowKey] = jobConfig
	}
	return result, nil
}

func (m *MysqlJobStore) getTasks(filters map[string]interface{}) (map[uint32]*task.TaskInfo, error) {
	var records = []storage.TaskRecord{}
	var result = make(map[uint32]*task.TaskInfo)
	q, args := getQueryAndArgs(tasksTable, filters, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("getTasks for filters %v returns no rows", filters)
		return result, nil
	}
	if err != nil {
		log.Errorf("getTasks for filter %v failed with error %v", filters, err)
		return nil, err
	}
	for _, taskRecord := range records {
		taskInfo, err := taskRecord.GetTaskInfo()
		if err != nil {
			log.Errorf("taskRecord %v GetTaskInfo failed, err=%v", taskRecord, err)
			return nil, err
		}
		result[uint32(taskRecord.InstanceId)] = taskInfo
	}
	return result, nil
}

//SetMesosStreamId stores the mesos stream id for a framework name
func (m *MysqlJobStore) SetMesosStreamId(frameworkName string, mesosStreamId string) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("os.HostName() failed with err=%v", err)
	}
	_, err = m.DB.Exec(setMesosStreamIdStmt, frameworkName, mesosStreamId, hostname, mesosStreamId)
	if err != nil {
		log.Errorf("SetMesosStreamId failed with framework named %v error = %v", frameworkName, err)
		return err
	}
	return nil
}

//SetMesosFrameworkId stores the mesos framework id for a framework name
func (m *MysqlJobStore) SetMesosFrameworkId(frameworkName string, frameworkId string) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Errorf("os.HostName() failed with err=%v", err)
	}
	_, err = m.DB.Exec(setMesosFrameworkIdStmt, frameworkName, frameworkId, hostname, frameworkId)
	if err != nil {
		log.Errorf("SetMesosFrameworkId failed with id %v error = %v", err)
		return err
	}
	return nil
}

//GetMesosStreamId reads the mesos stream id for a framework name
func (m *MysqlJobStore) GetMesosStreamId(frameworkName string) (string, error) {
	var records = []storage.MesosFrameworkInfo{}
	q, args := getQueryAndArgs(frameworksTable, map[string]interface{}{"framework_name=": frameworkName}, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetMesosStreamId for frmeworkName %v returns no rows", frameworkName)
		return "", nil
	}
	for _, frameworkInfo := range records {
		return frameworkInfo.MesosStreamId.String, nil
	}
	return "", nil
}

//GetFrameworkId reads the framework id for a framework name
func (m *MysqlJobStore) GetFrameworkId(frameworkName string) (string, error) {
	var records = []storage.MesosFrameworkInfo{}
	q, args := getQueryAndArgs(frameworksTable, map[string]interface{}{"framework_name=": frameworkName}, []string{"*"})
	err := m.DB.Select(&records, q, args...)
	if err == sql.ErrNoRows {
		log.Warnf("GetFrameworkId for frameworkName %v returns no rows", frameworkName)
		return "", nil
	}
	for _, frameworkInfo := range records {
		return frameworkInfo.FrameworkId.String, nil
	}
	return "", nil
}
