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
	"peloton/job"
	"peloton/task"
	"strings"
)

const (
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
	getTasksForJobStmt         = `SELECT * from tasks where col_key='` + colRuntimeInfo + `' and job_id = ?`
	getTasksForJobAndStateStmt = `SELECT * from tasks where col_key='` + colRuntimeInfo + `' and job_id = ? and task_state = ?`
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
		log.Infof("%v is read-only.", d.Database)
	} else {
		log.Infof("%v is read-write.", d.Database)
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
	log.Infof("connecting to %s:", dbString)
	db, err := sqlx.Open("mysql", dbString)
	if err != nil {
		return err
	}
	d.Conn = db
	log.Infof("connected to %s:", dbString)
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
	return err
}

// GetJob returns a job config given the job id
func (m *MysqlJobStore) GetJob(id *job.JobID) (*job.JobConfig, error) {
	var taskRec storage.JobRecord
	err := m.DB.Get(&taskRec, getJobStmt, id.Value)
	if err == sql.ErrNoRows {
		log.Warnf("GetJob for id %v returns no rows", id)
		return nil, nil
	}
	if err != nil {
		log.Errorf("Get has error %v", err)
		return nil, err
	}
	return taskRec.GetJobConfig()
}

// Query returns all jobs that contains the Labels.
//
// In the tasks table, the "Labels" field are compacted (all whitespaces and " are removed for each label),
// then stored as the "labels_summary" row. Mysql fulltext index are also set on this field.
// When a query comes, the query labels are compacted in the same way then queried against the fulltext index.
func (m *MysqlJobStore) Query(Labels *mesos_v1.Labels) ([]*job.JobConfig, error) {
	var queryLabels = ""
	records := []storage.JobRecord{}
	var result []*job.JobConfig
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
	log.Infof("Querying using labels %v, text (%v)", Labels, queryLabels)
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
		result = append(result, jobConfig)
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
func (m *MysqlJobStore) GetJobsByOwner(owner string) ([]*job.JobConfig, error) {
	records := []storage.JobRecord{}
	var result []*job.JobConfig
	err := m.DB.Select(&records, getJobsbyOwnerStmt, owner)
	if err == sql.ErrNoRows {
		log.Warnf("GetJobsByOwner for owner %v returns no rows", owner)
		return result, nil
	}
	if err != nil {
		log.Errorf("GetJobsByOwner for owner %v failed with error %v", owner, err)
		return nil, err
	}
	// TODO: consider select 'body' directly
	for _, record := range records {
		jobConfig, err := record.GetJobConfig()
		if err != nil {
			log.Warnf("record.GetJobConfig for owner %v, failed with error %v", owner, err)
			continue
		}
		result = append(result, jobConfig)
	}
	return result, nil
}

// GetTasksForJob returns the tasks (runtime_config) for a peloton job
func (m *MysqlJobStore) GetTasksForJob(id *job.JobID) ([]*task.RuntimeInfo, error) {
	records := []storage.TaskRecord{}
	var result []*task.RuntimeInfo
	err := m.DB.Select(&records, getTasksForJobStmt, id.Value)
	if err == sql.ErrNoRows {
		log.Warnf("GetTasksForJob for id %v returns no rows", id)
		return result, nil
	}
	if err != nil {
		log.Errorf("GetTasksForJob for id %v failed with error %v", id, err)
		return nil, err
	}
	// TODO: consider select 'body' directly
	for _, record := range records {
		runtimeInfo, err := record.GetRuntimeInfo()
		if err != nil {
			log.Errorf("record.GetRuntimeConfig for id %v, failed with error %v", id, err)
			continue
		}
		result = append(result, runtimeInfo)
	}
	return result, nil
}
