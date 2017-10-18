package mysql

import (
	"fmt"
	"time"

	_ "github.com/gemnasium/migrate/driver/mysql" // Pull in MySQL driver for migrate
	"github.com/gemnasium/migrate/migrate"
	_ "github.com/go-sql-driver/mysql" // Pull in MySQL driver for sqlx
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
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
