package mysql

import (
	"code.uber.internal/go-common.git/x/log"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/mattes/migrate/migrate"
	"github.com/stretchr/testify/suite"
	"mesos/v1"
	"peloton/job"
	"strconv"
	"testing"
)

func downSync(cfg *Config) []error {
	connString := cfg.MigrateString()
	errors, ok := migrate.DownSync(connString, cfg.Migrations)
	if !ok {
		return errors
	}
	return nil
}

// LoadConfig loads test configuration
// TODO: figure out how to install mysql 5.7.x before testing
func prepareTest() *Config {
	conf := &Config{
		User:       "peloton",
		Password:   "peloton",
		Host:       "127.0.0.1",
		Port:       3306,
		Database:   "peloton",
		Migrations: "migrations",
	}
	err := conf.Connect()
	if err != nil {
		panic(err)
	}
	if errs := downSync(conf); errs != nil {
		log.Warnf(fmt.Sprintf("downSync is having the following error: %+v", errs))
	}

	// bring the schema up to date
	if errs := conf.AutoMigrate(); errs != nil {
		panic(fmt.Sprintf("%+v", errs))
	}
	fmt.Println("setting up again")
	return conf
}

type MysqlStoreTestSuite struct {
	suite.Suite
	store *MysqlJobStore
	db    *sqlx.DB
}

func (suite *MysqlStoreTestSuite) SetupTest() {
	conf := prepareTest()

	suite.db = conf.Conn
	suite.store = NewMysqlJobStore(conf.Conn)
}

func (suite *MysqlStoreTestSuite) TearDownTest() {
	fmt.Println("tearing down")
	//_, err := suite.db.Exec("DELETE FROM task")
	//suite.NoError(err)
}

func TestMysqlStore(t *testing.T) {
	suite.Run(t, new(MysqlStoreTestSuite))
}

/*
func (suite *MysqlStoreTestSuite) TestCreateGetRuntimeInfo() {

}
*/

func (suite *MysqlStoreTestSuite) TestCreateGetJobConfig() {
	// Create 10 jobs in db
	var originalJobs []*job.JobConfig
	var records = 10
	var keys = []string{"testKey0", "testKey1", "testKey2", "key0"}
	var vals = []string{"testVal0", "testVal1", "testVal2", "val0"}
	for i := 0; i < records; i++ {

		var jobId = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		var sla = job.SlaConfig{
			Priority:               22,
			MinimumInstanceCount:   3 + uint32(i),
			MinimumInstancePercent: 50,
			Preemptible:            false,
		}
		var resourceConfig = job.ResourceConfig{
			CpusLimit:   0.8,
			MemLimitMb:  800,
			DiskLimitMb: 1500,
			FdLimit:     1000 + uint32(i),
		}
		var labels = mesos_v1.Labels{
			Labels: []*mesos_v1.Label{
				&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
				&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
				&mesos_v1.Label{Key: &keys[2], Value: &vals[2]},
			},
		}
		// Add some special label to job0 and job1
		if i < 2 {
			labels.Labels = append(labels.Labels, &mesos_v1.Label{Key: &keys[3], Value: &vals[3]})
		}

		// Add owner to job0 and job1
		var owner = "team6"
		if i < 2 {
			owner = "money"
		}
		var jobconfig = job.JobConfig{
			Name:       "TestJob_" + strconv.Itoa(i),
			OwningTeam: owner,
			LdapGroups: []string{"money", "team6", "otto"},
			Sla:        &sla,
			Resource:   &resourceConfig,
			Labels:     &labels,
		}
		originalJobs = append(originalJobs, &jobconfig)
		err := suite.store.CreateJob(&jobId, &jobconfig, "uber")
		suite.NoError(err)
	}
	// search by job id
	for i := 0; i < records; i++ {
		var jobId = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
		result, err := suite.store.GetJob(&jobId)
		suite.NoError(err)
		suite.Equal(result.Name, originalJobs[i].Name)
		suite.Equal(result.Resource.FdLimit, originalJobs[i].Resource.FdLimit)
		suite.Equal(result.Sla.MinimumInstanceCount, originalJobs[i].Sla.MinimumInstanceCount)
	}

	// Query by owner
	var jobs []*job.JobConfig
	var err error
	var labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
			&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), len(originalJobs))

	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[0], Value: &vals[0]},
			&mesos_v1.Label{Key: &keys[1], Value: &vals[1]},
			&mesos_v1.Label{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)
	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[3], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 2)

	labelQuery = mesos_v1.Labels{
		Labels: []*mesos_v1.Label{
			&mesos_v1.Label{Key: &keys[2], Value: &vals[3]},
		},
	}
	jobs, err = suite.store.Query(&labelQuery)
	suite.NoError(err)
	suite.Equal(len(jobs), 0)

	jobs, err = suite.store.GetJobsByOwner("team6")
	suite.NoError(err)
	suite.Equal(len(jobs), records-2)

	jobs, err = suite.store.GetJobsByOwner("money")
	suite.NoError(err)
	suite.Equal(len(jobs), 2)

	jobs, err = suite.store.GetJobsByOwner("owner")
	suite.NoError(err)
	suite.Equal(len(jobs), 0)
	/*
		// Delete job
		for i := 0; i < records; i++ {
			var jobId = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
			err := suite.store.DeleteJob(&jobId)
			suite.NoError(err)
		}
		// Get should not return anything
		for i := 0; i < records; i++ {
			var jobId = job.JobID{Value: "TestJob_" + strconv.Itoa(i)}
			result, err := suite.store.GetJob(&jobId)
			suite.NoError(err)
			suite.Nil(result)
		}*/
}
