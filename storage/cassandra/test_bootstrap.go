package cassandra

import (
	"fmt"
	"os"
	"path"
	"strings"

	"code.uber.internal/infra/peloton/storage/cassandra/impl"
	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	log "github.com/sirupsen/logrus"
)

func downSync(cfg *Config) []error {
	connString := cfg.MigrateString()
	errors, ok := migrate.DownSync(connString, cfg.Migrations)
	if !ok {
		return errors
	}
	return nil
}

// MigrateForTest instantiates a config with db table freshly migrated
// see https://github.com/mattes/migrate/issues/86, currently Migration
// can take a long time (upsync/downsync take 3 min) with local docker,
// To verify with jenkins
func MigrateForTest() *Config {
	conf := Config{
		CassandraConn: &impl.CassandraConn{
			ContactPoints: []string{"127.0.0.1"},
			Port:          9043,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
		},
		StoreName:          "peloton_test",
		Migrations:         "migrations",
		MaxBatchSize:       20,
		MaxParallelBatches: 10,
		MaxUpdatesPerJob:   10,
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get PWD, err=%v", err)
	}

	for !strings.HasSuffix(path.Clean(dir), "/peloton") && len(dir) > 1 {
		dir = path.Join(dir, "..")
	}

	conf.Migrations = path.Join(dir, "storage", "cassandra", conf.Migrations)
	log.Infof("pwd=%v migration path=%v", dir, conf.Migrations)
	if errs := downSync(&conf); errs != nil {
		log.Warnf(fmt.Sprintf("downSync is having the following error: %+v", errs))
	}
	log.Infof("downSync complete")
	// bring the schema up to date
	if errs := conf.AutoMigrate(); errs != nil {
		panic(fmt.Sprintf("%+v", errs))
	}
	return &conf
}
