package mysql

import (
	"fmt"
	"github.com/gemnasium/migrate/migrate"
	"os"
	"path"
	"strings"

	log "github.com/Sirupsen/logrus"
)

func downSync(cfg *Config) []error {
	connString := cfg.MigrateString()
	errors, ok := migrate.DownSync(connString, cfg.Migrations)
	if !ok {
		return errors
	}
	return nil
}

// LoadConfigWithDB instantiates a config with a DB connection
func LoadConfigWithDB() *Config {
	conf := &Config{
		User:         "peloton",
		Password:     "peloton",
		Host:         "127.0.0.1",
		Port:         8194,
		Database:     "peloton",
		Migrations:   "migrations",
		MaxBatchSize: 20,
	}
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get PWD, err=%v", err)
	}

	for !strings.HasSuffix(path.Clean(dir), "/peloton") && len(dir) > 1 {
		dir = path.Join(dir, "..")
	}

	conf.Migrations = path.Join(dir, "storage", "mysql", conf.Migrations)
	fmt.Println("dir=", dir, conf.Migrations)
	err = conf.Connect()
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
