package config

import (
	"code.uber.internal/infra/peloton/storage/cassandra"
	"code.uber.internal/infra/peloton/storage/mysql"
)

// Config contains the different DB config values for each
// supported backend
type Config struct {
	MySQL              mysql.Config     `yaml:"mysql"`
	Cassandra          cassandra.Config `yaml:"cassandra"`
	UseCassandra       bool             `yaml:"use_cassandra"`
	AutoMigrate        bool             `yaml:"auto_migrate"`
	DbWriteConcurrency int              `yaml:"db_write_concurrency"`
}
