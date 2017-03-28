package config

import (
	"code.uber.internal/infra/peloton/storage/cassandra"
	"code.uber.internal/infra/peloton/storage/mysql"
)

// Config contains the different DB config values for each
// supported backend
// TODO: Fix the cycle imports between storage and mysql/stapi so we
// can move storage/config/config.go to storage/config.go
type Config struct {
	MySQL    mysql.Config     `yaml:"mysql"`
	STAPI    cassandra.Config `yaml:"stapi_store_config"`
	UseSTAPI bool             `yaml:"use_stapi"`
}
