package config

import (
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/storage/stapi"
)

// Config contains the different DB config values for each
// supported backend
// TODO: Fix the cycle imports between storage and mysql/stapi so we
// can move storage/config/config.go to storage/config.go
type Config struct {
	MySQL    mysql.Config `yaml:"mysql"`
	STAPI    stapi.Config `yaml:"stapi_store_config"`
	UseSTAPI bool         `yaml:"use_stapi"`
}
