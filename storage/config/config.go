package config

import (
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/storage/stapi"
)

// StorageConfig contains the different DB config values for each supported backend
type StorageConfig struct {
	MySQL mysql.Config `yaml:"mysql"`
	STAPI stapi.Config `yaml:"stapi"`
}
