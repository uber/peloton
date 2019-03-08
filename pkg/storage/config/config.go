// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"github.com/uber/peloton/pkg/storage/cassandra"
)

// Config contains the different DB config values for each
// supported backend
type Config struct {
	Cassandra          cassandra.Config `yaml:"cassandra"`
	UseCassandra       bool             `yaml:"use_cassandra"`
	AutoMigrate        bool             `yaml:"auto_migrate"`
	DbWriteConcurrency int              `yaml:"db_write_concurrency"`
}
