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

package stores

import (
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/storage"
	"github.com/uber/peloton/pkg/storage/cassandra"
	storage_config "github.com/uber/peloton/pkg/storage/config"
)

// MustCreateStore creates a generic store that is needed by peloton
// and exits if store can't be created
func MustCreateStore(
	cfg *storage_config.Config, rootScope tally.Scope) storage.Store {
	log.WithFields(log.Fields{
		"cassandra_connection": cfg.Cassandra.CassandraConn,
		"cassandra_config":     cfg.Cassandra,
	}).Info("Cassandra Config")
	if cfg.AutoMigrate {
		if errs := cfg.Cassandra.AutoMigrate(); errs != nil {
			log.Fatalf("Could not migrate database: %+v", errs)
		}
	}
	store, err := cassandra.NewStore(&cfg.Cassandra, rootScope)
	if err != nil {
		log.Fatalf("Could not create cassandra store: %+v", err)
	}
	return store
}
