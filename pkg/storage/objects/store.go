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

package objects

import (
	"fmt"
	"os"
	"path"
	"strings"

	pelotonstore "github.com/uber/peloton/pkg/storage"
	"github.com/uber/peloton/pkg/storage/connectors/cassandra"
	"github.com/uber/peloton/pkg/storage/objects/base"
	"github.com/uber/peloton/pkg/storage/orm"

	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Objs is a global list of storage objects. Every storage object will be added
// using an init method to this list. This list will be used when creating the
// ORM client.
var Objs []base.Object

// Store contains ORM client as well as metrics
type Store struct {
	oClient orm.Client
	metrics *pelotonstore.Metrics
}

// NewCassandraStore creates a new Cassandra storage client
func NewCassandraStore(
	config *cassandra.Config,
	scope tally.Scope,
) (*Store, error) {
	connector, err := cassandra.NewCassandraConnector(config, scope)
	if err != nil {
		return nil, err
	}
	// TODO: Load up all objects automatically instead of explicitly adding
	// them here. Might need to add some Go init() magic to do this.
	oclient, err := orm.NewClient(connector, Objs...)
	if err != nil {
		return nil, err
	}
	return &Store{
		oClient: oclient,
		metrics: pelotonstore.NewMetrics(scope),
	}, nil
}

// GenerateTestCassandraConfig generates a test config for local C* client
// This is meant for sharing testing code only, not for production
func GenerateTestCassandraConfig() *cassandra.Config {
	return &cassandra.Config{
		CassandraConn: &cassandra.CassandraConn{
			ContactPoints: []string{"127.0.0.1"},
			Port:          9043,
			CQLVersion:    "3.4.2",
			MaxGoRoutines: 1000,
		},
		StoreName:  "peloton_test",
		Migrations: "migrations",
	}
}

// MigrateSchema will migrate DB schema on the peloton_test keyspace.
func MigrateSchema(conf *cassandra.Config) {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("failed to get PWD, err=%v", err)
	}

	for !strings.HasSuffix(path.Clean(dir), "/peloton") && len(dir) > 1 {
		dir = path.Join(dir, "..")
	}

	conf.Migrations = path.Join(
		dir, "pkg", "storage", "cassandra", conf.Migrations)
	log.Infof("pwd=%v migration path=%v", dir, conf.Migrations)

	connStr := fmt.Sprintf(
		"cassandra://%v:%v/%v?protocol=4&disable_init_host_lookup",
		conf.CassandraConn.ContactPoints[0],
		conf.CassandraConn.Port,
		conf.StoreName)
	connStr = strings.Replace(connStr, " ", "", -1)

	errs, ok := migrate.DownSync(connStr, conf.Migrations)
	if !ok {
		log.Fatalf("DownSync failed with errors: %v", errs)
	}
	log.Infof("DownSync complete")

	errs, ok = migrate.UpSync(connStr, conf.Migrations)
	if !ok {
		log.Fatalf("UpSync failed with errors: %v", errs)
	}

	log.Infof("UpSync complete")
}
