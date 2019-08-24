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

package cassandra

import (
	"fmt"
	"strings"

	"github.com/uber/peloton/pkg/storage/cassandra/impl"

	_ "github.com/gemnasium/migrate/driver/cassandra" // Pull in C* driver for migrate
	"github.com/gemnasium/migrate/migrate"
	log "github.com/sirupsen/logrus"
)

const (
	createKeyspaceStmt = `CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s`
)

// Migrator manages the keyspace schema versions
type Migrator struct {
	Config *Config
}

// NewMigrator creates a DB migrator
func NewMigrator(config *Config) (*Migrator, error) {
	return &Migrator{
		Config: config,
	}, nil
}

// createKeyspace creates the keyspace if not exists
func (m *Migrator) createKeyspace() error {
	session, err := impl.CreateStoreSession(m.Config.CassandraConn, "")
	if err != nil {
		return err
	}
	defer session.Close()

	// Build the CQL statement to create a keyspace with the given
	// replication setting from Cassandra config
	replicationStr := fmt.Sprintf(`{ 'class' : '%s'`, m.Config.Replication.Strategy)
	for _, replica := range m.Config.Replication.Replicas {
		replicationStr = replicationStr + fmt.Sprintf(
			`, '%s': '%d'`, replica.Name, replica.Value)
	}
	replicationStr = replicationStr + "}"
	stmt := fmt.Sprintf(createKeyspaceStmt, m.Config.StoreName, replicationStr)

	// Create the keyspace in Cassandra if not exsits
	if err = session.Query(stmt).Exec(); err != nil {
		return err
	}
	return nil
}

// UpSync applies the schema migrations to new version
func (m *Migrator) UpSync() []error {
	if err := m.createKeyspace(); err != nil {
		return []error{err}
	}

	connStr := m.connString()
	errs, ok := migrate.UpSync(connStr, m.Config.Migrations)
	if !ok {
		return errs
	}
	log.Infof("UpSync succeeded")
	return nil
}

// downSync rolls back the schema migration to previous version.
// This is only used in the unit test since it destroys the whole keyspace.
func (m *Migrator) downSync() []error {
	connStr := m.connString()
	errs, ok := migrate.DownSync(connStr, m.Config.Migrations)
	if !ok {
		return errs
	}
	log.Infof("DownSync succeeded")
	return nil
}

// Version returns the current version of migration. Returns 0 in
// error case.
func (m *Migrator) Version() (uint64, error) {
	connStr := m.connString()
	version, err := migrate.Version(connStr, m.Config.Migrations)
	if err != nil {
		return 0, err
	}
	return uint64(version), nil
}

// connString returns the DB string required for schema migration
// Assumes that the keyspace (indicated by StoreName) is already created
func (m *Migrator) connString() string {
	// see https://github.com/gemnasium/migrate/pull/17 on why disable_init_host_lookup is needed
	// This is for making local testing faster with docker running on mac
	conn := m.Config.CassandraConn
	connStr := fmt.Sprintf(
		"cassandra://%v:%v/%v?protocol=4&disable_init_host_lookup",
		conn.ContactPoints[0],
		conn.Port,
		m.Config.StoreName)

	if len(conn.Username) != 0 {
		connStr = fmt.Sprintf("cassandra://%v:%v@%v:%v/%v",
			conn.Username,
			conn.Password,
			conn.ContactPoints[0],
			conn.Port,
			m.Config.StoreName)
	}
	connStr = strings.Replace(connStr, " ", "", -1)
	log.Debugf("Cassandra migration string %v", connStr)
	return connStr
}
