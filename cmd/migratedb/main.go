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

package main

import (
	"os"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/storage/cassandra"

	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("migratedb", "Tool to manage DB schema for Peloton")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	configFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	cassandraHosts = app.Flag(
		"cassandra-hosts", "Cassandra hosts").
		Envar("CASSANDRA_HOSTS").
		Strings()

	cassandraStore = app.Flag(
		"cassandra-store", "Cassandra store name").
		Default("").
		Envar("CASSANDRA_STORE").
		String()

	cassandraPort = app.Flag(
		"cassandra-port", "Cassandra port to connect").
		Default("0").
		Envar("CASSANDRA_PORT").
		Int()

	// Top level migrate DB commands
	upCmd      = app.Command("up", "Apply all DB migrations. Will create keyspace if not exists")
	versionCmd = app.Command("version", "Get the current schema version.")
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(
		&logging.LogFieldFormatter{
			Formatter: &logging.SecretsFormatter{Formatter: &log.JSONFormatter{}},
			Fields: log.Fields{
				common.AppLogField: app.Name,
			},
		},
	)

	// Use stdout here, since the output of migratedb command might get
	// parsed, and we don't want it to be mangled with output of stderr.
	log.SetOutput(os.Stdout)

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)
	log.WithField("files", *configFiles).Debug("Loading migratedb config")

	var cfg Config
	if err := config.Parse(&cfg, *configFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *cassandraHosts != nil && len(*cassandraHosts) > 0 {
		cfg.Storage.Cassandra.CassandraConn.ContactPoints = *cassandraHosts
	}

	if *cassandraStore != "" {
		cfg.Storage.Cassandra.StoreName = *cassandraStore
	}

	if *cassandraPort != 0 {
		cfg.Storage.Cassandra.CassandraConn.Port = *cassandraPort
	}

	log.WithField("config", cfg).Debug("Loaded migratedb config")

	migrator, err := cassandra.NewMigrator(&cfg.Storage.Cassandra)
	if err != nil {
		log.Fatalf("Could not create DB migrator: %v", err)
	}

	switch cmd {
	case upCmd.FullCommand():
		if errs := migrator.UpSync(); errs != nil {
			log.Fatalf("Could not migrate database: %+v", errs)
		}
	case versionCmd.FullCommand():
		version, err := migrator.Version()
		if err != nil {
			log.Fatalf("Could not get schema version: %v", err)
		}
		log.WithField("version", version).Info("Database schema version")
	}

}
