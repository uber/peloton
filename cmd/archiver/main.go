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
	"time"

	archiverConfig "github.com/uber/peloton/pkg/archiver/config"
	"github.com/uber/peloton/pkg/archiver/engine"
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New(archiverConfig.PelotonArchiver, "Peloton Archiver")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	enableSentry = app.Flag(
		"enable-sentry", "enable logging hook up to sentry").
		Default("false").
		Envar("ENABLE_SENTRY_LOGGING").
		Bool()

	cfgFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	zkServers = app.Flag(
		"zk-server",
		"Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	httpPort = app.Flag(
		"http-port", "Archiver HTTP port (archiver.http_port override) "+
			"(set $PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Archiver gRPC port (archiver.grpc_port override) "+
			"(set $PORT to override)").
		Envar("GRPC_PORT").
		Int()

	enableArchiver = app.Flag(
		"enable-archiver", "enable Archiver").
		Default("false").
		Envar("ENABLE_ARCHIVER").
		Bool()

	podEventsCleanup = app.Flag(
		"pod-events-cleanup", "enable Pod Events Cleanup").
		Default("false").
		Envar("POD_EVENTS_CLEANUP").
		Bool()

	streamOnlyMode = app.Flag(
		"stream-only-mode", "Archiver streams jobs without deleting them").
		Default("false").
		Envar("STREAM_ONLY_MODE").
		Bool()

	archiveInterval = app.Flag(
		"archive-interval",
		"Archive interval duration in h/m/s (archiver.archive_interval override) (set $ARCHIVE_INTERVAL to override)").
		Envar("ARCHIVE_INTERVAL").
		String()

	archiveAge = app.Flag(
		"archive-age",
		"Archive age duration in h/m/s (archiver.archive_age override) (set $ARCHIVE_AGE to override)").
		Envar("ARCHIVE_AGE").
		String()

	archiveStepSize = app.Flag(
		"archive-step-size",
		"Archive step size in h/m/s (archiver.archive_step_size override) (set $ARCHIVE_STEP_SIZE to override)").
		Envar("ARCHIVE_STEP_SIZE").
		String()

	kafkaTopic = app.Flag(
		"kafka-topic",
		"kafka topic used by archiver to stream completed jobs").
		Envar("KAFKA_TOPIC").
		String()

	authType = app.Flag(
		"auth-type",
		"Define the auth type used, default to NOOP").
		Default("NOOP").
		Envar("AUTH_TYPE").
		Enum("NOOP", "BASIC")

	authConfigFile = app.Flag(
		"auth-config-file",
		"config file for the auth feature, which is specific to the auth type used").
		Default("").
		Envar("AUTH_CONFIG_FILE").
		String()
)

func main() {
	var cfg archiverConfig.Config
	var err error

	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(
		&logging.LogFieldFormatter{
			Formatter: &log.JSONFormatter{},
			Fields: log.Fields{
				common.AppLogField: app.Name,
			},
		},
	)

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}

	log.SetLevel(initialLevel)

	log.WithField("files", *cfgFiles).
		Info("Loading archiver config")

	if err = config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithError(err).
			Fatal("Cannot parse yaml config")
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	if *httpPort != 0 {
		cfg.Archiver.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.Archiver.GRPCPort = *grpcPort
	}

	if *enableArchiver {
		cfg.Archiver.Enable = *enableArchiver
	}

	if *streamOnlyMode {
		cfg.Archiver.StreamOnlyMode = *streamOnlyMode
	}

	if *podEventsCleanup {
		cfg.Archiver.PodEventsCleanup = *podEventsCleanup
	}

	if *archiveInterval != "" {
		cfg.Archiver.ArchiveInterval, err = time.ParseDuration(*archiveInterval)
		if err != nil {
			log.WithError(err).
				WithField("ARCHIVE_INTERVAL", *archiveInterval).
				Fatal("Cannot parse Archive Interval")
		}
	}

	if *archiveAge != "" {
		cfg.Archiver.ArchiveAge, err = time.ParseDuration(*archiveAge)
		if err != nil {
			log.WithError(err).
				WithField("ARCHIVE_AGE", *archiveAge).
				Fatal("Cannot parse Archive Age")
		}
	}

	if *archiveStepSize != "" {
		cfg.Archiver.ArchiveStepSize, err = time.ParseDuration(*archiveStepSize)
		if err != nil {
			log.WithError(err).
				WithField("ARCHIVE_STEP_SIZE", *archiveStepSize).
				Fatal("Cannot parse Archive Step Size")
		}
	}

	if *kafkaTopic != "" {
		cfg.Archiver.KafkaTopic = *kafkaTopic
	}

	// zkservers list is needed to create peloton client.
	// Archiver does not depend on leader election
	if len(*zkServers) > 0 {
		cfg.Election.ZKServers = *zkServers
	}

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	log.WithField("config", cfg).
		Info("Loaded Archiver configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		archiverConfig.PelotonArchiver,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel),
	)

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	inbounds := rpc.NewInbounds(
		cfg.Archiver.HTTPPort,
		cfg.Archiver.GRPCPort,
		mux,
	)

	discovery, err := leader.NewZkServiceDiscovery(
		cfg.Election.ZKServers, cfg.Election.Root)
	if err != nil {
		log.WithError(err).
			Fatal("Could not create zk service discovery")
	}

	archiverEngine, err := engine.New(
		cfg,
		rootScope,
		mux,
		discovery,
		inbounds)
	if err != nil {
		log.WithError(err).
			WithField("zkservers", cfg.Election.ZKServers).
			WithField("zkroot", cfg.Election.Root).
			Fatal("Could not create archiver engine")
	}

	health.InitHeartbeat(rootScope, cfg.Health, nil)
	log.Info("Started archiver")

	if err := archiverEngine.Start(); err != nil {
		archiverEngine.Cleanup()
		log.WithError(err).Fatal("Archiver engine got a fatal error." +
			" Restarting.")
	}

	select {}
}
