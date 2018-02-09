package main

import (
	"os"

	"code.uber.internal/infra/peloton/archiver/engine"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/common/rpc"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	// PelotonArchiver application name
	PelotonArchiver = "peloton-archiver"
)

var (
	version string
	app     = kingpin.New(PelotonArchiver, "Peloton Archiver")

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
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)

	log.WithField("files", *cfgFiles).
		Info("Loading archiver config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).
			Fatal("Cannot parse yaml config")
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	// zkservers list is needed to create peloton client.
	// Archiver does not depend on leader election
	if len(*zkServers) > 0 {
		cfg.Election.ZKServers = *zkServers
	}

	log.WithField("config", cfg).
		Info("Loaded Archiver configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		PelotonArchiver,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel),
	)

	inbounds := rpc.NewInbounds(
		cfg.Archiver.HTTPPort,
		cfg.Archiver.GRPCPort,
		mux,
	)
	// dispatcher is started just to enable health checks
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     PelotonArchiver,
		Inbounds: inbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})
	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.WithError(err).
			Fatal("Could not start dispatcher")
	}

	archiverEngine, err := engine.New(cfg.Election, cfg.Archiver, rootScope, *debug)
	if err != nil {
		log.WithError(err).
			WithField("zkservers", cfg.Election.ZKServers).
			WithField("zkroot", cfg.Election.Root).
			Fatal("Could not create archiver engine")
	}
	archiverEngine.Start()
	defer archiverEngine.Stop()

	log.Info("Started archiver")

	health.InitHeartbeat(rootScope, cfg.Health, nil)

	select {}
}
