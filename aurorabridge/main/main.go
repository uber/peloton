package main

import (
	"fmt"
	"os"

	"code.uber.internal/infra/peloton/aurorabridge"
	"code.uber.internal/infra/peloton/aurorabridge/aurora/api"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/buildversion"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/leader"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	"github.com/apache/thrift/lib/go/thrift"
	log "github.com/sirupsen/logrus"
)

var (
	version string
	app     = kingpin.New("peloton-aurorabridge", "Peloton Aurora bridge")

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

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	port = app.Flag(
		"port", "aurora api port").
		Default("8082").
		Int()
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

	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.Fatalf("Error parsing yaml config: %s", err)
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	log.WithField("config", cfg).Info("Loaded AuroraBridge configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonAuroraBridge,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel))

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	server := aurorabridge.NewServer(
		*port,
	)

	// enable leader election for peloton aurora bridge
	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.PelotonAuroraBridgeRole,
		server)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}

	err = candidate.Start()
	if err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer candidate.Stop()

	health.InitHeartbeat(rootScope, cfg.Health, nil)

	// start thirft server for supporting aurora apis
	addr := fmt.Sprintf(":%d", *port)

	handler := aurorabridge.NewServiceHandler(rootScope)
	processor := api.NewAuroraSchedulerManagerProcessor(handler)
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		log.Fatalf("Error creating thrift socket: %s", err)
	}

	thriftserver := thrift.NewTSimpleServer6(
		processor,
		transport,
		thrift.NewTTransportFactory(),                 // Input transport.
		thrift.NewTTransportFactory(),                 // Output transport.
		thrift.NewTJSONProtocolFactory(),              // Input protocol.
		thrift.NewTBinaryProtocolFactory(false, true), // Output protocol.
	)

	log.Infof("Starting Aurora server on %s...", addr)
	log.Fatal(thriftserver.Serve())
}
