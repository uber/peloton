package main

import (
	"os"

	"code.uber.internal/infra/peloton/aurorabridge"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/buildversion"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/common/rpc"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

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

	httpPort = app.Flag(
		"http-port", "Aurora Bridge HTTP port (aurorabridge.http_port override) "+
			"(set $PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Aurora Bridge gRPC port (aurorabridge.grpc_port override) "+
			"(set $PORT to override)").
		Envar("GRPC_PORT").
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

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewAuroraBridgeInbounds(
		*httpPort,
		*grpcPort, // dummy grpc port for aurora bridge
		mux)

	// all leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")
	// setup the discovery service to detect jobmgr leaders and
	// configure the YARPC Peer dynamically
	t := rpc.NewTransport()
	jobmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.JobManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer jobmgrPeerChooser.Stop()

	jobmgrOutbound := t.NewOutbound(jobmgrPeerChooser)

	outbounds := yarpc.Outbounds{
		common.PelotonJobManager: transport.Outbounds{
			Unary: jobmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonAuroraBridge,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})

	server := aurorabridge.NewServer(
		*httpPort,
	)

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.PelotonAuroraBridgeRole,
		server,
	)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}

	aurorabridge.NewServiceHandler(
		rootScope,
		dispatcher)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	err = candidate.Start()
	if err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer candidate.Stop()

	log.WithFields(log.Fields{
		"httpPort": *httpPort,
	}).Info("Started Aurora Bridge")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	select {}
}
