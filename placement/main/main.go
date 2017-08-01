package main

import (
	"net/url"
	"os"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/common/rpc"
	"code.uber.internal/infra/peloton/placement"
	"code.uber.internal/infra/peloton/storage/stores"
	"code.uber.internal/infra/peloton/yarpc/peer"
)

var (
	version string
	app     = kingpin.New("peloton-placement", "Peloton Placement Engine")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	cfgFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	zkPath = app.Flag(
		"zk-path",
		"Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").
		Envar("MESOS_ZK_PATH").
		String()

	useCassandra = app.Flag(
		"use-cassandra", "Use cassandra storage implementation").
		Default("true").
		Envar("USE_CASSANDRA").
		Bool()

	cassandraHosts = app.Flag(
		"cassandra-hosts", "Cassandra hosts").
		Envar("CASSANDRA_HOSTS").
		Strings()

	cassandraStore = app.Flag(
		"cassandra-store", "Cassandra store name").
		Default("").
		Envar("CASSANDRA_STORE").
		String()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	httpPort = app.Flag(
		"http-port",
		"Placement engine HTTP port (placement.http_port override) "+
			"(set $HTTP_PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port",
		"Placement engine GRPC port (placement.grpc_port override) "+
			"(set $GRPC_PORT to override)").
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

	log.WithField("files", *cfgFiles).Info("Loading Placement Engnine config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	logging.ConfigureSentry(&cfg.SentryConfig)

	// now, override any CLI flags in the loaded config.Config
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}
	log.WithField("config", cfg).Info("Loaded Placement Engine config")

	if !*useCassandra {
		cfg.Storage.UseCassandra = false
	}

	if *cassandraHosts != nil && len(*cassandraHosts) > 0 {
		cfg.Storage.Cassandra.CassandraConn.ContactPoints = *cassandraHosts
	}

	if *cassandraStore != "" {
		cfg.Storage.Cassandra.StoreName = *cassandraStore
	}

	if *httpPort != 0 {
		cfg.Placement.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.Placement.GRPCPort = *grpcPort
	}

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	log.Info("Connecting to HostManager")
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.HostManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()

	hostmgrOutbound := http.NewOutbound(
		hostmgrPeerChooser,
		http.URLTemplate(
			(&url.URL{
				Scheme: "http",
				Path:   common.PelotonEndpointPath,
			}).String()),
	)

	log.Info("Connecting to ResourceManager")
	resmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.ResourceManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer resmgrPeerChooser.Stop()

	resmgrOutbound := http.NewOutbound(
		resmgrPeerChooser,
		http.URLTemplate(
			(&url.URL{
				Scheme: "http",
				Path:   common.PelotonEndpointPath,
			}).String()),
	)

	log.Info("Setup the PlacementEngine server")
	// Now attempt to setup the dispatcher
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewInbounds(
		cfg.Placement.HTTPPort,
		cfg.Placement.GRPCPort,
		mux,
	)

	log.Debug("Creating new YARPC dispatcher")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonPlacement,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})

	log.Debug("Starting YARPC dispatcher")
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start dispatcher: %v", err)
	}

	log.Info("Connect to the TaskStore")
	_, taskStore, _, _, _, _ := stores.CreateStores(&cfg.Storage, rootScope)

	// Initialize and start placement engine
	placementEngine := placement.New(
		dispatcher,
		rootScope,
		&cfg.Placement,
		common.PelotonResourceManager,
		common.PelotonHostManager,
		taskStore,
	)
	log.Info("Start the PlacementEngine")
	placementEngine.Start()
	defer placementEngine.Stop()

	log.Info("Initialize the Heartbeat process")
	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health)

	select {}
}
