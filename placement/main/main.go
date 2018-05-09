package main

import (
	"os"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"

	"code.uber.internal/infra/peloton/common"
	common_config "code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/common/rpc"
	"code.uber.internal/infra/peloton/placement"
	"code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/storage/stores"
	"code.uber.internal/infra/peloton/yarpc/peer"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"gopkg.in/alecthomas/kingpin.v2"
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

	zkPath = app.Flag(
		"zk-path",
		"Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").
		Envar("MESOS_ZK_PATH").
		String()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

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

	cassandraPort = app.Flag(
		"cassandra-port", "Cassandra port to connect").
		Default("0").
		Envar("CASSANDRA_PORT").
		Int()

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

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	taskType = app.Flag(
		"task-type", "Placement engine task type").
		Default("BATCH").
		Envar("TASK_TYPE").
		String()
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
	var cfg config.Config
	if err := common_config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	// now, override any CLI flags in the loaded config.Config
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

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

	if *datacenter != "" {
		cfg.Storage.Cassandra.CassandraConn.DataCenter = *datacenter
	}

	if *cassandraPort != 0 {
		cfg.Storage.Cassandra.CassandraConn.Port = *cassandraPort
	}

	cfg.Placement.TaskType = resmgr.TaskType_BATCH
	if *taskType != "" {
		if tt, ok := resmgr.TaskType_value[*taskType]; ok {
			cfg.Placement.TaskType = resmgr.TaskType(tt)
			if cfg.Placement.TaskType == resmgr.TaskType_STATEFUL {
				// Use mimir strategy for stateful task placement.
				cfg.Placement.Strategy = config.Mimir
				cfg.Placement.FetchOfferTasks = true
			}
		} else {
			log.WithField("placement_task_type", *taskType).Error("Invalid placement task type")
		}
	}
	log.WithField("placement_task_type", cfg.Placement.TaskType).Info("Placement engine task type")

	log.WithField("config", cfg).Info("Loaded Placement Engine config")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	log.Info("Connecting to HostManager")
	t := rpc.NewTransport()
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.HostManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser for host manager")
	}
	defer hostmgrPeerChooser.Stop()

	hostmgrOutbound := t.NewOutbound(hostmgrPeerChooser)

	log.Info("Connecting to ResourceManager")
	resmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.ResourceManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser for resource manager")
	}
	defer resmgrPeerChooser.Stop()

	resmgrOutbound := t.NewOutbound(resmgrPeerChooser)

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
	defer dispatcher.Stop()

	log.Info("Connect to the TaskStore")
	store := stores.MustCreateStore(&cfg.Storage, rootScope)

	engine := placement.New(
		dispatcher,
		rootScope,
		&cfg.Placement,
		common.PelotonResourceManager,
		common.PelotonHostManager,
		store, // store implements TaskStore
	)
	log.Info("Start the PlacementEngine")
	engine.Start()
	defer engine.Stop()

	log.Info("Initialize the Heartbeat process")
	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, nil)

	select {}
}
