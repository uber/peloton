package main

import (
	"net/url"
	"os"
	"time"

	"code.uber.internal/infra/peloton/common/backoff"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/background"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/common/rpc"
	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/hostmap"
	"code.uber.internal/infra/peloton/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/hostmgr/queue"
	"code.uber.internal/infra/peloton/hostmgr/reconcile"
	"code.uber.internal/infra/peloton/hostmgr/task"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage/stores"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("peloton-hostmgr", "Peloton Host Manager")

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

	configFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	dbHost = app.Flag(
		"db-host",
		"Database host (db.host override) (set $DB_HOST to override)").
		Envar("DB_HOST").
		String()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	httpPort = app.Flag(
		"http-port", "Host manager HTTP port (hostmgr.http_port override) "+
			"(set $HTTP_PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Host manager GRPC port (hostmgr.grpc_port override) "+
			"(set $GRPC_PORT to override)").
		Envar("GRPC_PORT").
		Int()

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

	cassandraPort = app.Flag(
		"cassandra-port", "Cassandra port to connect").
		Default("0").
		Envar("CASSANDRA_PORT").
		Int()

	autoMigrate = app.Flag(
		"auto-migrate", "Automatically update storage schemas.").
		Default("false").
		Envar("AUTO_MIGRATE").
		Bool()

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	mesosSecretFile = app.Flag(
		"mesos-secret-file",
		"Secret file containing one-liner password to connect to Mesos master").
		Default("").
		Envar("MESOS_SECRET_FILE").
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

	log.WithField("files", *configFiles).Info("Loading host manager config")
	var cfg Config
	if err := config.Parse(&cfg, *configFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	// now, override any CLI flags in the loaded config.Config
	if *httpPort != 0 {
		cfg.HostManager.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.HostManager.GRPCPort = *grpcPort
	}

	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
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

	if *cassandraPort != 0 {
		cfg.Storage.Cassandra.CassandraConn.Port = *cassandraPort
	}

	if *autoMigrate {
		cfg.Storage.AutoMigrate = *autoMigrate
	}

	if *datacenter != "" {
		cfg.Storage.Cassandra.CassandraConn.DataCenter = *datacenter
	}

	log.WithField("config", cfg).Debug("Loaded Host Manager config")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonHostManager,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel))

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewInbounds(
		cfg.HostManager.HTTPPort,
		cfg.HostManager.GRPCPort,
		mux,
	)

	mesosMasterDetector, err := mesos.NewZKDetector(cfg.Mesos.ZkPath)
	if err != nil {
		log.Fatalf("Failed to initialize mesos master detector: %v", err)
	}

	// NOTE: we start the server immediately even if no leader has been
	// detected yet.

	rootScope.Counter("boot").Inc(1)

	jobStore, taskStore, _, _, frameworkInfoStore, volumeStore := stores.CreateStores(&cfg.Storage, rootScope)

	authHeader, err := mesos.GetAuthHeader(&cfg.Mesos, *mesosSecretFile)
	if err != nil {
		log.WithError(err).Fatal("Cannot initialize auth header")
	}

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(
		&cfg.Mesos,
		frameworkInfoStore,
		authHeader,
	)

	// Active host manager needs a Mesos inbound
	var mInbound = mhttp.NewInbound(rootScope, driver)
	inbounds = append(inbounds, mInbound)

	// TODO: update Mesos url when leading mesos master changes
	mOutbound := mhttp.NewOutbound(
		mesosMasterDetector,
		driver.Endpoint(),
		authHeader,
	)

	// MasterOperatorClient API outbound
	mOperatorOutbound := mhttp.NewOutbound(
		mesosMasterDetector,
		url.URL{
			Scheme: "http",
			Path:   common.MesosMasterOperatorEndPoint,
		},
		authHeader,
	)

	// All leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")

	// TODO: Delete the outbounds from hostmgr to resmgr after switch
	// eventstream from push to pull (T1014913)

	// Setup the discovery service to detect resmgr leaders and
	// configure the YARPC Peer dynamically
	t := rpc.NewTransport()
	resmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.ResourceManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer resmgrPeerChooser.Stop()

	resmgrOutbound := t.NewOutbound(resmgrPeerChooser)

	outbounds := yarpc.Outbounds{
		common.MesosMasterScheduler: mOutbound,
		common.MesosMasterOperator:  mOperatorOutbound,
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// Mesos callbacks
	// NOTE: This blocks us to move all Mesos related logic into
	// hostmgr.Server because schedulerClient uses dispatcher...
	schedulerClient := mpb.NewSchedulerClient(
		dispatcher.ClientConfig(common.MesosMasterScheduler),
		cfg.Mesos.Encoding,
	)
	masterOperatorClient := mpb.NewMasterOperatorClient(
		dispatcher.ClientConfig(common.MesosMasterOperator),
		cfg.Mesos.Encoding,
	)

	mesos.InitManager(dispatcher, &cfg.Mesos, frameworkInfoStore)

	log.WithFields(log.Fields{
		"http_port": cfg.HostManager.HTTPPort,
		"url_path":  common.PelotonEndpointPath,
	}).Info("HostService initialized")

	// Declare background works
	reconciler := reconcile.NewTaskReconciler(
		schedulerClient,
		rootScope,
		driver,
		jobStore,
		taskStore,
		cfg.HostManager.TaskReconcilerConfig,
	)

	loader := hostmap.Loader{
		OperatorClient: masterOperatorClient,
		Scope:          rootScope.SubScope("hostmap"),
	}

	backgroundManager := background.NewManager()
	// Retry on hostmap loader with Background Manager.
	err = backoff.Retry(
		func() error {
			return backgroundManager.RegisterWorks(
				background.Work{
					Name:   "hostmap",
					Func:   loader.Load,
					Period: cfg.HostManager.HostmapRefreshInterval,
				},
			)
		}, backoff.NewRetryPolicy(cfg.HostManager.HostMgrBackoffRetryCount,
			time.Duration(cfg.HostManager.HostMgrBackoffRetryIntervalSec)*time.Second),
		func(error) bool {
			return true
		})
	if err != nil {
		log.WithError(err).Fatal("Cannot register hostmap loader background worker.")
	}
	// Retry on reconciler registry with Background Manager.
	err = backoff.Retry(
		func() error {
			return backgroundManager.RegisterWorks(
				background.Work{
					Name: "reconciler",
					Func: reconciler.Reconcile,
					Period: time.Duration(
						cfg.HostManager.TaskReconcilerConfig.ReconcileIntervalSec) * time.Second,
					InitialDelay: time.Duration(
						cfg.HostManager.TaskReconcilerConfig.InitialReconcileDelaySec) * time.Second,
				},
			)
		}, backoff.NewRetryPolicy(cfg.HostManager.HostMgrBackoffRetryCount,
			time.Duration(cfg.HostManager.HostMgrBackoffRetryIntervalSec)*time.Second),
		func(error) bool {
			return true
		})
	if err != nil {
		log.WithError(err).Fatal("Cannot register reconciler background worker.")
	}

	offer.InitEventHandler(
		dispatcher,
		rootScope,
		time.Duration(cfg.HostManager.OfferHoldTimeSec)*time.Second,
		time.Duration(cfg.HostManager.OfferPruningPeriodSec)*time.Second,
		schedulerClient,
		volumeStore,
		backgroundManager,
		cfg.HostManager.HostPruningPeriodSec,
	)

	maintenanceQueue := queue.NewMaintenanceQueue()

	// Init service handler.
	hostmgr.InitServiceHandler(
		dispatcher,
		rootScope,
		schedulerClient,
		masterOperatorClient,
		driver,
		volumeStore,
		cfg.Mesos,
		mesosMasterDetector,
	)

	hostsvc.InitServiceHandler(
		dispatcher,
		rootScope,
		masterOperatorClient,
		maintenanceQueue,
	)

	// Initializing TaskStateManager will start to record task status
	// update back to storage.  TODO(zhitao): This is
	// temporary. Eventually we should create proper API protocol for
	// `WaitTaskStatusUpdate` and allow RM/JM to retrieve this
	// separately.
	task.InitTaskStateManager(
		dispatcher,
		cfg.HostManager.TaskUpdateBufferSize,
		cfg.HostManager.TaskUpdateAckConcurrency,
		common.PelotonResourceManager,
		rootScope,
	)

	recoveryHandler := hostmgr.NewRecoveryHandler(rootScope, masterOperatorClient, maintenanceQueue)

	server := hostmgr.NewServer(
		rootScope,
		backgroundManager,
		cfg.HostManager.HTTPPort,
		cfg.HostManager.GRPCPort,
		mesosMasterDetector,
		mInbound,
		mOutbound,
		reconciler,
		recoveryHandler,
	)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.HostManagerRole,
		server,
	)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}
	err = candidate.Start()
	if err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer candidate.Stop()

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	log.WithFields(log.Fields{
		"httpPort": cfg.HostManager.HTTPPort,
		"grpcPort": cfg.HostManager.GRPCPort,
	}).Info("Started host manager")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	select {}
}
