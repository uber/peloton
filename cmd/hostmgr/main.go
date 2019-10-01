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
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"
	cqos "github.com/uber/peloton/.gen/qos/v1alpha1"

	"github.com/uber/peloton/pkg/auth"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/backoff"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr"
	bin_packing "github.com/uber/peloton/pkg/hostmgr/binpacking"
	"github.com/uber/peloton/pkg/hostmgr/goalstate"
	"github.com/uber/peloton/pkg/hostmgr/host"
	"github.com/uber/peloton/pkg/hostmgr/host/drainer"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/hostmover"
	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	"github.com/uber/peloton/pkg/hostmgr/hostsvc"
	"github.com/uber/peloton/pkg/hostmgr/mesos"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/transport/mhttp"
	hostmetric "github.com/uber/peloton/pkg/hostmgr/metrics"
	"github.com/uber/peloton/pkg/hostmgr/offer"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostcache"
	"github.com/uber/peloton/pkg/hostmgr/p2k/hostmgrsvc"
	"github.com/uber/peloton/pkg/hostmgr/p2k/plugins"
	mesosplugins "github.com/uber/peloton/pkg/hostmgr/p2k/plugins/mesos"
	"github.com/uber/peloton/pkg/hostmgr/p2k/podeventmanager"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/queue"
	"github.com/uber/peloton/pkg/hostmgr/reconcile"
	"github.com/uber/peloton/pkg/hostmgr/watchevent"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"
	"github.com/uber/peloton/pkg/storage/cassandra"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	"github.com/uber/peloton/pkg/storage/stores"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
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

	pelotonSecretFile = app.Flag(
		"peloton-secret-file",
		"Secret file containing all Peloton secrets").
		Default("").
		Envar("PELOTON_SECRET_FILE").
		String()

	scarceResourceTypes = app.Flag(
		"scarce-resource-type", "Scarce Resource Type.").
		Envar("SCARCE_RESOURCE_TYPES").
		String()

	slackResourceTypes = app.Flag(
		"slack-resource-type", "Slack Resource Type.").
		Envar("SLACK_RESOURCE_TYPES").
		String()

	enableRevocableResources = app.Flag(
		"enable-revocable-resources", "Revcocable Resources Enabled").
		Envar("ENABLE_REVOCABLE_RESOURCES").
		Bool()

	binPacking = app.Flag(
		"bin_packing", "Bin Packing enable/disable, by default disabled.").
		Envar("BIN_PACKING").
		String()

	qoSAdvisorServiceAddress = app.Flag(
		"qos_advisor_service_address", "Qos advisor service address.").
		Default("").
		Envar("QOS_ADVISOR_SERVICE_ADDRESS").
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

	kubeConfigFile = app.Flag(
		"kube-config-file",
		"YAML config file for kubernetes plugin").
		Default("").
		Envar("KUBECONFIG").
		String()

	enableK8s = app.Flag(
		"enable-k8s",
		"Enable k8s plugin").
		Envar("ENABLE_K8S").
		Bool()

	enableHostPool = app.Flag(
		"enable-host-pool",
		"Enable Host Pool Management").
		Envar("ENABLE_HOST_POOL").
		Bool()
)

const (
	taskEvictionQueueName = "task-eviction-queue"
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(
		&logging.LogFieldFormatter{
			Formatter: &logging.SecretsFormatter{Formatter: &log.JSONFormatter{}},
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

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if !*useCassandra {
		cfg.Storage.UseCassandra = false
	}

	if *cassandraHosts != nil && len(*cassandraHosts) > 0 {
		cfg.Storage.Cassandra.CassandraConn.ContactPoints = *cassandraHosts
	}

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	// Parse and setup peloton secrets
	if *pelotonSecretFile != "" {
		var secretsCfg config.PelotonSecretsConfig
		if err := config.Parse(&secretsCfg, *pelotonSecretFile); err != nil {
			log.WithError(err).
				WithField("peloton_secret_file", *pelotonSecretFile).
				Fatal("Cannot parse secret config")
		}
		cfg.Storage.Cassandra.CassandraConn.Username =
			secretsCfg.CassandraUsername
		cfg.Storage.Cassandra.CassandraConn.Password =
			secretsCfg.CassandraPassword
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

	if *scarceResourceTypes != "" {
		log.Info(strings.Split(*scarceResourceTypes, ","))
		cfg.HostManager.ScarceResourceTypes = strings.Split(*scarceResourceTypes, ",")
	}

	if *slackResourceTypes != "" {
		log.Info(strings.Split(*slackResourceTypes, ","))
		cfg.HostManager.SlackResourceTypes = strings.Split(*slackResourceTypes, ",")
	}

	if *enableRevocableResources {
		log.Info("Revocable Resource Enabled")
		cfg.Mesos.Framework.RevocableResourcesSupported = *enableRevocableResources
	}

	if *binPacking != "" {
		log.Info("Bin Packing is enabled")
		cfg.HostManager.BinPacking = *binPacking
	}

	if *enableK8s {
		cfg.K8s.Enabled = true
	}

	if *kubeConfigFile != "" {
		cfg.K8s.Kubeconfig = *kubeConfigFile
	}

	if *qoSAdvisorServiceAddress != "" {
		cfg.HostManager.QoSAdvisorService.Address = *qoSAdvisorServiceAddress
	}

	if *enableHostPool {
		log.Info("Host Pool Management Enabled")
		cfg.HostManager.EnableHostPool = *enableHostPool
	}

	log.WithField("config", cfg).Info("Loaded Host Manager configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonHostManager,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel))

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewInbounds(
		cfg.HostManager.HTTPPort,
		cfg.HostManager.GRPCPort,
		mux,
	)

	// TODO: Skip it when k8s is enabled.
	mesosMasterDetector, err := mesos.NewZKDetector(cfg.Mesos.ZkPath)
	if err != nil {
		log.Fatalf("Failed to initialize mesos master detector: %v", err)
	}

	// NOTE: we start the server immediately even if no leader has been
	// detected yet.

	rootScope.Counter("boot").Inc(1)

	store := stores.MustCreateStore(&cfg.Storage, rootScope)

	ormStore, ormErr := ormobjects.NewCassandraStore(
		cassandra.ToOrmConfig(&cfg.Storage.Cassandra),
		rootScope)
	if ormErr != nil {
		log.WithError(ormErr).Fatal("Failed to create ORM store for Cassandra")
	}
	activeJobsOps := ormobjects.NewActiveJobsOps(ormStore)

	authHeader, err := mesos.GetAuthHeader(&cfg.Mesos, *mesosSecretFile)
	if err != nil {
		log.WithError(err).Fatal("Cannot initialize auth header")
	}

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(
		&cfg.Mesos,
		store, // store implements FrameworkInfoStore
		authHeader,
	)

	// Active host manager needs a Mesos inbound
	var mInbound = mhttp.NewInbound(rootScope, driver)
	inbounds = append(inbounds, mInbound)

	// TODO: update Mesos url when leading mesos master changes
	mOutbound := mhttp.NewOutbound(
		rootScope,
		mesosMasterDetector,
		driver.Endpoint(),
		authHeader,
		mhttp.MaxConnectionsPerHost(cfg.Mesos.Framework.MaxConnectionsToMesosMaster),
	)

	// MasterOperatorClient API outbound
	mOperatorOutbound := mhttp.NewOutbound(
		rootScope,
		mesosMasterDetector,
		url.URL{
			Scheme: "http",
			Path:   common.MesosMasterOperatorEndPoint,
		},
		authHeader,
		mhttp.MaxConnectionsPerHost(cfg.Mesos.Framework.MaxConnectionsToMesosMaster),
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

	if cfg.HostManager.QoSAdvisorService.Address != "" {
		var cqosOutbound *grpc.Outbound
		// Setup the discovery service to detect QoSAdvisorService
		cqosTransport := grpc.NewTransport()
		cqosOutbound = cqosTransport.NewSingleOutbound(cfg.HostManager.
			QoSAdvisorService.Address)
		outbounds[common.QoSAdvisorService] =
			transport.Outbounds{
				Unary: cqosOutbound,
			}
	}

	securityManager, err := auth_impl.CreateNewSecurityManager(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not enable security feature")
	}

	authInboundMiddleware := inbound.NewAuthInboundMiddleware(securityManager)

	securityClient, err := auth_impl.CreateNewSecurityClient(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not establish secure inter-component communication")
	}

	authOutboundMiddleware := outbound.NewAuthOutboundMiddleware(securityClient)
	leaderCheckMiddleware := &inbound.LeaderCheckInboundMiddleware{}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  yarpc.UnaryInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware),
			Oneway: yarpc.OnewayInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware),
			Stream: yarpc.StreamInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware),
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authOutboundMiddleware,
			Oneway: authOutboundMiddleware,
			Stream: authOutboundMiddleware,
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

	mesos.InitManager(
		dispatcher,
		&cfg.Mesos,
		store, // store implements FrameworkInfoStore
	)

	var cQosClient cqos.QoSAdvisorServiceYARPCClient
	if cfg.HostManager.QoSAdvisorService.Address != "" {
		cQosClient = cqos.NewQoSAdvisorServiceYARPCClient(
			dispatcher.ClientConfig(common.QoSAdvisorService))
	}
	log.WithFields(log.Fields{
		"http_port": cfg.HostManager.HTTPPort,
		"url_path":  common.PelotonEndpointPath,
	}).Info("HostService initialized")

	// Declare background works
	reconciler := reconcile.NewTaskReconciler(
		schedulerClient,
		rootScope,
		driver,
		activeJobsOps,
		store, // store implements TaskStore
		cfg.HostManager.TaskReconcilerConfig,
	)

	ormobjects.InitHostInfoOps(ormStore)

	loader := host.Loader{
		OperatorClient:     masterOperatorClient,
		Scope:              rootScope.SubScope("hostmap"),
		SlackResourceTypes: cfg.HostManager.SlackResourceTypes,
		HostInfoOps:        ormobjects.GetHostInfoOps(),
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

	metric := hostmetric.NewMetrics(rootScope)
	if cfg.HostManager.QoSAdvisorService.Address != "" {
		bin_packing.Init(cQosClient, metric)
	} else {
		bin_packing.Init(nil, nil)
	}

	log.WithField("ranker_name", cfg.HostManager.BinPacking).
		Info("Bin packing is enabled")
	defaultRanker := bin_packing.GetRankerByName(cfg.HostManager.BinPacking)
	if defaultRanker == nil {
		log.WithField("ranker_name", cfg.HostManager.BinPacking).
			Fatal("Ranker not found")
	}

	watchevent.InitWatchProcessor(cfg.HostManager.Watch, metric)
	watchProcessor := watchevent.GetWatchProcessor()

	plugin := plugins.NewNoopPlugin()
	var hostCache hostcache.HostCache
	podEventCh := make(chan *scalar.PodEvent, plugins.EventChanSize)
	hostEventCh := make(chan *scalar.HostEvent, plugins.EventChanSize)

	// If k8s is enabled, return a k8s plugin.
	// TODO: start MesosPlugin after it's implemented.
	if cfg.K8s.Enabled {
		var err error
		plugin, err = plugins.NewK8sPlugin(
			cfg.K8s.Kubeconfig,
			podEventCh,
			hostEventCh,
		)
		if err != nil {
			log.WithError(err).Fatal("Cannot init host manager plugin.")
		}
	}

	// a temporary measure to enable mesos plugins for some usecases,
	// it is not fully ready to be used for v1alpha handler.
	mesosPlugin := mesosplugins.NewMesosManager(
		dispatcher,
		driver,
		schedulerClient,
		masterOperatorClient,
		cfg.HostManager.HostmapRefreshInterval,
		time.Duration(cfg.HostManager.OfferHoldTimeSec)*time.Second,
		rootScope,
		podEventCh,
		hostEventCh,
	)

	// Initialize offer pool event handler with nil host pool manager.
	// TODO: Refactor event stream handler and move it out of offer package
	//  to avoid circular dependency, since now offer pool event handler requires
	//  host pool manager, and host pool manager requires event stream handler
	//  which is part of offer pool event handler.
	offer.InitEventHandler(
		dispatcher,
		rootScope,
		schedulerClient,
		resmgrsvc.NewResourceManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonResourceManager)),
		backgroundManager,
		defaultRanker,
		cfg.HostManager,
		watchProcessor,
		nil,
		mesosPlugin,
	)

	// Construct host pool manager if it is enabled.
	var hostPoolManager manager.HostPoolManager
	if cfg.HostManager.EnableHostPool {
		hostPoolManager = manager.New(
			cfg.HostManager.HostPoolReconcileInterval,
			offer.GetEventHandler().GetEventStreamHandler(),
			ormobjects.GetHostInfoOps(),
			rootScope,
		)

		// Set host pool manager in offer pool event handler.
		offer.GetEventHandler().SetHostPoolManager(hostPoolManager)
	}

	// Create host cache instance.
	hostCache = hostcache.New(
		hostEventCh,
		backgroundManager,
		rootScope,
	)

	pem := podeventmanager.New(
		dispatcher,
		podEventCh,
		plugin,
		hostCache,
		cfg.HostManager.TaskUpdateBufferSize,
		rootScope,
		cfg.K8s.Enabled)

	// Create v1alpha hostmgr internal service handler.
	hostmgrsvc.NewServiceHandler(
		dispatcher,
		rootScope,
		plugin,
		hostCache,
		pem,
	)

	// Create Goal State Engine driver
	goalStateDriver := goalstate.NewDriver(
		ormobjects.GetHostInfoOps(),
		masterOperatorClient,
		rootScope,
		cfg.HostManager.GoalState,
		hostPoolManager,
	)

	// Create Host mover object
	hostMover := hostmover.NewHostMover(
		hostPoolManager,
		ormobjects.GetHostInfoOps(),
		goalStateDriver,
		rootScope,
		resmgrsvc.NewResourceManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonResourceManager)),
	)

	taskEvictionQueue := queue.NewTaskQueue(taskEvictionQueueName)

	// Create new hostmgr internal service handler.
	serviceHandler := hostmgr.NewServiceHandler(
		dispatcher,
		metric,
		schedulerClient,
		masterOperatorClient,
		driver,
		cfg.Mesos,
		mesosMasterDetector,
		&cfg.HostManager,
		cfg.HostManager.SlackResourceTypes,
		watchProcessor,
		hostPoolManager,
		goalStateDriver,
		ormobjects.GetHostInfoOps(),
		hostCache,
		mesosPlugin,
	)

	hostDrainer := drainer.NewDrainer(
		cfg.HostManager.HostDrainerPeriod,
		cfg.Mesos.Framework.Role,
		masterOperatorClient,
		goalStateDriver,
		ormobjects.GetHostInfoOps(),
		taskEvictionQueue,
	)

	hostsvc.InitServiceHandler(
		dispatcher,
		rootScope,
		hostDrainer,
		hostPoolManager,
		hostMover,
	)

	recoveryHandler := hostmgr.NewRecoveryHandler(
		rootScope,
		store,
		ormStore,
		hostCache,
	)

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
		hostDrainer,
		serviceHandler.GetReserver(),
		watchProcessor,
		plugin,
		hostCache,
		mesosPlugin,
		hostPoolManager,
	)
	server.Start()

	// Start dispatch loop.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	// Set nomination for leader check middleware
	leaderCheckMiddleware.SetNomination(server)

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

	// Start dispatch loop.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	log.WithFields(log.Fields{
		"httpPort": cfg.HostManager.HTTPPort,
		"grpcPort": cfg.HostManager.GRPCPort,
	}).Info("Started host manager")

	// We can *honestly* say the server is booted up now.
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	// Start collecting runtime metrics.
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	select {}
}
