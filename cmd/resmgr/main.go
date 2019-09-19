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

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	pb_hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	"github.com/uber/peloton/pkg/auth"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"
	"github.com/uber/peloton/pkg/resmgr"
	"github.com/uber/peloton/pkg/resmgr/entitlement"
	maintenance "github.com/uber/peloton/pkg/resmgr/host"
	"github.com/uber/peloton/pkg/resmgr/hostmover"
	"github.com/uber/peloton/pkg/resmgr/preemption"
	"github.com/uber/peloton/pkg/resmgr/respool"
	"github.com/uber/peloton/pkg/resmgr/respool/respoolsvc"
	"github.com/uber/peloton/pkg/resmgr/task"
	"github.com/uber/peloton/pkg/storage/cassandra"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	"github.com/uber/peloton/pkg/storage/stores"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("peloton-resmgr", "Peloton Resource Manager")

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

	httpPort = app.Flag(
		"http-port", "Resource manager HTTP port (resmgr.http_port override) "+
			"(set $HTTP_PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Resource manager GRPC port (resmgr.grpc_port override) "+
			"(set $GRPC_PORT to override)").
		Envar("GRPC_PORT").
		Int()

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

	pelotonSecretFile = app.Flag(
		"peloton-secret-file",
		"Secret file containing all Peloton secrets").
		Default("").
		Envar("PELOTON_SECRET_FILE").
		String()

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	enablePreemption = app.Flag(
		"enable_preemption", "Enabling preemption").
		Default("false").
		Envar("ENABLE_PREEMPTION").
		Bool()

	taskPreemptionPeriod = app.Flag(
		"task_preemption_period",
		"Setting task preemption period").
		Envar("TASK_PREEMPTION_PERIOD").
		Duration()

	enableSLATracking = app.Flag(
		"enable_sla_tracking", "Enabling SLA tracking").
		Default("false").
		Envar("ENABLE_SLA_TRACKING").
		Bool()

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

	hostMgrAPIVersionStr = app.Flag(
		"hostmgr-api-version",
		"Define the API Version of host manager").
		Default("").
		Envar("HOSTMGR_API_VERSION").
		String()

	useHostPool = app.Flag(
		"use_host_pool", "Using host pool").
		Default("false").
		Envar("USE_HOST_POOL").
		Bool()
)

func getConfig(cfgFiles ...string) Config {
	log.WithField("files", cfgFiles).
		Info("Loading Resource Manager config")

	var cfg Config
	if err := config.Parse(&cfg, cfgFiles...); err != nil {
		log.WithError(err).Fatal("Cannot parse yaml config")
	}
	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	// now, override any CLI flags in the loaded config.Config
	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}
	if *httpPort != 0 {
		cfg.ResManager.HTTPPort = *httpPort
	}
	if *grpcPort != 0 {
		cfg.ResManager.GRPCPort = *grpcPort
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
	if *datacenter != "" {
		cfg.Storage.Cassandra.CassandraConn.DataCenter = *datacenter
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

	if *enablePreemption {
		cfg.ResManager.PreemptionConfig.Enabled = *enablePreemption
	}
	if *taskPreemptionPeriod != 0 {
		cfg.ResManager.PreemptionConfig.TaskPreemptionPeriod = *taskPreemptionPeriod
	}
	if *enableSLATracking {
		cfg.ResManager.RmTaskConfig.EnableSLATracking = *enableSLATracking
	}
	if *hostMgrAPIVersionStr != "" {
		hostMgrAPIVersion, err := api.ParseVersion(*hostMgrAPIVersionStr)
		if err != nil {
			log.WithError(err).Fatal("Failed to parse hostmgr-api-version")
		}
		cfg.ResManager.HostManagerAPIVersion = hostMgrAPIVersion
	}
	if cfg.ResManager.HostManagerAPIVersion == "" {
		cfg.ResManager.HostManagerAPIVersion = api.V0
	}

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}
	if *useHostPool {
		cfg.ResManager.UseHostPool = *useHostPool
	}

	log.
		WithField("config", cfg).
		Info("Loaded Resource Manager config")
	return cfg
}

func main() {
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

	cfg := getConfig(*cfgFiles...)

	log.WithField("config", cfg).
		Info("Completed Resource Manager config")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonResourceManager,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()
	rootScope.Counter("boot").Inc(1)

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))
	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	store := stores.MustCreateStore(&cfg.Storage, rootScope)
	ormStore, ormErr := ormobjects.NewCassandraStore(
		cassandra.ToOrmConfig(&cfg.Storage.Cassandra),
		rootScope)
	if ormErr != nil {
		log.WithError(ormErr).Fatal("Failed to create ORM store for Cassandra")
	}
	respoolOps := ormobjects.NewResPoolOps(ormStore)
	activeJobsOps := ormobjects.NewActiveJobsOps(ormStore)

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewInbounds(
		cfg.ResManager.HTTPPort,
		cfg.ResManager.GRPCPort,
		mux,
	)

	// all leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")
	// setup the discovery service to detect hostmgr leaders and
	// configure the YARPC Peer dynamically
	t := rpc.NewTransport()
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.HostManagerRole,
		t,
	)
	if err != nil {
		log.
			WithError(err).
			WithField("role", common.HostManagerRole).
			Fatal("Could not create smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()

	hostmgrOutbound := t.NewOutbound(hostmgrPeerChooser)

	outbounds := yarpc.Outbounds{
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	securityManager, err := auth_impl.CreateNewSecurityManager(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not enable security feature")
	}

	authInboundMiddleware := inbound.NewAuthInboundMiddleware(securityManager)
	yarpcMetricsMiddleware := &inbound.YAPRCMetricsInboundMiddleware{Scope: rootScope.SubScope("yarpc")}

	securityClient, err := auth_impl.CreateNewSecurityClient(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not establish secure inter-component communication")
	}

	authOutboundMiddleware := outbound.NewAuthOutboundMiddleware(securityClient)
	leaderCheckMiddleware := &inbound.LeaderCheckInboundMiddleware{}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonResourceManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  yarpc.UnaryInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware, yarpcMetricsMiddleware),
			Oneway: yarpc.OnewayInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware, yarpcMetricsMiddleware),
			Stream: yarpc.StreamInboundMiddleware(authInboundMiddleware, leaderCheckMiddleware, yarpcMetricsMiddleware),
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authOutboundMiddleware,
			Oneway: authOutboundMiddleware,
			Stream: authOutboundMiddleware,
		},
	})

	hostmgrClient := hostsvc.NewInternalHostServiceYARPCClient(
		dispatcher.ClientConfig(
			common.PelotonHostManager),
	)

	hostServiceClient := pb_hostsvc.NewHostServiceYARPCClient(
		dispatcher.ClientConfig(
			common.PelotonHostManager),
	)

	// Initializing Resource Pool Tree.
	tree := respool.NewTree(
		rootScope,
		respoolOps,
		store, // store implements JobStore
		store, // store implements TaskStore
		*cfg.ResManager.PreemptionConfig)

	// Initialize resource pool service handlers
	respoolsvc.InitServiceHandler(
		dispatcher,
		rootScope,
		tree,
		ormobjects.NewResPoolOps(ormStore),
	)

	// Initializing the rmtasks in-memory tracker
	task.InitTaskTracker(
		rootScope,
		cfg.ResManager.RmTaskConfig,
	)

	// Initializing the task scheduler
	task.InitScheduler(
		rootScope,
		tree,
		cfg.ResManager.TaskSchedulingPeriod,
		task.GetTracker(),
	)

	// Initializing the entitlement calculator
	calculator := entitlement.NewCalculator(
		cfg.ResManager.EntitlementCaculationPeriod,
		rootScope,
		dispatcher,
		tree,
		cfg.ResManager.HostManagerAPIVersion,
		cfg.ResManager.UseHostPool,
	)

	// Initializing the task reconciler
	reconciler := task.NewReconciler(
		task.GetTracker(),
		store, // store implements TaskStore
		rootScope,
		cfg.ResManager.TaskReconciliationPeriod,
	)

	// Initializing the task preemptor
	preemptor := preemption.NewPreemptor(
		rootScope,
		cfg.ResManager.PreemptionConfig,
		task.GetTracker(),
		tree,
	)

	// Initializing the host drainer
	drainer := maintenance.NewDrainer(
		rootScope,
		hostmgrClient,
		cfg.ResManager.HostDrainerPeriod,
		task.GetTracker(),
		preemptor)

	// Initializing the batch scorer
	batchScorer := hostmover.NewBatchScorer(
		cfg.ResManager.EnableHostScorer,
		hostServiceClient)

	// Initialize resource manager service handlers
	serviceHandler := resmgr.NewServiceHandler(
		dispatcher,
		rootScope,
		task.GetTracker(),
		batchScorer,
		tree,
		preemptor,
		hostmgrClient,
		cfg.ResManager,
	)

	// Initialize recovery
	recoveryHandler := resmgr.NewRecovery(
		rootScope,
		store, // store implements TaskStore
		activeJobsOps,
		ormobjects.NewJobConfigOps(ormStore),
		ormobjects.NewJobRuntimeOps(ormStore),
		serviceHandler,
		tree,
		cfg.ResManager,
		hostmgrClient,
	)

	// Initialize the server
	server := resmgr.NewServer(rootScope,
		cfg.ResManager.HTTPPort,
		cfg.ResManager.GRPCPort,
		tree,
		recoveryHandler,
		calculator,
		reconciler,
		preemptor,
		drainer,
		batchScorer,
	)
	// Set nomination for leader check middleware
	leaderCheckMiddleware.SetNomination(server)

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.ResourceManagerRole,
		server,
	)

	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}

	if err = candidate.Start(); err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer candidate.Stop()

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	log.WithFields(log.Fields{
		"http_port": cfg.ResManager.HTTPPort,
		"grpc_port": cfg.ResManager.GRPCPort,
	}).Info("Started resource manager")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	// start collecting runtime metrics
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	select {}
}
