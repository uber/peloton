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

	"github.com/uber/peloton/pkg/auth"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/api"
	"github.com/uber/peloton/pkg/common/async"
	"github.com/uber/peloton/pkg/common/buildversion"
	common_config "github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"
	"github.com/uber/peloton/pkg/placement"
	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/hosts"
	tally_metrics "github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/offers"
	offers_v0 "github.com/uber/peloton/pkg/placement/offers/v0"
	offers_v1 "github.com/uber/peloton/pkg/placement/offers/v1"
	"github.com/uber/peloton/pkg/placement/plugins"
	"github.com/uber/peloton/pkg/placement/plugins/batch"
	mimir_strategy "github.com/uber/peloton/pkg/placement/plugins/mimir"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/algorithms"
	"github.com/uber/peloton/pkg/placement/tasks"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostsvc_v1 "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

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

	taskDequeueLimit = app.Flag(
		"task-dequeue-limit", "Number of tasks to dequeue").
		Envar("TASK_DEQUEUE_LIMIT").
		Int()

	taskDequeuePeriod = app.Flag(
		"task-dequeue-period", "Period at which tasks are dequeued to be placed in seconds").
		Envar("TASK_DEQUEUE_PERIOD").
		Default("0").
		Int()

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
		"use-host-pool", "Use host pool").
		Default("false").
		Envar("USE_HOST_POOL").
		Bool()
)

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

	log.WithField("files", *cfgFiles).
		Info("Loading Placement Engnine config")
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

	if *taskType != "" {
		overridePlacementStrategy(*taskType, &cfg)
	}

	if *taskDequeueLimit != 0 {
		cfg.Placement.TaskDequeueLimit = *taskDequeueLimit
	}

	if *taskDequeuePeriod != 0 {
		cfg.Placement.TaskDequeuePeriod = time.Duration(*taskDequeuePeriod) * time.Second
	}

	if *hostMgrAPIVersionStr != "" {
		hostMgrAPIVersion, err := api.ParseVersion(*hostMgrAPIVersionStr)
		if err != nil {
			log.WithError(err).Fatal("Failed to parse hostmgr-api-version")
		}
		cfg.Placement.HostManagerAPIVersion = hostMgrAPIVersion
	}
	if cfg.Placement.HostManagerAPIVersion == "" {
		cfg.Placement.HostManagerAPIVersion = api.V0
	}

	if *useHostPool {
		log.Info("Use Host Pool for placement")
		cfg.Placement.UseHostPool = true
	}

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	if cfg.Placement.HostManagerAPIVersion == "" {
		cfg.Placement.HostManagerAPIVersion = api.V0
	}

	log.WithField("placement_task_type", cfg.Placement.TaskType).
		WithField("strategy", cfg.Placement.Strategy).
		Info("Placement engine type")

	log.WithField("config", cfg).
		Info("Completed Loading Placement Engine config")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))
	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	log.Info("Connecting to HostManager")
	t := rpc.NewTransport()
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.HostManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error": err,
				"role":  common.HostManagerRole},
		).Fatal("Could not create smart peer chooser for host manager")
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
		log.WithFields(
			log.Fields{
				"error": err,
				"role":  common.ResourceManagerRole},
		).Fatal("Could not create smart peer chooser for resource manager")
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
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  authInboundMiddleware,
			Oneway: authInboundMiddleware,
			Stream: authInboundMiddleware,
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authOutboundMiddleware,
			Oneway: authOutboundMiddleware,
			Stream: authOutboundMiddleware,
		},
	})

	log.Debug("Starting YARPC dispatcher")
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start dispatcher: %v", err)
	}
	defer dispatcher.Stop()

	tallyMetrics := tally_metrics.NewMetrics(
		rootScope.SubScope("placement"))
	resourceManager := resmgrsvc.NewResourceManagerServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonResourceManager))
	hostManager := hostsvc.NewInternalHostServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonHostManager))

	var offerService offers.Service
	if cfg.Placement.HostManagerAPIVersion.IsV1() {
		hostManagerV1 := hostsvc_v1.NewHostManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager))
		offerService = offers_v1.NewService(
			hostManagerV1,
			resourceManager,
			tallyMetrics,
		)
	} else {
		offerService = offers_v0.NewService(
			hostManager,
			resourceManager,
			tallyMetrics,
		)
	}
	taskService := tasks.NewService(
		resourceManager,
		&cfg.Placement,
		tallyMetrics,
	)
	hostsService := hosts.NewService(
		hostManager,
		resourceManager,
		tallyMetrics,
	)

	strategy := initPlacementStrategy(cfg)

	pool := async.NewPool(async.PoolOptions{
		MaxWorkers: cfg.Placement.Concurrency,
	}, nil)
	pool.Start()

	engine := placement.New(
		rootScope,
		&cfg.Placement,
		offerService,
		taskService,
		hostsService,
		strategy,
		pool,
	)
	log.Info("Start the PlacementEngine")
	engine.Start()
	defer engine.Stop()

	log.Info("Initialize the Heartbeat process")
	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, nil)

	// start collecting runtime metrics
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	select {}
}

func initPlacementStrategy(cfg config.Config) plugins.Strategy {
	var strategy plugins.Strategy
	switch cfg.Placement.Strategy {
	case config.Batch:
		strategy = batch.New(&cfg.Placement)
	case config.Mimir:
		// TODO avyas check mimir concurrency parameters
		cfg.Placement.Concurrency = 1
		placer := algorithms.NewPlacer(4, 300)
		strategy = mimir_strategy.New(placer, &cfg.Placement)
	}
	return strategy
}

// overrides the strategy based on the task type supplied at runtime.
func overridePlacementStrategy(taskType string, cfg *config.Config) {
	tt, ok := resmgr.TaskType_value[taskType]
	if !ok {
		log.WithField("placement_task_type", taskType).
			Fatal("Invalid placement task type")
	}

	cfg.Placement.TaskType = resmgr.TaskType(tt)
	switch cfg.Placement.TaskType {
	case resmgr.TaskType_STATEFUL, resmgr.TaskType_STATELESS:
		// Use mimir strategy for stateful and stateless task placement.
		cfg.Placement.Strategy = config.Mimir
		cfg.Placement.FetchOfferTasks = true
	default:
		// Use batch strategy for everything else.
		cfg.Placement.Strategy = config.Batch
		cfg.Placement.FetchOfferTasks = false
	}
}
