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
	"net/http"
	"os"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/auth"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/background"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/jobmgr"
	"github.com/uber/peloton/pkg/jobmgr/cached"
	"github.com/uber/peloton/pkg/jobmgr/goalstate"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc"
	"github.com/uber/peloton/pkg/jobmgr/jobsvc/stateless"
	"github.com/uber/peloton/pkg/jobmgr/logmanager"
	"github.com/uber/peloton/pkg/jobmgr/podsvc"
	"github.com/uber/peloton/pkg/jobmgr/task/activermtask"
	"github.com/uber/peloton/pkg/jobmgr/task/deadline"
	"github.com/uber/peloton/pkg/jobmgr/task/event"
	"github.com/uber/peloton/pkg/jobmgr/task/launcher"
	"github.com/uber/peloton/pkg/jobmgr/task/placement"
	"github.com/uber/peloton/pkg/jobmgr/task/preemptor"
	"github.com/uber/peloton/pkg/jobmgr/tasksvc"
	"github.com/uber/peloton/pkg/jobmgr/updatesvc"
	"github.com/uber/peloton/pkg/jobmgr/volumesvc"
	"github.com/uber/peloton/pkg/jobmgr/watchsvc"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"
	"github.com/uber/peloton/pkg/storage/stores"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

const (
	_httpClientTimeout = 15 * time.Second
)

var (
	version string
	app     = kingpin.New(common.PelotonJobManager, "Peloton Job Manager")

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
		"http-port", "Job manager HTTP port (jobmgr.http_port override) "+
			"(set $PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Job manager gRPC port (jobmgr.grpc_port override) "+
			"(set $PORT to override)").
		Envar("GRPC_PORT").
		Int()

	placementDequeLimit = app.Flag(
		"placement-dequeue-limit", "Placements dequeue limit for each get "+
			"placements attempt (jobmgr.placement_dequeue_limit override) "+
			"(set $PLACEMENT_DEQUEUE_LIMIT to override)").
		Envar("PLACEMENT_DEQUEUE_LIMIT").
		Int()

	getPlacementsTimeout = app.Flag(
		"get-placements-timeout", "Timeout in milisecs for GetPlacements call "+
			"(jobmgr.get_placements_timeout override) "+
			"(set $GET_PLACEMENTS_TIMEOUT to override) ").
		Envar("GET_PLACEMENTS_TIMEOUT").
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

	mesosAgentWorkDir = app.Flag(
		"mesos-agent-work-dir", "Mesos agent work dir").
		Default("/var/lib/mesos/agent").
		Envar("MESOS_AGENT_WORK_DIR").
		String()

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	enableSecrets = app.Flag(
		"enable-secrets", "enable handing secrets for this cluster").
		Default("false").
		Envar("ENABLE_SECRETS").
		Bool()

	// TODO: remove this flag and all related code after
	// storage layer can figure out recovery
	jobType = app.Flag(
		"job-type", "Cluster job type").
		Default("BATCH").
		Envar("JOB_TYPE").
		Enum("BATCH", "SERVICE")

	jobRuntimeCalculationViaCache = app.Flag(
		"job-runtime-calculation-via-cache",
		"Enable runtime re-calculation from cache "+
			"when MV diverged").
		Default("false").
		Envar("JOB_RUNTIME_CALCULATION_VIA_CACHE").
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

	log.WithField("job_type", jobType).Info("Loaded job type for the cluster")

	log.WithField("files", *cfgFiles).Info("Loading job manager config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	if *enableSecrets {
		cfg.JobManager.JobSvcCfg.EnableSecrets = true
	}

	if *jobRuntimeCalculationViaCache {
		cfg.JobManager.JobRuntimeCalculationViaCache = true
	}
	// now, override any CLI flags in the loaded config.Config
	if *httpPort != 0 {
		cfg.JobManager.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.JobManager.GRPCPort = *grpcPort
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if *placementDequeLimit != 0 {
		cfg.JobManager.Placement.PlacementDequeueLimit = *placementDequeLimit
	}

	if *getPlacementsTimeout != 0 {
		cfg.JobManager.Placement.GetPlacementsTimeout = *getPlacementsTimeout
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

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	log.WithField("config", cfg).Info("Loaded Job Manager configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonJobManager,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel),
	)

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	// store implements JobStore, TaskStore, VolumeStore, UpdateStore
	// and FrameworkInfoStore
	store := stores.MustCreateStore(&cfg.Storage, rootScope)
	ormStore, ormErr := ormobjects.NewCassandraStore(
		&cfg.Storage.Cassandra,
		rootScope)
	if ormErr != nil {
		log.WithError(ormErr).Fatal("Failed to create ORM store for Cassandra")
	}

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewInbounds(
		cfg.JobManager.HTTPPort,
		cfg.JobManager.GRPCPort,
		mux,
	)

	// all leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")
	// setup the discovery service to detect resmgr leaders and
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

	// setup the discovery service to detect hostmgr leaders and
	// configure the YARPC Peer dynamically
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.HostManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()

	hostmgrOutbound := t.NewOutbound(hostmgrPeerChooser)

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

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonJobManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  authInboundMiddleware,
			Stream: authInboundMiddleware,
			Oneway: authInboundMiddleware,
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authOutboundMiddleware,
			Stream: authOutboundMiddleware,
			Oneway: authOutboundMiddleware,
		},
	})

	// Declare background works
	backgroundManager := background.NewManager()

	// Register UpdateActiveTasks function
	activeJobCache := activermtask.NewActiveRMTasks(dispatcher, rootScope)

	backgroundManager.RegisterWorks(
		background.Work{
			Name: "ActiveCacheJob",
			Func: func(_ *atomic.Bool) {
				activeJobCache.UpdateActiveTasks()
			},
			Period: time.Duration(cfg.JobManager.ActiveTaskUpdatePeriod),
		},
	)

	watchProcessor := watchsvc.InitV1AlphaWatchServiceHandler(
		dispatcher,
		rootScope,
		cfg.JobManager.Watch,
	)

	jobFactory := cached.InitJobFactory(
		store, // store implements JobStore
		store, // store implements TaskStore
		store, // store implements UpdateStore
		store, // store implements VolumeStore
		ormStore,
		rootScope,
		[]cached.JobTaskListener{watchsvc.NewWatchListener(watchProcessor)},
	)

	// TODO: We need to cleanup the client names
	launcher.InitTaskLauncher(
		dispatcher,
		common.PelotonHostManager,
		jobFactory,
		store, // store implements TaskStore
		store, // store implements VolumeStore
		ormStore,
		rootScope,
	)

	goalStateDriver := goalstate.NewDriver(
		dispatcher,
		store, // store implements JobStore
		store, // store implements TaskStore
		store, // store implements VolumeStore
		store, // store implements UpdateStore
		ormStore,
		jobFactory,
		launcher.GetLauncher(),
		job.JobType(job.JobType_value[*jobType]),
		rootScope,
		cfg.JobManager.GoalState,
		cfg.JobManager.JobRuntimeCalculationViaCache,
	)

	// Init placement processor
	placementProcessor := placement.InitProcessor(
		dispatcher,
		common.PelotonResourceManager,
		jobFactory,
		goalStateDriver,
		launcher.GetLauncher(),
		&cfg.JobManager.Placement,
		rootScope,
	)

	// Create a new task preemptor
	taskPreemptor := preemptor.New(
		dispatcher,
		common.PelotonResourceManager,
		store, // store implements TaskStore
		jobFactory,
		goalStateDriver,
		&cfg.JobManager.Preemptor,
		rootScope,
	)

	// Create a new Dead Line tracker for jobs
	deadlineTracker := deadline.New(
		dispatcher,
		store, // store implements JobStore
		store, // store implements TaskStore
		jobFactory,
		goalStateDriver,
		rootScope,
		&cfg.JobManager.Deadline,
	)

	// Create the Task status update which pulls task update events
	// from HM once started after gaining leadership
	statusUpdate := event.NewTaskStatusUpdate(
		dispatcher,
		store, // store implements JobStore
		store, // store implements TaskStore
		store, // store implements VolumeStore
		jobFactory,
		goalStateDriver,
		[]event.Listener{},
		rootScope,
	)

	server := jobmgr.NewServer(
		cfg.JobManager.HTTPPort,
		cfg.JobManager.GRPCPort,
		jobFactory,
		goalStateDriver,
		taskPreemptor,
		deadlineTracker,
		placementProcessor,
		statusUpdate,
		backgroundManager,
		watchProcessor,
	)

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.JobManagerRole,
		server,
	)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}

	jobsvc.InitServiceHandler(
		dispatcher,
		rootScope,
		store, // store implements JobStore
		store, // store implements TaskStore
		ormStore,
		jobFactory,
		goalStateDriver,
		candidate,
		common.PelotonResourceManager, // TODO: to be removed
		cfg.JobManager.JobSvcCfg,
	)

	stateless.InitV1AlphaJobServiceHandler(
		dispatcher,
		store,
		store,
		store,
		ormStore,
		jobFactory,
		goalStateDriver,
		candidate,
		cfg.JobManager.JobSvcCfg,
		activeJobCache,
	)

	tasksvc.InitServiceHandler(
		dispatcher,
		rootScope,
		store, // store implements JobStore
		store, // store implements TaskStore
		store, // store implements UpdateStore
		store, // store implements FrameworkInfoStore
		jobFactory,
		goalStateDriver,
		candidate,
		*mesosAgentWorkDir,
		common.PelotonHostManager,
		logmanager.NewLogManager(&http.Client{Timeout: _httpClientTimeout}),
		activeJobCache,
	)

	podsvc.InitV1AlphaPodServiceHandler(
		dispatcher,
		store,
		store,
		store,
		jobFactory,
		goalStateDriver,
		candidate,
		logmanager.NewLogManager(&http.Client{Timeout: _httpClientTimeout}),
		*mesosAgentWorkDir,
		hostsvc.NewInternalHostServiceYARPCClient(dispatcher.ClientConfig(common.PelotonHostManager)),
	)

	volumesvc.InitServiceHandler(
		dispatcher,
		rootScope,
		store, // store implements JobStore
		store, // store implements TaskStore
		store, // store implements VolumeStore
	)

	updatesvc.InitServiceHandler(
		dispatcher,
		rootScope,
		store, // store implements JobStore
		store, // store implements UpdateStore
		goalStateDriver,
		jobFactory,
	)

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
		"httpPort": cfg.JobManager.HTTPPort,
		"grpcPort": cfg.JobManager.GRPCPort,
	}).Info("Started job manager")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	// start collecting runtime metrics
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	select {}
}
