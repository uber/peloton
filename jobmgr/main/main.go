package main

import (
	"fmt"
	"net/url"
	"os"
	"runtime"

	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/jobsvc"
	"code.uber.internal/infra/peloton/jobmgr/task/event"
	"code.uber.internal/infra/peloton/jobmgr/task/launcher"
	"code.uber.internal/infra/peloton/jobmgr/tasksvc"
	"code.uber.internal/infra/peloton/jobmgr/upgrade"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/storage/stores"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"

	log "github.com/sirupsen/logrus"
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

	jobmgrPort = app.Flag(
		"port", "Job manager port (jobmgr.port override) (set $PORT to override)").
		Envar("PORT").
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
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)

	log.WithField("files", *cfgFiles).Info("Loading job manager config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	logging.ConfigureSentry(&cfg.SentryConfig)

	// now, override any CLI flags in the loaded config.Config
	if *jobmgrPort != 0 {
		cfg.JobManager.Port = *jobmgrPort
	}

	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if *placementDequeLimit != 0 {
		cfg.JobManager.TaskLauncher.PlacementDequeueLimit = *placementDequeLimit
	}

	if *getPlacementsTimeout != 0 {
		cfg.JobManager.TaskLauncher.GetPlacementsTimeout = *getPlacementsTimeout
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

	log.WithField("config", cfg).Info("Loaded Job Manager configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonJobManager,
		metrics.TallyFlushInterval)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	jobStore, taskStore, upgradeStore, _, _, volumeStore := stores.CreateStores(&cfg.Storage, rootScope)

	t := http.NewTransport()

	inbounds := []transport.Inbound{
		t.NewInbound(
			fmt.Sprintf(":%d", cfg.JobManager.Port),
			http.Mux(common.PelotonEndpointPath, mux),
		),
	}

	// all leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")
	// setup the discovery service to detect resmgr leaders and
	// configure the YARPC Peer dynamically
	resmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.ResourceManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer resmgrPeerChooser.Stop()

	resmgrOutbound := t.NewOutbound(
		resmgrPeerChooser,
		http.URLTemplate(
			(&url.URL{
				Scheme: "http",
				Path:   common.PelotonEndpointPath,
			}).String()))

	// setup the discovery service to detect hostmgr leaders and
	// configure the YARPC Peer dynamically
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.HostManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()

	hostmgrOutbound := t.NewOutbound(
		hostmgrPeerChooser,
		http.URLTemplate(
			(&url.URL{
				Scheme: "http",
				Path:   common.PelotonEndpointPath,
			}).String()))

	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonJobManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})

	// Init service handler.
	// TODO: change to updated jobmgr.Config
	runtimeUpdater := job.NewJobRuntimeUpdater(
		jobStore,
		taskStore,
		resmgrsvc.NewResourceManagerServiceYarpcClient(
			dispatcher.ClientConfig(common.PelotonResourceManager)),
		rootScope)
	jobsvc.InitServiceHandler(
		dispatcher,
		rootScope,
		jobStore,
		taskStore,
		runtimeUpdater,
		common.PelotonResourceManager, // TODO: to be removed
	)
	tasksvc.InitServiceHandler(
		dispatcher,
		rootScope,
		jobStore,
		taskStore,
		runtimeUpdater,
	)
	upgrade.InitServiceHandler(dispatcher, jobStore, upgradeStore)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	// Init the Task status update which pulls task update events
	// from HM once started after gaining leadership
	event.InitTaskStatusUpdate(
		dispatcher,
		common.PelotonHostManager,
		jobStore,
		taskStore,
		runtimeUpdater,
		common.PelotonResourceManager,
		rootScope,
	)

	// Init the Task status update which pulls task update events
	// from HM once started after gaining leadership
	event.InitTaskStatusUpdateRM(
		dispatcher,
		common.PelotonHostManager,
		jobStore,
		taskStore,
		runtimeUpdater,
		common.PelotonResourceManager,
		rootScope,
	)

	server := jobmgr.NewServer(
		cfg.JobManager.Port,
	)

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope.SubScope("election"),
		common.JobManagerRole,
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

	// TODO: We need to cleanup the client names
	launcher.InitTaskLauncher(
		dispatcher,
		common.PelotonResourceManager,
		common.PelotonHostManager,
		taskStore,
		volumeStore,
		&cfg.JobManager.TaskLauncher,
		rootScope,
	)
	launcher.GetLauncher().Start()
	defer launcher.GetLauncher().Stop()

	log.WithField("Port", cfg.JobManager.Port).Info("Started Peloton job manager")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health)

	select {}
}
