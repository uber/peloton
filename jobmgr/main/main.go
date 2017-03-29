package main

import (
	"fmt"
	"os"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"

	"net/url"
	"runtime"

	"code.uber.internal/infra/peloton/common/config"
	log "github.com/Sirupsen/logrus"
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
		cfg.JobManager.PlacementDequeueLimit = *placementDequeLimit
	}

	if *getPlacementsTimeout != 0 {
		cfg.JobManager.GetPlacementsTimeout = *getPlacementsTimeout
	}

	log.WithField("config", cfg).Info("Loaded Job Manager configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonJobManager,
		metrics.TallyFlushInterval)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// TODO: fix metric scope
	store := mysql.NewJobStore(cfg.Storage.MySQL, rootScope.SubScope("storage"))
	store.DB.SetMaxOpenConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetMaxIdleConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	inbounds := []transport.Inbound{
		http.NewInbound(
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
	if err := resmgrPeerChooser.Start(); err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not start smart peer chooser")
	}
	defer resmgrPeerChooser.Stop()
	resmgrOutbound := http.NewChooserOutbound(
		resmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointPath,
		})

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
	if err := hostmgrPeerChooser.Start(); err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not start smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()
	hostmgrOutbound := http.NewChooserOutbound(
		hostmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointPath,
		})

	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		// Temp fix for T730605 so that we don't have to change
		// peloton client, we should change it back to
		// common.PelotonJobManager when master is deprecated
		Name:      common.PelotonMaster,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Init service handler.
	// TODO: change to updated jobmgr.Config
	job.InitServiceHandler(
		dispatcher,
		rootScope,
		store,
		store,
		common.PelotonResourceManager, // TODO: to be removed
	)
	task.InitServiceHandler(
		dispatcher,
		rootScope,
		store,
		store,
		common.PelotonHostManager, // TODO: to be removed
	)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	// Init the Task status update which pulls task update events from HM
	task.InitTaskStatusUpdate(
		dispatcher,
		common.PelotonHostManager,
		store,
		rootScope,
	)

	// TODO: We need to cleanup the client names
	task.InitTaskLauncher(
		dispatcher,
		common.PelotonResourceManager,
		common.PelotonHostManager,
		store,
		&cfg.JobManager,
		rootScope,
	)

	task.GetLauncher().Start()
	defer task.GetLauncher().Stop()
	log.WithField("Port ", cfg.JobManager.Port).Info(" Started Peloton job manager on port ")

	select {}
}
