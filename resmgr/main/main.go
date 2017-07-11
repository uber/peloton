package main

import (
	"fmt"
	"net/url"
	"os"
	"runtime"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/health"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/resmgr"
	"code.uber.internal/infra/peloton/resmgr/entitlement"
	"code.uber.internal/infra/peloton/resmgr/respool"
	"code.uber.internal/infra/peloton/resmgr/task"
	"code.uber.internal/infra/peloton/storage/stores"
	"code.uber.internal/infra/peloton/yarpc/peer"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"
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

	resmgrPort = app.Flag(
		"port", "Resource manager port (resmgr.port override) (set $PORT to override)").
		Envar("PORT").
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
	// After go 1.5 the GOMAXPROCS is default to # of CPUs
	// As we need to do quite some DB writes, set the GOMAXPROCS to
	// 2 * NumCPUs
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

	log.WithField("files", *cfgFiles).Info("Loading Resource Manager config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	// now, override any CLI flags in the loaded config.Config
	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if *resmgrPort != 0 {
		cfg.ResManager.Port = *resmgrPort
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

	log.WithField("config", cfg).Info("Loaded Resource Manager config")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonResourceManager,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()
	rootScope.Counter("boot").Inc(1)

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	jobStore, taskStore, _, respoolStore, _, _ := stores.CreateStores(&cfg.Storage, rootScope)

	t := http.NewTransport()

	// NOTE: we "mount" the YARPC endpoints under /yarpc, so we can
	// mux in other HTTP handlers
	inbounds := []transport.Inbound{
		t.NewInbound(
			fmt.Sprintf(":%d", cfg.ResManager.Port),
			http.Mux(common.PelotonEndpointPath, mux),
		),
	}

	// all leader discovery metrics share a scope (and will be tagged
	// with role={role})
	discoveryScope := rootScope.SubScope("discovery")
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
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonResourceManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
	})

	// Initialize service handlers
	respool.InitServiceHandler(dispatcher, rootScope, respoolStore, jobStore, taskStore)

	// Initializing the resmgr state machine
	task.InitTaskTracker(rootScope)

	task.InitScheduler(rootScope, cfg.ResManager.TaskSchedulingPeriod,
		task.GetTracker())

	entitlement.InitCalculator(
		dispatcher,
		cfg.ResManager.EntitlementCaculationPeriod,
	)

	serviceHandler := resmgr.InitServiceHandler(dispatcher, rootScope, task.GetTracker())

	resmgr.InitRecovery(rootScope, jobStore, taskStore, serviceHandler)

	server := resmgr.NewServer(rootScope, cfg.ResManager.Port)
	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.ResourceManagerRole,
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
	log.Infof("Started Resource Manager on port %v", cfg.ResManager.Port)

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health)

	select {}
}
