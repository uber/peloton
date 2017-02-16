package main

import (
	"fmt"
	"os"
	"runtime"

	"code.uber.internal/infra/peloton/common"
	common_metrics "code.uber.internal/infra/peloton/common/metrics"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/resmgr"
	"code.uber.internal/infra/peloton/resmgr/config"
	res "code.uber.internal/infra/peloton/resmgr/respool"
	tq "code.uber.internal/infra/peloton/resmgr/taskqueue"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/peer"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("peloton-resmgr", "Peloton Resource Manager")
	debug   = app.Flag("debug", "enable debug mode (print full json responses)").Short('d').Default("false").Bool()
	configs = app.Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").Short('c').Required().ExistingFiles()
	env     = app.Flag("env", "environment (development will do no mesos master auto discovery) (set $PELOTON_ENVIRONMENT to override)").Short('e').Default("development").
		Envar("PELOTON_ENVIRONMENT").Enum("development", "production")
	logFormatJSON     = app.Flag("log-json", "Log in JSON format").Default("true").Bool()
	dbHost            = app.Flag("db-host", "Database host (db.host override) (set $DB_HOST to override)").Envar("DB_HOST").String()
	electionZkServers = app.Flag("election-zk-server", "Election Zookeeper servers. Specify multiple times for multiple servers (election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
				Envar("ELECTION_ZK_SERVERS").Strings()
	resmgrPort = app.Flag("resmgr-port", "Master port (resmgr.port override) (set $RESMGR_PORT to override)").Envar("RESMGR_PORT").Int()
)

func main() {
	// After go 1.5 the GOMAXPROCS is default to # of CPUs
	// As we need to do quite some DB writes, set the GOMAXPROCS to
	// 2 * NumCPUs
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	if *logFormatJSON {
		log.SetFormatter(&log.JSONFormatter{})
	}
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	log.Debugf("Loading config from %v...", *configs)
	cfg, err := config.New(*configs...)
	if err != nil {
		log.Fatalf("Error initializing configuration: %v", err)
	}

	// now, override any CLI flags in the loaded config.Config
	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if *resmgrPort != 0 {
		cfg.ResMgr.Port = *resmgrPort
	}
	log.WithField("config", cfg).Debug("Loaded Peloton Resource Manager configuration")

	rootScope, scopeCloser, mux := common_metrics.InitMetricScope(&cfg.Metrics, common.PelotonResourceManager, common_metrics.TallyFlushInterval)
	defer scopeCloser.Close()
	rootScope.Counter("boot").Inc(1)

	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// Migrate DB if necessary
	if errs := cfg.Storage.MySQL.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}

	// Initialize resmgr store
	resstore := mysql.NewResourcePoolStore(cfg.Storage.MySQL.Conn, rootScope.SubScope("storage"))
	resstore.DB.SetMaxOpenConns(cfg.ResMgr.DbWriteConcurrency)
	resstore.DB.SetMaxIdleConns(cfg.ResMgr.DbWriteConcurrency)
	resstore.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	// Initialize job and task stores
	store := mysql.NewJobStore(cfg.Storage.MySQL, rootScope.SubScope("storage"))
	store.DB.SetMaxOpenConns(cfg.ResMgr.DbWriteConcurrency)
	store.DB.SetMaxIdleConns(cfg.ResMgr.DbWriteConcurrency)
	store.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	// NOTE: we "mount" the YARPC endpoints under /yarpc, so we can mux in other HTTP handlers
	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.ResMgr.Port), http.Mux(common.PelotonEndpointURL, mux)),
	}

	resmgrPeerChooser := peer.NewPeerChooser(common.ResourceManagerRole)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     common.PelotonResourceManager,
		Inbounds: inbounds,
	})

	// Initalize managers
	metrics := metrics.New(rootScope.SubScope("resource_manager"))

	rm := res.InitServiceHandler(dispatcher, cfg, resstore, &metrics)
	taskqueue := tq.InitTaskQueue(dispatcher, &metrics, store, store)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton resource manager on port %v", cfg.ResMgr.Port)

	// This is the address of the local resource manager address to be announced via leader election
	ip, err := util.ListenIP()
	if err != nil {
		log.Fatalf("Failed to get ip, err=%v", err)
	}
	localPelotonRMAddr := fmt.Sprintf("http://%s:%d", ip, cfg.ResMgr.Port)
	resMgrLeader := resmgr.NewServer(*env, resmgrPeerChooser, cfg, localPelotonRMAddr,
		*rm, taskqueue)
	leadercandidate, err := leader.NewCandidate(cfg.Election, rootScope.SubScope("election"), common.ResourceManagerRole, resMgrLeader)
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Fatal("Unable to create leader candidate")
	}
	err = leadercandidate.Start()
	if err != nil {
		log.WithFields(log.Fields{"error": err}).Fatal("Unable to start leader candidate")
	}
	defer leadercandidate.Stop()

	select {}
}
