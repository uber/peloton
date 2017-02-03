package main

import (
	"fmt"
	nethttp "net/http"
	"os"
	"runtime"
	"time"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/resmgr"
	"code.uber.internal/infra/peloton/resmgr/config"
	resleader "code.uber.internal/infra/peloton/resmgr/election"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/peer"
	log "github.com/Sirupsen/logrus"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallyprom "github.com/uber-go/tally/prometheus"
	tallystatsd "github.com/uber-go/tally/statsd"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
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

	var reporter tally.StatsReporter
	var promHandler nethttp.Handler
	metricSeparator := "."
	if cfg.Metrics.Prometheus != nil && cfg.Metrics.Prometheus.Enable {
		metricSeparator = "_"
		promreporter := tallyprom.NewReporter(nil)
		reporter = promreporter
		promHandler = promreporter.HTTPHandler()
	} else if cfg.Metrics.Statsd != nil && cfg.Metrics.Statsd.Enable {
		log.Infof("Metrics configured with statsd endpoint %s", cfg.Metrics.Statsd.Endpoint)
		c, err := statsd.NewClient(cfg.Metrics.Statsd.Endpoint, "")
		if err != nil {
			log.Fatalf("Unable to setup Statsd client: %v", err)
		}
		reporter = tallystatsd.NewReporter(c, tallystatsd.NewOptions())
	} else {
		log.Warnf("No metrics backends configured, using the statsd.NoopClient")
		c, _ := statsd.NewNoopClient()
		reporter = tallystatsd.NewReporter(c, tallystatsd.NewOptions())
	}
	metricScope, scopeCloser := tally.NewRootScope("peloton_framework", map[string]string{}, reporter, metricFlushInterval, metricSeparator)
	defer scopeCloser.Close()
	metricScope.Counter("boot").Inc(1)

	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// Migrate DB if necessary
	if errs := cfg.Storage.MySQL.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}

	// Initialize resmgr store
	resstore := mysql.NewResourcePoolStore(cfg.Storage.MySQL.Conn, metricScope.SubScope("storage"))
	resstore.DB.SetMaxOpenConns(cfg.ResMgr.DbWriteConcurrency)
	resstore.DB.SetMaxIdleConns(cfg.ResMgr.DbWriteConcurrency)
	resstore.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()
	if promHandler != nil {
		// if prometheus support is enabled, handle /metrics to serve prom metrics
		log.Infof("Setting up prometheus metrics handler at /metrics")
		mux.Handle("/metrics", promHandler)
	}
	// expose a /health endpoint that just returns 200
	mux.HandleFunc("/health", func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		// TODO: make this healthcheck live, and check some kind of internal health?
		if true {
			w.WriteHeader(nethttp.StatusOK)
			fmt.Fprintln(w, `\(★ω★)/`)
		} else {
			w.WriteHeader(nethttp.StatusInternalServerError)
			fmt.Fprintln(w, `(╥﹏╥)`)
		}
	})

	// NOTE: we "mount" the YARPC endpoints under /yarpc, so we can mux in other HTTP handlers
	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.ResMgr.Port), http.Mux(config.FrameworkURLPath, mux)),
	}

	resmgrPeerChooser := peer.NewPeerChooser()
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "peloton-resmgr",
		Inbounds: inbounds,
	})

	// Initalize managers
	metrics := metrics.New(metricScope.SubScope("resource-manager"))

	rm := resmgr.InitManager(dispatcher, cfg, resstore, &metrics)

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
	pLeader := resleader.NewElectionHandler(*env, resmgrPeerChooser, cfg, localPelotonRMAddr,
		rm)
	leader.NewZkElection(cfg.Election, localPelotonRMAddr, pLeader)

	select {}
}
