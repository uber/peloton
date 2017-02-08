package main

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/placement"
	placementconfig "code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
)

var (
	version string
	app     = kingpin.New("peloton-placement", "Peloton Placement Engine")

	debug = app.
		Flag("debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Bool()

	configs = app.
		Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	zkPath = app.
		Flag("zk-path", "Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").
		Envar("MESOS_ZK_PATH").
		String()

	dbHost = app.
		Flag("db-host", "Database host (db.host override) (set $DB_HOST to override)").
		Envar("DB_HOST").
		String()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	var cfg placementconfig.Config

	if err := config.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse placement engine config")
	}

	// now, override any CLI flags in the loaded config.Config
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
	}

	rootScope, scopeCloser, _ := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metricFlushInterval,
	)
	defer scopeCloser.Close()

	rootScope.Counter("boot").Inc(1)

	// TODO: hook up with service discovery code to auto-detect the resmgr leader
	resmgrPeerChooser := peer.NewPeerChooser()
	resmgrAddr := fmt.Sprintf("http://%s:%d", cfg.Placement.ResmgrHost, cfg.Placement.ResmgrPort)
	resmgrPeerChooser.UpdatePeer(resmgrAddr, common.PelotonResourceManager)
	// TODO: change FrameworkURLPath to resource manager URL path
	resmgrOutbound := http.NewChooserOutbound(
		resmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointURL,
		},
	)

	// TODO: hookup with service discovery for hostmgr leader
	hostmgrPeerChooser := peer.NewPeerChooser()
	hostmgrAddr := fmt.Sprintf("http://%s:%d", cfg.Placement.HostmgrHost, cfg.Placement.HostmgrPort)
	hostmgrPeerChooser.UpdatePeer(hostmgrAddr, common.PelotonHostManager)
	hostmgrOutbound := http.NewChooserOutbound(
		hostmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointURL,
		},
	)

	// TODO: remove mesos dependency once we switch to hostMgr launch task api
	// Initialize job and task stores
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	store := mysql.NewJobStore(cfg.Storage.MySQL, rootScope.SubScope("storage"))

	// Initialize mesos driver and  detector
	mesos.InitSchedulerDriver(&cfg.Mesos, store)
	mesosMasterDetector, err := mesos.NewZKDetector(cfg.Mesos.ZkPath)
	if err != nil {
		log.Fatalf("Failed to initialize mesos master detector: %v", err)
	}
	mesosMasterLocation, err := mesosMasterDetector.GetMasterLocation()
	if err != nil {
		log.Fatalf("Failed to get mesos leading master location, err=%v", err)
	}
	mesosURL := fmt.Sprintf("http://%s%s", mesosMasterLocation, "/api/v1/scheduler")

	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
		// TODO: remove mOutbounds once we switch to hostMgr launch task api
		common.MesosMaster: mhttp.NewOutbound(mesosURL),
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonPlacement,
		Outbounds: outbounds,
	})

	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start dispatcher: %v", err)
	}

	// TODO: remove dependency on mesos once we switch to hostMgr launch task api
	mesosClient := mpb.New(dispatcher.ClientConfig("mesos-master"), "x-protobuf")

	placementMetrics := placement.NewMetrics(rootScope)
	// Initialize and start placement engine
	placement.InitServer(
		dispatcher,
		&cfg.Placement,
		mesosClient,
		&placementMetrics,
		common.PelotonResourceManager,
		common.PelotonHostManager,
	)

	select {}
}
