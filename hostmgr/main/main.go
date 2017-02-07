package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"

	log "github.com/Sirupsen/logrus"
)

const (
	productionEnvValue = "production"
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
	rootMetricScope     = "peloton_hostmgr"
)

var (
	version string
	app     = kingpin.New("peloton-hostmgr", "Peloton Host Manager")

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

	env = app.
		Flag("env", "environment (development will do no mesos master auto discovery) (set $PELOTON_ENVIRONMENT to override)").
		Short('e').
		Default("development").
		Envar("PELOTON_ENVIRONMENT").
		Enum("development", "production")
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)

	var cfg hostmgr.Config

	if err := config.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse host manager config")
	}

	rootScope, scopeCloser, mux := metrics.InitMetricScope(&cfg.Metrics, common.PelotonHostManager, metricFlushInterval)
	defer scopeCloser.Close()

	// NOTE: we "mount" the YARPC endpoints under /yarpc, so we can mux in other HTTP handlers
	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.HostManager.Port), http.Mux(common.PelotonEndpointURL, mux)),
	}

	// TODO(zhitao): Confirm this code is working when mesos leader changes.
	mesosMasterDetector, err := mesos.NewZKDetector(cfg.Mesos.ZkPath)
	if err != nil {
		log.Fatalf("Failed to initialize mesos master detector: %v", err)
	}

	mesosMasterLocation, err := mesosMasterDetector.GetMasterLocation()
	if err != nil {
		log.Fatalf("Failed to get mesos leading master location, err=%v", err)
	}
	log.Infof("Detected Mesos leading master location: %s", mesosMasterLocation)

	rootScope.Counter("boot").Inc(1)

	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}

	// TODO(wu): Remove this once `make pcluster` is capable to auto migrate the database.
	// Migrate DB if necessary
	if errs := cfg.Storage.MySQL.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}

	store := mysql.NewJobStore(cfg.Storage.MySQL, rootScope.SubScope("storage"))

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos, store)

	// Active host manager needs a Mesos inbound
	var mInbound = mhttp.NewInbound(driver)
	inbounds = append(inbounds, mInbound)

	// TODO: update mesos url when leading mesos master changes
	mesosURL := fmt.Sprintf("http://%s%s", mesosMasterLocation, driver.Endpoint())
	mOutbound := mhttp.NewOutbound(mesosURL)

	outbounds := yarpc.Outbounds{
		common.MesosMaster: mOutbound,
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonHostManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Init service handler.
	hostmgr.InitServiceHandler(dispatcher)

	log.WithFields(log.Fields{
		"port":     cfg.HostManager.Port,
		"url_path": common.PelotonEndpointURL,
	}).Info("HostService initialized")

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// mesos callbacks
	mesosClient := mpb.New(dispatcher.ClientConfig("mesos-master"), cfg.Mesos.Encoding)
	mesos.InitManager(dispatcher, &cfg.Mesos, store)

	offerManager := offer.InitManager(
		dispatcher,
		time.Duration(cfg.HostManager.OfferHoldTimeSec)*time.Second,
		time.Duration(cfg.HostManager.OfferPruningPeriodSec)*time.Second,
		mesosClient)

	// Initializing TaskStateManager will start to record task status update back to storage.
	// TODO(zhitao): This is temporary. Eventually we should create proper API protocol for
	// `WaitTaskStatusUpdate` and allow RM/JM to retrieve this separately.
	task.InitTaskStateManager(
		dispatcher,
		cfg.HostManager.TaskUpdateBufferSize,
		cfg.HostManager.TaskUpdateAckConcurrency,
		cfg.HostManager.DbWriteConcurrency,
		store,
		store)

	// This is the address of the local server address to be announced via leader election
	ip, err := util.ListenIP()
	if err != nil {
		log.Fatalf("Failed to get ip, err=%v", err)
	}

	localAddr := fmt.Sprintf("http://%s:%d", ip, cfg.HostManager.Port)

	server := hostmgr.NewServer(
		&cfg,
		mesosMasterDetector,
		mInbound,
		mOutbound,
		offerManager,
		localAddr)

	leader.NewZkElection(cfg.Election, localAddr, server)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	select {}
}
