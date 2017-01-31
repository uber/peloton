package main

import (
	"fmt"
	nethttp "net/http"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/config"
	"code.uber.internal/infra/peloton/master/job"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/master/offer"
	"code.uber.internal/infra/peloton/master/resmgr"
	"code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/master/upgrade"
	"code.uber.internal/infra/peloton/scheduler"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
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
	productionEnvValue = "production"
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
)

var (
	version string
	app     = kingpin.New("peloton-framework", "Peloton Mesos Framework")
	debug   = app.Flag("debug", "enable debug mode (print full json responses)").Short('d').Default("false").Bool()
	configs = app.Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").Short('c').Required().ExistingFiles()
	env     = app.Flag("env", "environment (development will do no mesos master auto discovery) (set $PELOTON_ENVIRONMENT to override)").Short('e').Default("development").
		Envar("PELOTON_ENVIRONMENT").Enum("development", "production")
	logFormatJSON    = app.Flag("log-json", "Log in JSON format").Default("true").Bool()
	zkPath           = app.Flag("zk-path", "Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").Envar("MESOS_ZK_PATH").String()
	dbHost           = app.Flag("db-host", "Database host (db.host override) (set $DB_HOST to override)").Envar("DB_HOST").String()
	taskDequeueLimit = app.Flag("task-dequeue-limit", "Scheduler task dequeue limit (scheduler.task_dequeue_limit override) (set $SCHEDULER_TASK_DEQUEUE_LIMIT to override)").
				Envar("SCHEDULER_TASK_DEQUEUE_LIMIT").Int()
	electionZkServers = app.Flag("election-zk-server", "Election Zookeeper servers. Specify multiple times for multiple servers (election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
				Envar("ELECTION_ZK_SERVERS").Strings()
	masterPort    = app.Flag("master-port", "Master port (master.port override) (set $MASTER_PORT to override)").Envar("MASTER_PORT").Int()
	offerHoldTime = app.Flag("offer-hold", "Master offer time (master.offer_hold_time_sec override) (set $OFFER_HOLD_TIME to override)").
			HintOptions("5s", "1m").Envar("OFFER_HOLD_TIME").Duration()
	offerPruningPeriod = app.Flag("offer-pruning-period", "Master offer pruning period (master.offer_pruning_period_sec override) (set $OFFER_PRUNING_PERIOD to override)").
				HintOptions("20s").Envar("OFFER_PRUNING_PERIOD").Duration()
)

type pelotonMaster struct {
	mesosInbound  mhttp.Inbound
	peerChooser   *peer.Chooser
	mesosOutbound transport.Outbounds
	taskQueue     *task.Queue
	store         *mysql.JobStore
	cfg           *config.Config
	mutex         *sync.Mutex
	// Local address for peloton master
	localAddr       string
	mesosDetector   mesos.MasterDetector
	offerManager    *offer.Manager
	env             string
	resourceManager *resmgr.ResourceManager
}

func newPelotonMaster(env string,
	mInbound mhttp.Inbound,
	mOutbounds transport.Outbounds,
	pChooser *peer.Chooser,
	tq *task.Queue,
	store *mysql.JobStore,
	cfg *config.Config,
	localPelotonMasterAddr string,
	mesosDetector mesos.MasterDetector,
	om *offer.Manager,
	rm *resmgr.ResourceManager) *pelotonMaster {
	result := pelotonMaster{
		env:             env,
		mesosInbound:    mInbound,
		peerChooser:     pChooser,
		mesosOutbound:   mOutbounds,
		taskQueue:       tq,
		store:           store,
		cfg:             cfg,
		mutex:           &sync.Mutex{},
		localAddr:       localPelotonMasterAddr,
		mesosDetector:   mesosDetector,
		offerManager:    om,
		resourceManager: rm,
	}
	return &result
}

// GainedLeadershipCallBack is the callback when the current node becomes the leader
func (p *pelotonMaster) GainedLeadershipCallBack() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Gained leadership")

	// Gained leadership, register with mesos, then refill task queue if needed
	var err error
	mesosMasterAddr := p.cfg.Mesos.HostPort
	// Not using zkDetector for development for now because on Mac, mesos
	// containers are launched in bridged network, therefore master
	// registers with docker internal ip to zk, which can not be
	// accessed by peloton running outside the container.
	if p.env == productionEnvValue {
		mesosMasterAddr, err = p.mesosDetector.GetMasterLocation()
		if err != nil {
			log.Errorf("Failed to get mesosMasterAddr, err = %v", err)
			return err
		}
	}

	err = p.mesosInbound.StartMesosLoop(mesosMasterAddr)
	if err != nil {
		log.Errorf("Failed to StartMesosLoop, err = %v", err)
		return err
	}
	err = p.taskQueue.LoadFromDB(p.store, p.store)
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
		return err
	}

	err = p.peerChooser.UpdatePeer(p.localAddr)
	if err != nil {
		log.Errorf("Failed to update peer with p.localPelotonMasterAddr, err = %v", err)
		return err
	}
	p.offerManager.Start()
	p.resourceManager.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (p *pelotonMaster) LostLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Lost leadership")
	err := p.mesosInbound.Stop()
	p.taskQueue.Reset()
	if err != nil {
		log.Errorf("Failed to stop mesos inbound, err = %v", err)
	}

	p.offerManager.Stop()
	p.resourceManager.Stop()

	return err
}

// NewLeaderCallBack is the callback when some other node becomes the leader, leader is hostname of the leader
func (p *pelotonMaster) NewLeaderCallBack(leader string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	log.Infof("New Leader is elected : %v", leader)
	// leader changes, so point pelotonMasterOutbound to the new leader
	return p.peerChooser.UpdatePeer(leader)
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (p *pelotonMaster) ShutDownCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Quiting the election")
	return nil
}

// GetHostPort function returns the peloton master address
func (p *pelotonMaster) GetHostPort() string {
	return p.localAddr
}

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
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}
	if *dbHost != "" {
		cfg.StorageConfig.MySQLConfig.Host = *dbHost
	}
	if *taskDequeueLimit != 0 {
		cfg.Scheduler.TaskDequeueLimit = *taskDequeueLimit
	}
	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}
	if *masterPort != 0 {
		cfg.Master.Port = *masterPort
	}
	if *offerHoldTime != 0 {
		cfg.Master.OfferHoldTimeSec = int(offerHoldTime.Seconds())
	}
	if *offerPruningPeriod != 0 {
		cfg.Master.OfferPruningPeriodSec = int(offerPruningPeriod.Seconds())
	}

	log.WithField("config", cfg).Debug("Loaded Peloton configuration")

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
	if err := cfg.StorageConfig.MySQLConfig.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// Migrate DB if necessary
	if errs := cfg.StorageConfig.MySQLConfig.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}
	// Initialize job and task stores
	store := mysql.NewJobStore(cfg.StorageConfig.MySQLConfig, metricScope.SubScope("storage"))
	store.DB.SetMaxOpenConns(cfg.Master.DbWriteConcurrency)
	store.DB.SetMaxIdleConns(cfg.Master.DbWriteConcurrency)
	store.DB.SetConnMaxLifetime(cfg.StorageConfig.MySQLConfig.ConnLifeTime)

	resstore := mysql.NewResourcePoolStore(cfg.StorageConfig.MySQLConfig.Conn, metricScope.SubScope("storage"))
	resstore.DB.SetMaxOpenConns(cfg.Master.DbWriteConcurrency)
	resstore.DB.SetMaxIdleConns(cfg.Master.DbWriteConcurrency)
	resstore.DB.SetConnMaxLifetime(cfg.StorageConfig.MySQLConfig.ConnLifeTime)

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos, store)

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
		http.NewInbound(fmt.Sprintf(":%d", cfg.Master.Port), http.Mux(config.FrameworkURLPath, mux)),
	}

	mesosMasterLocation := cfg.Mesos.HostPort
	mesosMasterDetector, err := mesos.NewZKDetector(cfg.Mesos.ZkPath)
	if err != nil {
		log.Fatalf("Failed to initialize mesos master detector: %v", err)
	}
	// Not doing this for development for now because on Mac, mesos
	// containers are launched in bridged network, therefore master
	// registers with docker internal ip to zk, which can not be
	// accessed by peloton running outside the container.
	if *env == productionEnvValue {
		mesosMasterLocation, err = mesosMasterDetector.GetMasterLocation()
		if err != nil {
			log.Fatalf("Failed to get mesos leading master location, err=%v", err)
		}
		log.Infof("Detected Mesos leading master location: %s", mesosMasterLocation)
	}

	// Each master needs a Mesos inbound
	var mInbound = mhttp.NewInbound(driver)
	inbounds = append(inbounds, mInbound)

	// TODO: update mesos url when leading mesos master changes
	mesosURL := fmt.Sprintf("http://%s%s", mesosMasterLocation, driver.Endpoint())

	mOutbounds := mhttp.NewOutbound(mesosURL)
	pelotonMasterPeerChooser := peer.NewPeerChooser()
	// The leaderUrl for pOutbound would be updated by leader election NewLeaderCallBack once leader is elected
	pOutbound := http.NewChooserOutbound(pelotonMasterPeerChooser, &url.URL{Scheme: "http", Path: config.FrameworkURLPath})
	pOutbounds := transport.Outbounds{
		Unary: pOutbound,
	}
	outbounds := yarpc.Outbounds{
		"mesos-master":   mOutbounds,
		"peloton-master": pOutbounds,
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-master",
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Initalize managers
	metrics := metrics.New(metricScope.SubScope("master"))
	job.InitManager(dispatcher, &cfg.Master, store, store, &metrics)
	task.InitManager(dispatcher, store, store, &metrics)
	tq := task.InitTaskQueue(dispatcher, &metrics)
	upgrade.InitManager(dispatcher)

	mesosClient := mpb.New(dispatcher.ClientConfig("mesos-master"), cfg.Mesos.Encoding)

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// mesos callbacks
	mesos.InitManager(dispatcher, &cfg.Mesos, store)
	om := offer.InitManager(dispatcher, time.Duration(cfg.Master.OfferHoldTimeSec)*time.Second,
		time.Duration(cfg.Master.OfferPruningPeriodSec)*time.Second,
		mesosClient)

	task.InitTaskStateManager(dispatcher, &cfg.Master, store, store)

	rm := resmgr.InitManager(dispatcher, &cfg.Master, resstore, &metrics)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton master on port %v", cfg.Master.Port)

	// This is the address of the local peloton master address to be announced via leader election
	ip, err := util.ListenIP()
	if err != nil {
		log.Fatalf("Failed to get ip, err=%v", err)
	}
	localPelotonMasterAddr := fmt.Sprintf("http://%s:%d", ip, cfg.Master.Port)
	pMaster := newPelotonMaster(*env, mInbound, mOutbounds, pelotonMasterPeerChooser, tq, store, cfg, localPelotonMasterAddr,
		mesosMasterDetector, om, rm)
	leader.NewZkElection(cfg.Election, localPelotonMasterAddr, pMaster)

	// Defer initializing scheduler till the end
	schedulerMetrics := scheduler.NewMetrics(metricScope.SubScope("scheduler"))
	scheduler.InitManager(dispatcher, &cfg.Scheduler, mesosClient, &schedulerMetrics)

	select {}
}
