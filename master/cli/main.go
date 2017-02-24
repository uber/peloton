package main

import (
	"fmt"
	nethttp "net/http"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	peloton_common "code.uber.internal/infra/peloton/common"
	common_metrics "code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/hostmgr/offer"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/config"
	"code.uber.internal/infra/peloton/master/metrics"
	master_task "code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/master/upgrade"
	"code.uber.internal/infra/peloton/placement"
	"code.uber.internal/infra/peloton/resmgr/respool"
	taskq "code.uber.internal/infra/peloton/resmgr/taskqueue"
	"code.uber.internal/infra/peloton/util"
	"code.uber.internal/infra/peloton/yarpc/encoding/mpb"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"

	"code.uber.internal/infra/peloton/storage"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/storage/stapi"
	log "github.com/Sirupsen/logrus"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallyprom "github.com/uber-go/tally/prometheus"
	tallystatsd "github.com/uber-go/tally/statsd"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

var (
	version string
	app     = kingpin.New("peloton-framework", "Peloton Mesos Framework")

	debug = app.Flag("debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	configs = app.Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").Short('c').Required().ExistingFiles()
	env     = app.Flag("env", "environment (development will do no mesos master auto discovery) (set $PELOTON_ENVIRONMENT to override)").Short('e').Default("development").
		Envar("PELOTON_ENVIRONMENT").Enum("development", "production")
	logFormatJSON    = app.Flag("log-json", "Log in JSON format").Default("true").Bool()
	zkPath           = app.Flag("zk-path", "Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").Envar("MESOS_ZK_PATH").String()
	dbHost           = app.Flag("db-host", "Database host (db.host override) (set $DB_HOST to override)").Envar("DB_HOST").String()
	taskDequeueLimit = app.Flag("task-dequeue-limit", "Placement Engine task dequeue limit (placement.task_dequeue_limit override) (set $PLACEMENT_TASK_DEQUEUE_LIMIT to override)").
				Envar("PLACEMENT_TASK_DEQUEUE_LIMIT").Int()
	electionZkServers = app.Flag("election-zk-server", "Election Zookeeper servers. Specify multiple times for multiple servers (election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
				Envar("ELECTION_ZK_SERVERS").Strings()
	port          = app.Flag("port", "Master port (master.port override) (set $PORT to override)").Envar("PORT").Int()
	offerHoldTime = app.Flag("offer-hold", "Master offer time (master.offer_hold_time_sec override) (set $OFFER_HOLD_TIME to override)").
			HintOptions("5s", "1m").Envar("OFFER_HOLD_TIME").Duration()
	offerPruningPeriod = app.Flag("offer-pruning-period", "Master offer pruning period (master.offer_pruning_period_sec override) (set $OFFER_PRUNING_PERIOD to override)").
				HintOptions("20s").Envar("OFFER_PRUNING_PERIOD").Duration()
	useSTAPI       = app.Flag("use-stapi", "Use STAPI storage implementation").Default("false").Envar("USE_STAPI").Bool()
	cassandraHosts = app.Flag("cassandra-hosts", "Cassandra hosts for STAPI").Envar("CASSANDRA_HOSTS").Strings()
)

type pelotonMaster struct {
	mesosInbound  mhttp.Inbound
	mesosOutbound transport.Outbounds
	taskQueue     *taskq.Queue
	cfg           *config.Config
	mutex         *sync.Mutex
	// Local address for peloton master
	localAddr      string
	mesosDetector  mesos.MasterDetector
	offerManager   *offer.Manager
	respoolService *respool.ServiceHandler
	env            string
}

func newPelotonMaster(env string,
	mInbound mhttp.Inbound,
	mOutbounds transport.Outbounds,
	tq *taskq.Queue,
	cfg *config.Config,
	localPelotonMasterAddr string,
	mesosDetector mesos.MasterDetector,
	om *offer.Manager,
	rm *respool.ServiceHandler) *pelotonMaster {
	result := pelotonMaster{
		env:            env,
		mesosInbound:   mInbound,
		mesosOutbound:  mOutbounds,
		taskQueue:      tq,
		cfg:            cfg,
		mutex:          &sync.Mutex{},
		localAddr:      localPelotonMasterAddr,
		mesosDetector:  mesosDetector,
		offerManager:   om,
		respoolService: rm,
	}
	return &result
}

// GainedLeadershipCallback is the callback when the current node becomes the leader
func (p *pelotonMaster) GainedLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.WithFields(log.Fields{"role": peloton_common.MasterRole}).Info("Gained leadership")

	// Gained leadership, register with mesos, then refill task queue if needed
	mesosMasterAddr, err := p.mesosDetector.GetMasterLocation()
	if err != nil {
		log.Errorf("Failed to get mesosMasterAddr, err = %v", err)
		return err
	}

	err = p.mesosInbound.StartMesosLoop(mesosMasterAddr)
	if err != nil {
		log.Errorf("Failed to StartMesosLoop, err = %v", err)
		return err
	}
	err = p.taskQueue.LoadFromDB()
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
		return err
	}

	p.offerManager.Start()

	p.respoolService.Start()

	return nil
}

// LostLeadershipCallback is the callback when the current node lost leadership
func (p *pelotonMaster) LostLeadershipCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.WithFields(log.Fields{"role": peloton_common.MasterRole}).Info("Lost leadership")
	err := p.mesosInbound.Stop()
	p.taskQueue.Reset()
	if err != nil {
		log.Errorf("Failed to stop mesos inbound, err = %v", err)
	}

	p.offerManager.Stop()

	p.respoolService.Stop()

	return err
}

// ShutDownCallback is the callback to shut down gracefully if possible
func (p *pelotonMaster) ShutDownCallback() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	log.Infof("Quiting the election")
	return nil
}

// GetID function returns the peloton master address
// required to implement leader.Nomination
func (p *pelotonMaster) GetID() string {
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
		cfg.Storage.MySQL.Host = *dbHost
	}
	if *taskDequeueLimit != 0 {
		cfg.Placement.TaskDequeueLimit = *taskDequeueLimit
	}
	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}
	if *port != 0 {
		cfg.Master.Port = *port
	}
	if *offerHoldTime != 0 {
		cfg.Master.OfferHoldTimeSec = int(offerHoldTime.Seconds())
	}
	if *offerPruningPeriod != 0 {
		cfg.Master.OfferPruningPeriodSec = int(offerPruningPeriod.Seconds())
	}
	if *useSTAPI {
		cfg.Storage.UseSTAPI = true
	}
	if *cassandraHosts != nil && len(*cassandraHosts) > 0 {
		if *useSTAPI {
			cfg.Storage.STAPI.Stapi.Cassandra.ContactPoints = *cassandraHosts
		}
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
	metricScope, scopeCloser := tally.NewRootScope("peloton_framework", map[string]string{}, reporter, common_metrics.TallyFlushInterval, metricSeparator)
	defer scopeCloser.Close()
	metricScope.Counter("boot").Inc(1)
	var jobStore storage.JobStore
	var taskStore storage.TaskStore
	var frameworkStore storage.FrameworkInfoStore

	if !cfg.Storage.UseSTAPI {
		// Connect to mysql DB
		if err := cfg.Storage.MySQL.Connect(); err != nil {
			log.Fatalf("Could not connect to database: %+v", err)
		}
		// Migrate DB if necessary
		if errs := cfg.Storage.MySQL.AutoMigrate(); errs != nil {
			log.Fatalf("Could not migrate database: %+v", errs)
		}
		// Initialize job and task stores
		store := mysql.NewJobStore(cfg.Storage.MySQL, metricScope.SubScope("storage"))
		store.DB.SetMaxOpenConns(cfg.Master.DbWriteConcurrency)
		store.DB.SetMaxIdleConns(cfg.Master.DbWriteConcurrency)
		store.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

		jobStore = store
		taskStore = store
		frameworkStore = store
	} else {
		log.Infof("stapi Config: %v", cfg.Storage.STAPI)
		if errs := cfg.Storage.STAPI.AutoMigrate(); errs != nil {
			log.Fatalf("Could not migrate database: %+v", errs)
		}
		store, err := stapi.NewStore(&cfg.Storage.STAPI, metricScope.SubScope("storage"))
		if err != nil {
			log.Fatalf("Could not create stapi store: %+v", err)
		}
		jobStore = store
		taskStore = store
		frameworkStore = store
	}
	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos, frameworkStore)

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

	mesosMasterLocation, err = mesosMasterDetector.GetMasterLocation()
	if err != nil {
		log.Fatalf("Failed to get mesos leading master location, err=%v", err)
	}
	log.Infof("Detected Mesos leading master location: %s", mesosMasterLocation)

	// Each master needs a Mesos inbound
	var mInbound = mhttp.NewInbound(driver)
	inbounds = append(inbounds, mInbound)

	// TODO: update mesos url when leading mesos master changes
	mesosURL := fmt.Sprintf("http://%s%s", mesosMasterLocation, driver.Endpoint())

	mOutbounds := mhttp.NewOutbound(mesosURL)
	peerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		metricScope.SubScope("discovery"),
		peloton_common.MasterRole)
	if err != nil {
		log.Fatalf("Could not create smart peer chooser for %s: %v", peloton_common.MasterRole, err)
	}
	if err := peerChooser.Start(); err != nil {
		log.Fatalf("Could not start smart peer chooser for %s: %v", peloton_common.MasterRole, err)
	}
	defer peerChooser.Stop()
	// The leaderUrl for pOutbound would be updated by leader election NewLeaderCallBack once leader is elected
	pOutbound := http.NewChooserOutbound(peerChooser, &url.URL{Scheme: "http", Path: peloton_common.PelotonEndpointURL})
	pOutbounds := transport.Outbounds{
		Unary: pOutbound,
	}
	outbounds := yarpc.Outbounds{
		peloton_common.MesosMaster:   mOutbounds,
		peloton_common.PelotonMaster: pOutbounds,
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      peloton_common.PelotonMaster,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// TODO: Refactor our storage interfaces to avoid passing both
	// jobstore and taskstore.

	masterScope := metricScope.SubScope("master")
	metrics := metrics.New(masterScope)

	// Initialize job manager related handlers
	jobmgrMetrics := jobmgr.NewMetrics(masterScope)
	job.InitServiceHandler(
		dispatcher,
		&cfg.JobManager,
		jobStore,
		taskStore,
		&jobmgrMetrics,
		peloton_common.PelotonMaster)
	task.InitServiceHandler(dispatcher, jobStore, taskStore, &jobmgrMetrics)
	upgrade.InitManager(dispatcher)

	// Initialize resource manager related handlers
	// Initialize resmgr store
	resstore := mysql.NewResourcePoolStore(cfg.Storage.MySQL.Conn, metricScope.SubScope("storage"))
	resstore.DB.SetMaxOpenConns(cfg.Master.DbWriteConcurrency)
	resstore.DB.SetMaxIdleConns(cfg.Master.DbWriteConcurrency)
	resstore.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	// Initialize resource pool service handler
	resmgrInbounds := []transport.Inbound{
		http.NewInbound(
			fmt.Sprintf(":%d", cfg.ResMgr.Port),
			http.Mux(config.FrameworkURLPath, nethttp.NewServeMux()),
		),
	}
	// create a separate dispatcher for resmgr so client can work with both master and multi-app modes
	resmgrDispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     peloton_common.PelotonResourceManager,
		Inbounds: resmgrInbounds,
	})
	rm := respool.InitServiceHandler(resmgrDispatcher, &cfg.ResMgr, resstore, &metrics)

	// Initialize task queue
	tq := taskq.InitTaskQueue(dispatcher, &metrics, jobStore, taskStore)

	// Initialize host manager related handlers

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// mesos callbacks
	mesos.InitManager(dispatcher, &cfg.Mesos, frameworkStore)
	mesosClient := mpb.New(
		dispatcher.ClientConfig(peloton_common.MesosMaster),
		cfg.Mesos.Encoding)
	om := offer.InitManager(
		dispatcher,
		time.Duration(cfg.Master.OfferHoldTimeSec)*time.Second,
		time.Duration(cfg.Master.OfferPruningPeriodSec)*time.Second,
		mesosClient)

	master_task.InitTaskStateManager(
		dispatcher,
		cfg.Master.TaskUpdateBufferSize,
		cfg.Master.TaskUpdateAckConcurrency,
		cfg.Master.DbWriteConcurrency,
		jobStore,
		taskStore)

	hostmgrMetrics := hostmgr.NewMetrics(metricScope.SubScope("master"))
	// Init host manager service handler
	hostmgr.InitServiceHandler(
		dispatcher,
		mesosClient,
		hostmgrMetrics,
		om.Pool())

	// Start master dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton master on port %v", cfg.Master.Port)

	// Start resmgr dispatch loop
	if err := resmgrDispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton resmgr on port %v", cfg.ResMgr.Port)

	// This is the address of the local peloton master address to be announced
	// via leader election
	ip, err := util.ListenIP()
	if err != nil {
		log.Fatalf("Failed to get ip, err=%v", err)
	}
	localPelotonMasterAddr := fmt.Sprintf("http://%s:%d", ip, cfg.Master.Port)
	pMaster := newPelotonMaster(
		*env,
		mInbound,
		mOutbounds,
		tq,
		cfg,
		localPelotonMasterAddr,
		mesosMasterDetector,
		om,
		rm)
	leadercandidate, err := leader.NewCandidate(
		cfg.Election,
		metricScope.SubScope("election"),
		peloton_common.MasterRole,
		pMaster)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}
	err = leadercandidate.Start()
	if err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer leadercandidate.Stop()

	// Initialize and start placement engine
	placementEngine := placement.New(
		dispatcher,
		&cfg.Placement,
		metricScope.SubScope("placement"),
		peloton_common.PelotonMaster,
		peloton_common.PelotonMaster)
	placementEngine.Start()
	defer placementEngine.Stop()

	select {}
}
