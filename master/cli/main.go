package main

import (
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master"
	"code.uber.internal/infra/peloton/master/job"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/master/offer"
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
	env     = app.Flag("env", "environment (development will do no mesos master auto discovery) (set $ENVIRONMENT to override)").Short('e').Default("development").
		Envar("ENVIRONMENT\nUBER_ENVIRONMENT").Enum("development", "production")
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
	cfg           *AppConfig
	mutex         *sync.Mutex
	// Local address for peloton master
	localAddr     string
	mesosDetector mesos.MasterDetector
	offerManager  *offer.Manager
	env           string
}

func newPelotonMaster(env string,
	mInbound mhttp.Inbound,
	mOutbounds transport.Outbounds,
	pChooser *peer.Chooser,
	tq *task.Queue,
	store *mysql.JobStore,
	cfg *AppConfig,
	localPelotonMasterAddr string,
	mesosDetector mesos.MasterDetector,
	om *offer.Manager) *pelotonMaster {
	result := pelotonMaster{
		env:           env,
		mesosInbound:  mInbound,
		peerChooser:   pChooser,
		mesosOutbound: mOutbounds,
		taskQueue:     tq,
		store:         store,
		cfg:           cfg,
		mutex:         &sync.Mutex{},
		localAddr:     localPelotonMasterAddr,
		mesosDetector: mesosDetector,
		offerManager:  om,
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
	cfg, err := NewAppConfig(*configs...)
	if err != nil {
		log.Fatalf("Error initializing configuration: %v", err)
	}

	// now, override any CLI flags in the loaded AppConfig
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}
	if *dbHost != "" {
		cfg.DbConfig.Host = *dbHost
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
	metricSeparator := "."
	if cfg.Metrics.Prometheus != nil && cfg.Metrics.Prometheus.Enable {
		metricSeparator = "_"
		reporter = tallyprom.NewReporter(nil)
		log.Fatalf("Metrics configured with prometheus endpoint /metrics, not wired up yet!")
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
	if err := cfg.DbConfig.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// Migrate DB if necessary
	if errs := cfg.DbConfig.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}
	// Initialize job and task stores
	store := mysql.NewJobStore(cfg.DbConfig.Conn, metricScope.SubScope("storage"))

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos, store)
	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.Master.Port)),
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
	pOutbound := http.NewChooserOutbound(pelotonMasterPeerChooser, &url.URL{Scheme: "http"})
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
	metrics := master.NewMetrics(metricScope.SubScope("master"))
	job.InitManager(dispatcher, store, store, &metrics)
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
	task.InitTaskStateManager(dispatcher, store, store, mesosClient)

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
		mesosMasterDetector, om)
	leader.NewZkElection(cfg.Election, localPelotonMasterAddr, pMaster)

	// Defer initializing scheduler till the end
	schedulerMetrics := scheduler.NewMetrics(metricScope.SubScope("scheduler"))
	scheduler.InitManager(dispatcher, &cfg.Scheduler, mesosClient, &schedulerMetrics)

	select {}
}
