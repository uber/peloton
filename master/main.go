package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/peloton/leader"
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
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

const (
	environmentKey     = "UBER_ENVIRONMENT"
	productionEnvValue = "production"
	// metricFlushInterval is the flush interval for metrics as configured in Tally. This may be unused, depending on
	// if the backend selected supports push or pull metrics
	metricFlushInterval = 30 * time.Second
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
}

func newPelotonMaster(mInbound mhttp.Inbound,
	mOutbounds transport.Outbounds,
	pChooser *peer.Chooser,
	tq *task.Queue,
	store *mysql.JobStore,
	cfg *AppConfig,
	localPelotonMasterAddr string,
	mesosDetector mesos.MasterDetector,
	om *offer.Manager) *pelotonMaster {
	result := pelotonMaster{
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
	if os.Getenv(environmentKey) == productionEnvValue {
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
	// Parse command line arguments
	flag.Parse()

	// Load configuration YAML file
	var cfg AppConfig
	if err := config.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	} else {
		// Dump the current configuration
		log.WithField("config", cfg).Info("Loading Peloton configuration")
	}

	// Override app config from env vars if set
	LoadConfigFromEnv(&cfg)

	log.Configure(&cfg.Logging, cfg.Verbose)
	log.ConfigureSentry(&cfg.Sentry)

	var reporter tally.StatsReporter
	if cfg.Metrics.Prometheus != nil && cfg.Metrics.Prometheus.Enable {
		// TODO: setup prom
		log.Fatalf("Prom metrics unimplemented, until https://github.com/uber-go/tally/pull/16 lands")
	} else if cfg.Metrics.Statsd != nil && cfg.Metrics.Statsd.Enable {
		log.Infof("Metrics configured with statsd endpoint %s", cfg.Metrics.Statsd.Endpoint)
		c, err := statsd.NewClient(cfg.Metrics.Statsd.Endpoint, "")
		if err != nil {
			log.Fatalf("Unable to setup Statsd client: %v", err)
		}
		reporter = tallystatsd.NewStatsdReporter(c, tallystatsd.NewOptions())
	} else {
		log.Warnf("No metrics backends configured, using the statsd.NoopClient")
		c, _ := statsd.NewNoopClient()
		reporter = tallystatsd.NewStatsdReporter(c, tallystatsd.NewOptions())
	}
	metricScope, scopeCloser := tally.NewRootScope("peloton_framework", map[string]string{}, reporter, metricFlushInterval)
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
	store := mysql.NewMysqlJobStore(cfg.DbConfig.Conn)

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos, store)
	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(cfg.Master.Port)),
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
	if os.Getenv(environmentKey) == productionEnvValue {
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
	job.InitManager(dispatcher, store, store, metricScope.SubScope("job"))
	task.InitManager(dispatcher, store, store, metricScope.SubScope("task"))
	tq := task.InitTaskQueue(dispatcher)
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
	pMaster := newPelotonMaster(mInbound, mOutbounds, pelotonMasterPeerChooser, tq, store, &cfg, localPelotonMasterAddr,
		mesosMasterDetector, om)
	leader.NewZkElection(cfg.Election, localPelotonMasterAddr, pMaster)

	// Defer initializing scheduler till the end
	scheduler.InitManager(dispatcher, &cfg.Scheduler, mesosClient)

	select {}
}
