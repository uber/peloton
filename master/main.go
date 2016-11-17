package main

import (
	"flag"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"golang.org/x/net/context"
	"os"
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
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"strconv"
	"sync"
)

const (
	environmentKey     = "UBER_ENVIRONMENT"
	productionEnvValue = "production"
)

// Simple request interceptor which logs the request summary
type requestLogInterceptor struct{}

func (requestLogInterceptor) Handle(
	ctx context.Context,
	opts transport.Options,
	req *transport.Request,
	resw transport.ResponseWriter,
	handler transport.Handler) error {

	log.Debugf("Received a %s request from %s", req.Procedure, req.Caller)
	return handler.Handle(ctx, opts, req, resw)
}

type pelotonMaster struct {
	mesosInbound           mhttp.Inbound
	pelotonMasterOutbound  transport.Outbound
	mesosOutbound          transport.Outbound
	taskQueue              *task.TaskQueue
	store                  *mysql.MysqlJobStore
	cfg                    *AppConfig
	mutex                  *sync.Mutex
	localPelotonMasterAddr string
	mesosDetector          mesos.MasterDetector
	offerManager           *offer.OfferManager
}

func NewPelotonMaster(mInbound mhttp.Inbound, mOutbound transport.Outbound, pOutbound transport.Outbound, tq *task.TaskQueue,
	store *mysql.MysqlJobStore, cfg *AppConfig, localPelotonMasterAddr string, mesosDetector mesos.MasterDetector,
	om *offer.OfferManager) *pelotonMaster {
	result := pelotonMaster{
		mesosInbound:          mInbound,
		pelotonMasterOutbound: pOutbound,
		mesosOutbound:         mOutbound,
		taskQueue:             tq,
		store:                 store,
		cfg:                   cfg,
		mutex:                 &sync.Mutex{},
		localPelotonMasterAddr: localPelotonMasterAddr,
		mesosDetector:          mesosDetector,
		offerManager:           om,
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
	}
	err = p.taskQueue.LoadFromDB(p.store, p.store)
	if err != nil {
		log.Errorf("Failed to taskQueue.LoadFromDB, err = %v", err)
	}
	util.SetOutboundURL(p.pelotonMasterOutbound, p.localPelotonMasterAddr)

	// TODO: Start offer manager

	return err
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

	// TODO: Shut down offer manager

	return err
}

// NewLeaderCallBack is the callback when some other node becomes the leader, leader is hostname of the leader
func (p *pelotonMaster) NewLeaderCallBack(leader string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	log.Infof("New Leader is elected : %v", leader)
	// leader changes, so point pelotonMasterOutbound to the new leader
	util.SetOutboundURL(p.pelotonMasterOutbound, leader)
	return nil
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
	return p.localPelotonMasterAddr
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

	metrics, err := cfg.Metrics.New()
	if err != nil {
		log.Fatalf("Could not connect to metrics: %v", err)
	}
	metrics.Counter("boot").Inc(1)

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
	driver := mesos.InitSchedulerDriver(&cfg.Mesos.Framework, store)
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
	mesosUrl := fmt.Sprintf("http://%s%s", mesosMasterLocation, driver.Endpoint())

	mOutbound := mhttp.NewOutbound(mesosUrl)
	// The leaderUrl for pOutbound would be updated by leader election NewLeaderCallBack once leader is elected
	pOutbound := http.NewOutbound("")
	outbounds := transport.Outbounds{
		"mesos-master":   mOutbound,
		"peloton-master": pOutbound,
	}
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:        "peloton-master",
		Inbounds:    inbounds,
		Outbounds:   outbounds,
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})

	// Initalize managers
	job.InitManager(dispatcher, store, store)
	task.InitManager(dispatcher, store, store)
	tq := task.InitTaskQueue(dispatcher)
	upgrade.InitManager(dispatcher)

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// mesos callbacks
	mesos.InitManager(dispatcher, &cfg.Mesos, store)
	om := offer.InitManager(dispatcher, time.Duration(cfg.Master.OfferHoldTimeSec)*time.Second,
		time.Duration(cfg.Master.OfferPruningPeriodSec)*time.Second)
	task.InitTaskStateManager(dispatcher, store, store)

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

	pMaster := NewPelotonMaster(mInbound, mOutbound, pOutbound, tq, store, &cfg, localPelotonMasterAddr,
		mesosMasterDetector, om)
	leader.NewZkElection(cfg.Election, localPelotonMasterAddr, pMaster)

	// Defer initializing scheduler till the end
	scheduler.InitManager(dispatcher, &cfg.Scheduler)

	select {}
}
