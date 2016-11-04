package main

import (
	"flag"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"golang.org/x/net/context"
	"os"

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
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"strconv"
)

const (
	environmentKey     = "UBER_ENVIRONMENT"
	productionEnvValue = "production"
)

var role = flag.String("role", "leader",
	"The role of Peloton master [leader|follower]")

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

	// initialize leader election
	// TODO: integrate leader election into peloton components
	if os.Getenv(environmentKey) == productionEnvValue {
		// TODO : enable for Prod once zk node is set up and we are confident to roll this out
		log.Infof("Skip leader election in %v for now", productionEnvValue)
	} else {
		leader.InitLeaderElection(cfg.Election)
	}

	// Check if the master is a leader or follower
	// TODO: use zookeeper for leader election
	var masterPort int
	if *role == "leader" {
		masterPort = cfg.Master.Leader.Port
	} else if *role == "follower" {
		masterPort = cfg.Master.Follower.Port
	} else {
		log.Fatalf("Unknown master role '%s', must be leader or follower", *role)
	}

	// Initialize YARPC dispatcher with necessary inbounds and outbounds
	driver := mesos.InitSchedulerDriver(&cfg.Mesos.Framework, store)
	inbounds := []transport.Inbound{
		http.NewInbound(":" + strconv.Itoa(masterPort)),
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

	// Only leader needs a Mesos inbound
	if *role == "leader" {
		inbounds = append(inbounds, mhttp.NewInbound(mesosMasterLocation, driver))
	}

	// TODO: update mesos url when leading mesos master changes
	mesosUrl := fmt.Sprintf("http://%s%s", mesosMasterLocation, driver.Endpoint())
	leaderUrl := fmt.Sprintf("http://%s:%d", cfg.Master.Leader.Host, cfg.Master.Leader.Port)

	// TODO: initialize one outbound for each follower so we can
	// switch to it at fail-over
	outbounds := transport.Outbounds{
		"mesos-master":   mhttp.NewOutbound(mesosUrl),
		"peloton-master": http.NewOutbound(leaderUrl),
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
	task.InitTaskQueue(dispatcher)
	upgrade.InitManager(dispatcher)

	// Only leader needs to initialize Mesos and Offer managers
	if *role == "leader" {
		mesos.InitManager(dispatcher, &cfg.Mesos, store)
		offer.InitManager(dispatcher)
		task.InitTaskStateManager(dispatcher, store, store)
	}

	// Defer initalizing to the end
	scheduler.InitManager(dispatcher, &cfg.Scheduler)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton master %v", *role)
	select {}
}
