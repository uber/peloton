package main

import (
	"flag"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"golang.org/x/net/context"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
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
		log.WithField("config", cfg).Info("Loading Peloton configuration")
	}
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
	// Only leader needs a Mesos inbound
	if *role == "leader" {
		inbounds = append(inbounds, mhttp.NewInbound(cfg.Mesos.HostPort, driver))
	}

	// TODO: resolve Mesos master and Peloton master URLs from zookeeper
	mesosUrl := fmt.Sprintf("http://%s%s", cfg.Mesos.HostPort, driver.Endpoint())
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
	scheduler.InitManager(dispatcher)

	// Only leader needs to initialize Mesos and Offer managers
	if *role == "leader" {
		mesos.InitManager(dispatcher, &cfg.Mesos, store)
		offer.InitManager(dispatcher)
		task.InitTaskStateManager(dispatcher, store, store)
	}

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	log.Infof("Started Peloton master %v", *role)
	select {}
}
