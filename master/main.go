package main

import (
	"golang.org/x/net/context"

	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/transport"
	"github.com/yarpc/yarpc-go/transport/http"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/master/job"
	"code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/master/upgrade"
	"code.uber.internal/infra/peloton/master/offer"
	"code.uber.internal/infra/peloton/yarpc/transport/mhttp"
	"code.uber.internal/infra/peloton/storage/mysql"
)

// Simple request interceptor which logs the request summary
type requestLogInterceptor struct{}

func (requestLogInterceptor) Handle(
	ctx context.Context,
	opts transport.Options,
	req *transport.Request,
	resw transport.ResponseWriter,
	handler transport.Handler) error {

	log.Infof("Received a %s request from %s", req.Procedure, req.Caller)
	return handler.Handle(ctx, opts, req, resw)
}

func main() {
	var cfg AppConfig
	if err := config.Load(&cfg); err != nil {
		log.Fatalf("Error initializing configuration: %s", err)
	}
	log.Configure(&cfg.Logging, cfg.Verbose)
	log.ConfigureSentry(&cfg.Sentry)

	metrics, err := cfg.Metrics.New()
	if err != nil {
		log.Fatalf("Could not connect to metrics: %v", err)
	}
	metrics.Counter("boot").Inc(1)

	// connect to mysql DB
	if err := cfg.DbConfig.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}

	// Migrate DB if necessary
	if errs := cfg.DbConfig.AutoMigrate(); errs != nil {
		log.Fatalf("Could not migrate database: %+v", errs)
	}

	// TODO: Load framework ID from DB
	f := mesos.NewSchedulerDriver(&cfg.Mesos.Framework, nil)
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-master",
		Inbounds: []transport.Inbound{
			http.NewInbound(":" + string(cfg.Master.Port)),
			mhttp.NewInbound(cfg.Mesos.HostPort, f),
		},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})
	store := mysql.NewMysqlJobStore(cfg.DbConfig.Conn)

	mesos.InitManager(dispatcher)
	job.InitManager(dispatcher, store, store)
	task.InitManager(dispatcher, store, store)
	upgrade.InitManager(dispatcher)
	offer.InitManager(dispatcher)

	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	log.Info("Started rpc server")
	select {}
}
