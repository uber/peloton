package main

import (
	"golang.org/x/net/context"

	"github.com/yarpc/yarpc-go"
	"github.com/yarpc/yarpc-go/transport"
	"github.com/yarpc/yarpc-go/transport/http"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/log"

	"code.uber.internal/infra/peloton/master/job"
	"code.uber.internal/infra/peloton/master/task"
	"code.uber.internal/infra/peloton/master/upgrade"
)

type appConfig struct {
	Logging  log.Configuration
	Metrics  metrics.Configuration
	Sentry   log.SentryConfiguration
	Verbose  bool
}

type requestLogInterceptor struct{}

func (requestLogInterceptor) Handle(
	ctx context.Context,
	opts transport.Options,
	req *transport.Request,
	resw transport.ResponseWriter,
	handler transport.Handler) error {

	log.Infof("Received a request to %q from client %q (encoding %q)\n",
		req.Procedure, req.Caller, req.Encoding)
	return handler.Handle(ctx, opts, req, resw)
}

func main() {
	var cfg appConfig
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

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-master",
		Inbounds: []transport.Inbound{
			http.NewInbound(":5289"),
		},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})

	job.InitManager(dispatcher)
	task.InitManager(dispatcher)
	upgrade.InitManager(dispatcher)


	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	log.Info("Started rpc server")
	select {}

}
