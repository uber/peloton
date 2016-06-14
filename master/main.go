package main

import (
    "golang.org/x/net/context"

    "github.com/yarpc/yarpc-go"
    "github.com/yarpc/yarpc-go/encoding/json"
    "github.com/yarpc/yarpc-go/transport"
    "github.com/yarpc/yarpc-go/transport/http"

    "code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/log"

    "peloton/job"
)

type appConfig struct {
    Logging  log.Configuration
    Metrics  metrics.Configuration
    Sentry   log.SentryConfiguration
    Verbose  bool
}

type jobManagerHandler struct {
}

func (h *jobManagerHandler) Create(
    reqMeta *json.ReqMeta,
    body *job.CreateRequest) (*job.CreateResponse, *json.ResMeta, error) {

    log.Infof("Job create called: %s", body)
    return &job.CreateResponse{}, nil, nil
}

func (h *jobManagerHandler) Get(
    reqMeta *json.ReqMeta,
    body *job.GetRequest) (*job.GetResponse, *json.ResMeta, error) {

    log.Infof("Job get called: %s", body)
    return &job.GetResponse{}, nil, nil
}

func (h *jobManagerHandler) Query(
    reqMeta *json.ReqMeta,
    body *job.QueryRequest) (*job.QueryResponse, *json.ResMeta, error) {

    log.Infof("Job query called: %s", body)
    return &job.QueryResponse{}, nil, nil
}

func (h *jobManagerHandler) Delete(
    reqMeta *json.ReqMeta,
    body *job.DeleteRequest) (*job.DeleteResponse, *json.ResMeta, error) {

    log.Infof("Job delete called: %s", body)
    return &job.DeleteResponse{}, nil, nil
}

type requestLogInterceptor struct{}

func (requestLogInterceptor) Handle(
    ctx context.Context, req *transport.Request, resw transport.ResponseWriter,
    handler transport.Handler) error {
    
    log.Infof("Received a request to %q\n", req.Procedure)
    return handler.Handle(ctx, req, resw)
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

	rpc := yarpc.New(yarpc.Config{
		Name: "peloton-master",
		Inbounds: []transport.Inbound{
			http.NewInbound(":5289"),
		},
		Interceptor: yarpc.Interceptors(requestLogInterceptor{}),
	})

	handler := jobManagerHandler{}

	json.Register(rpc, json.Procedure("create", handler.Create))
	json.Register(rpc, json.Procedure("get", handler.Get))
	json.Register(rpc, json.Procedure("query", handler.Query))
	json.Register(rpc, json.Procedure("delete", handler.Delete))

	if err := rpc.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

    log.Info("Started rpc server")
    select {}

}
