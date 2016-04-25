package main

import (
	gen "code.uber.internal/infra/peloton/.gen/go/peloton-master"

	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
)

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

	if _, err := cfg.TChannel.New("peloton-master", metrics, registerHandlers); err != nil {
		log.Fatalf("TChannel.New failed: %v", err)
	}

	// Block forever.
	select {}
}

func registerHandlers(ch *tchannel.Channel, server *thrift.Server) {
	server.Register(gen.NewTChanMyServiceServer(myHandler{}))
}

type myHandler struct{}

func (myHandler) Hello(ctx thrift.Context) (string, error) {
	return "Hello World!", nil
}
