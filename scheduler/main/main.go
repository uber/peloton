package main

import (
	"code.uber.internal/go-common.git/x/config"
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
)

type appConfig struct {
	Logging log.Configuration
	Metrics metrics.Configuration
	Sentry  log.SentryConfiguration
	Verbose bool
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

	// Block forever.
	select {}
}
