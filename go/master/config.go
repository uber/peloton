package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/go-common.git/x/tchannel"
)

type appConfig struct {
	Logging  log.Configuration
	Metrics  metrics.Configuration
	TChannel xtchannel.Configuration
	Sentry   log.SentryConfiguration
	Verbose  bool
}
