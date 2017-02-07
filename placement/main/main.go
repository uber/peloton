package main

import (
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/metrics"
	placementconfig "code.uber.internal/infra/peloton/placement/config"

	"go.uber.org/yarpc"

	log "github.com/Sirupsen/logrus"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
)

var (
	version string
	app     = kingpin.New("peloton-placement", "Peloton Placement Engine")

	debug = app.
		Flag("debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Bool()

	configs = app.
		Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	env = app.
		Flag("env", "environment (set $PELOTON_ENVIRONMENT to override)").
		Short('e').
		Default("development").
		Envar("PELOTON_ENVIRONMENT").
		Enum("development", "production")
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)

	var cfg placementconfig.Config

	if err := config.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse placement engine config")
	}

	rootScope, scopeCloser, _ := metrics.InitMetricScope(&cfg.Metrics, common.PelotonPlacement, metricFlushInterval)
	defer scopeCloser.Close()

	rootScope.Counter("boot").Inc(1)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonPlacement,
	})

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	// TODO(wu): Add placement engine initialization logic
	log.Warn("The placement engine is currently embedded in the master, dont use this")

	select {}
}
