package main

import (
	"gopkg.in/alecthomas/kingpin.v2"
	nethttp "net/http"
	"os"
	"time"

	"code.uber.internal/infra/peloton/common/config"
	placementconfig "code.uber.internal/infra/peloton/placement/config"

	"go.uber.org/yarpc"

	log "github.com/Sirupsen/logrus"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallyprom "github.com/uber-go/tally/prometheus"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
	rootMetricScope     = "peloton_placement"
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

	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()

	// TODO: Refactor metrics initialization code into common package.
	var reporter tally.StatsReporter
	var promHandler nethttp.Handler
	metricSeparator := "."
	if cfg.Metrics.Prometheus != nil && cfg.Metrics.Prometheus.Enable {
		metricSeparator = "_"
		promreporter := tallyprom.NewReporter(nil)
		reporter = promreporter
		promHandler = promreporter.HTTPHandler()
	} else if cfg.Metrics.Statsd != nil && cfg.Metrics.Statsd.Enable {
		log.Infof("Metrics configured with statsd endpoint %s", cfg.Metrics.Statsd.Endpoint)
		c, err := statsd.NewClient(cfg.Metrics.Statsd.Endpoint, "")
		if err != nil {
			log.Fatalf("Unable to setup Statsd client: %v", err)
		}
		reporter = tallystatsd.NewReporter(c, tallystatsd.NewOptions())
	} else {
		log.Warnf("No metrics backends configured, using the statsd.NoopClient")
		c, _ := statsd.NewNoopClient()
		reporter = tallystatsd.NewReporter(c, tallystatsd.NewOptions())
	}

	if promHandler != nil {
		// if prometheus support is enabled, handle /metrics to serve prom metrics
		log.Infof("Setting up prometheus metrics handler at /metrics")
		mux.Handle("/metrics", promHandler)
	}

	metricScope, scopeCloser := tally.NewRootScope(
		rootMetricScope,
		map[string]string{},
		reporter,
		metricFlushInterval,
		metricSeparator)
	defer scopeCloser.Close()

	metricScope.Counter("boot").Inc(1)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: "peloton-placement",
	})

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	// TODO(wu): Add placement engine initialization logic
	log.Warn("The placement engine is currently embedded in the master, dont use this")

	select {}
}
