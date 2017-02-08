package metrics

import (
	"fmt"
	"io"
	nethttp "net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallyprom "github.com/uber-go/tally/prometheus"
	tallystatsd "github.com/uber-go/tally/statsd"
)

// Config will be contianing the metrics configuration
type Config struct {
	Prometheus *prometheusConfig `yaml:"prometheus"`
	Statsd     *statsdConfig     `yaml:"statsd"`
}

type prometheusConfig struct {
	Enable bool `yaml:"enable"`
}

type statsdConfig struct {
	Enable   bool   `yaml:"enable"`
	Endpoint string `yaml:"endpoint"`
}

// InitMetricScope initialize a root scope and its closer, with a http server mux
func InitMetricScope(
	cfg *Config,
	rootMetricScope string,
	metricFlushInterval time.Duration) (tally.Scope, io.Closer, *nethttp.ServeMux) {
	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()
	var reporter tally.StatsReporter
	var promHandler nethttp.Handler
	metricSeparator := "."
	if cfg.Prometheus != nil && cfg.Prometheus.Enable {
		// tally panics if scope name contains "-", hence force convert to "_"
		rootMetricScope = strings.Replace(rootMetricScope, "-", "_", -1)
		metricSeparator = "_"
		promReporter := tallyprom.NewReporter(nil)
		reporter = promReporter
		promHandler = promReporter.HTTPHandler()
	} else if cfg.Statsd != nil && cfg.Statsd.Enable {
		log.Infof("Metrics configured with statsd endpoint %s", cfg.Statsd.Endpoint)
		c, err := statsd.NewClient(cfg.Statsd.Endpoint, "")
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
	mux.HandleFunc("/health", func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		// TODO: make this healthcheck live, and check some kind of internal health?
		if true {
			w.WriteHeader(nethttp.StatusOK)
			fmt.Fprintln(w, `\(★ω★)/`)
		} else {
			w.WriteHeader(nethttp.StatusInternalServerError)
			fmt.Fprintln(w, `(╥﹏╥)`)
		}
	})

	metricScope, scopeCloser := tally.NewRootScope(
		rootMetricScope,
		map[string]string{},
		reporter,
		metricFlushInterval,
		metricSeparator)
	return metricScope, scopeCloser, mux
}
