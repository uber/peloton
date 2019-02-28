// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"fmt"
	"io"
	nethttp "net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	log "github.com/sirupsen/logrus"

	"github.com/uber-go/tally"
	tallym3 "github.com/uber-go/tally/m3"
	tallymulti "github.com/uber-go/tally/multi"
	tallyprom "github.com/uber-go/tally/prometheus"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	// TallyFlushInterval is the flush interval for metrics buffered in Tally to
	// be flushed to the backend
	TallyFlushInterval = 1 * time.Second

	// M3 related configs
	m3HostPort = "M3_HOSTPORT"
	m3Service  = "M3_SERVICE"
	m3ENV      = "M3_ENV"
	// TODO: reuse the global var name constant CLUSTER
	m3Cluster = "CLUSTER"
)

// Config will be containing the metrics configuration
type Config struct {
	MultiReporter  bool                   `yaml:"multi_reporter"`
	Prometheus     *prometheusConfig      `yaml:"prometheus"`
	Statsd         *statsdConfig          `yaml:"statsd"`
	M3             *tallym3.Configuration `yaml:"m3"`
	RuntimeMetrics *runtimeConfig         `yaml:"runtime_metrics"`
}

// runtimeConfig contains configuration for initializing runtime metrics
type runtimeConfig struct {
	Enabled         bool          `yaml:"enabled"`
	CollectInterval time.Duration `yaml:"interval"`
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
	// TODO: move mux setup out of metrics initialization
	mux := setupServeMux()

	var reporter tally.StatsReporter
	metricSeparator := "."
	// promethus panics if scope name contains "-", hence force convert to "_"
	rootMetricScope = SafeScopeName(rootMetricScope)

	var metricScope tally.Scope
	var scopeCloser io.Closer
	if cfg.MultiReporter {
		var m3Reporter tallym3.Reporter
		var promReporter tallyprom.Reporter

		metricSeparator = "_"

		if cfg.Prometheus != nil && cfg.Prometheus.Enable {
			promReporter = tallyprom.NewReporter(tallyprom.Options{})
			promHandler := promReporter.HTTPHandler()

			// if prometheus support is enabled, handle /metrics to serve prom metrics
			log.Infof("Setting up prometheus metrics handler at /metrics")
			mux.Handle("/metrics", promHandler)
		}

		loadM3Configs(cfg.M3)
		if cfg.M3 != nil && cfg.M3.HostPort != "" {
			r, err := cfg.M3.NewReporter()
			if err != nil {
				log.Fatalf("Failed to create m3 reporter: %v", err)
			}
			m3Reporter = r
		}

		var reporter tally.CachedStatsReporter
		if m3Reporter != nil && promReporter != nil {
			reporter = tallymulti.NewMultiCachedReporter(m3Reporter, promReporter)
		} else if m3Reporter != nil {
			reporter = tallymulti.NewMultiCachedReporter(m3Reporter)
		} else if promReporter != nil {
			reporter = tallymulti.NewMultiCachedReporter(promReporter)
		} else {
			log.Fatal("No metrics reporter is configured")
		}

		metricScope, scopeCloser = tally.NewRootScope(
			tally.ScopeOptions{
				Prefix:         rootMetricScope,
				Tags:           map[string]string{},
				CachedReporter: reporter,
				Separator:      metricSeparator,
			},
			metricFlushInterval)
	} else {
		// To use statsd, MultiReporter should be turned off
		if cfg.Statsd != nil && cfg.Statsd.Enable {
			log.Infof("Metrics configured with statsd endpoint %s", cfg.Statsd.Endpoint)
			c, err := statsd.NewClient(cfg.Statsd.Endpoint, "")
			if err != nil {
				log.Fatalf("Unable to setup Statsd client: %v", err)
			}
			reporter = tallystatsd.NewReporter(c, tallystatsd.Options{})
		} else {
			log.Warnf("No metrics backends configured, using the statsd.NoopClient")
			c, _ := statsd.NewNoopClient()
			reporter = tallystatsd.NewReporter(c, tallystatsd.Options{})
		}

		metricScope, scopeCloser = tally.NewRootScope(
			tally.ScopeOptions{
				Prefix:    rootMetricScope,
				Tags:      map[string]string{},
				Reporter:  reporter,
				Separator: metricSeparator,
			},
			metricFlushInterval)
	}

	return metricScope, scopeCloser, mux
}

// SafeScopeName returns a safe scope name that removes dash which is not
// allowed in tally
func SafeScopeName(name string) string {
	return strings.Replace(name, "-", "", -1)
}

// setupServeMux constructs server mux which is used to mux together other (non-RPC) handlers,
// like metrics exposition endpoints, etc
func setupServeMux() *nethttp.ServeMux {
	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()
	mux.HandleFunc("/health", func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		// TODO: make this healthcheck live, and check some kind of internal health?
		if true {
			w.WriteHeader(nethttp.StatusOK)
			fmt.Fprintln(w, `OK`)
		} else {
			w.WriteHeader(nethttp.StatusInternalServerError)
			fmt.Fprintln(w, `UNHEALTHY`)
		}
	})

	// Profiling
	mux.Handle("/debug/pprof/", nethttp.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", nethttp.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", nethttp.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", nethttp.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", nethttp.HandlerFunc(pprof.Trace))

	return mux
}

// loadM3Configs loads m3 configs from env vars if exist
func loadM3Configs(cfg *tallym3.Configuration) {
	if cfg == nil {
		return
	}

	log.Info("Load m3 configs from env vars")

	if v := os.Getenv(m3HostPort); v != "" {
		log.WithField("hostport", v).
			Info("Load m3 config")
		cfg.HostPort = v
	}

	if v := os.Getenv(m3Service); v != "" {
		log.WithField("service", v).
			Info("Load m3 config")
		cfg.Service = v
	}

	if v := os.Getenv(m3ENV); v != "" {
		log.WithField("env", v).
			Info("Load m3 config")
		cfg.Env = v
	}

	if v := os.Getenv(m3Cluster); v != "" {
		log.WithField("tag.cluster", v).
			Info("Load m3 config")
		if cfg.CommonTags == nil {
			cfg.CommonTags = make(map[string]string)
		}
		cfg.CommonTags[strings.ToLower(m3Cluster)] = v
	}
}
