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

package apiproxy

import (
	"github.com/uber/peloton/pkg/common/config"
	"os"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"

	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string

	app = kingpin.New(common.PelotonAPIProxy, "Peloton API Proxy")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	cfgFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(
		&logging.LogFieldFormatter{
			Formatter: &logging.SecretsFormatter{Formatter: &log.JSONFormatter{}},
			Fields: log.Fields{
				common.AppLogField: app.Name,
			},
		},
	)

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)

	log.WithField("files", *cfgFiles).Info("Loading API Proxy config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	log.WithField("config", cfg).Info("Loaded API Proxy configuration")

	rootScope, scopeCloser, _ := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonAPIProxy,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	// start collecting runtime metrics
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	select {}
}
