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

package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/rpc"
	cqos "github.com/uber/peloton/pkg/mock-cqos"

	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	hostLoadOverwrite = "/host-load"
	_usage            = "usage: `/host-load?peloton-mesos-agent0=1&peloton-mesos" +
		"-agent0=2&peloton-mesos-agent0=3`"
)

var (
	app = kingpin.New("mock-cqos", "Peloton mock Cqos advisor "+
		"for testing purpose only")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

	configFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	grpcPort = app.Flag(
		"grpc-port", "Mock Cqos GRPC port (mockcqos.grpc_port override) "+
			"(set $GRPC_PORT to override)").
		Envar("GRPC_PORT").
		Int()

	httpPort = app.Flag(
		"http-port", "Mock Cqos HTTP port (mockcqos.http_port override) "+
			"(set $PORT to override)").
		Envar("HTTP_PORT").
		Int()

	hostname0 = "peloton-mesos-agent0"
	hostname1 = "peloton-mesos-agent1"
	hostname2 = "peloton-mesos-agent2"
	hostnames = [3]string{hostname0, hostname1, hostname2}

	defaultHostLoad = map[string]int32{
		hostname0: 1,
		hostname1: 2,
		hostname2: 3,
	}

	serviceHandler *cqos.ServiceHandler
)

func main() {
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

	log.WithField("files", *configFiles).Info("Loading mock cqos config")
	var cfg Config
	if err := config.Parse(&cfg, *configFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *grpcPort != 0 {
		cfg.CqosAdvisor.GRPCPort = *grpcPort
	}

	if *httpPort != 0 {
		cfg.CqosAdvisor.HTTPPort = *httpPort
	}

	log.WithField("config", cfg).Info("Loaded mock cqos configuration")

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if true {
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "%s=%s\n", "Status", "OK")
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "%s=%s\n", "Status", "UNHEALTHY")
		}
	})

	// Profiling
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))

	mux.HandleFunc(hostLoadOverwrite, HostLoadHandler())

	inbounds := rpc.NewInbounds(cfg.CqosAdvisor.HTTPPort,
		cfg.CqosAdvisor.GRPCPort, mux)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "mock-cqos",
		Inbounds: inbounds,
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  yarpc.UnaryInboundMiddleware(),
			Oneway: yarpc.OnewayInboundMiddleware(),
			Stream: yarpc.StreamInboundMiddleware(),
		},
	})

	serviceHandler = cqos.NewServiceHandler(dispatcher, defaultHostLoad)
	cqos.NewServiceHandler(dispatcher, defaultHostLoad)

	// Start YARPC dispatcher.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	// Block the main process.
	select {}
}

// Handler returns a handler for mock-cqos Put request
func HostLoadHandler() func(http.ResponseWriter,
	*http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Overwriting mock-cqos mock hostload: ")
		result := make(map[string]int32)
		values := r.URL.Query()

		for _, name := range hostnames {
			v, ok := values[name]
			if !ok || len(v) == 0 {
				result[name] = 100
				fmt.Fprintf(w, "%s=%s\n", name, "100")
				continue
			}
			score, err := strconv.Atoi(v[0])
			if err != nil {
				fmt.Fprintln(w, _usage)
				fmt.Fprintf(w, err.Error())
				continue
			}
			result[name] = int32(score)
			fmt.Fprintf(w, "%s=%s\n", name, v[0])
		}
		log.WithField("hostload:",
			result).Info("Resetting host load")
		serviceHandler.UpdateHostLoad(result)

	}
}
