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
	"net"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/rpc"
	_ "go.uber.org/automaxprocs"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("mock-cqos", "Peloton mock Cqos advisor "+
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

	log.WithField("files", *configFiles).Info("Loading mock cqos config")
	var cfg Config
	if err := config.Parse(&cfg, *configFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *grpcPort != 0 {
		cfg.CqosAdvisor.GRPCPort = *grpcPort
	}

	log.WithField("config", cfg).Info("Loaded mock cqos configuration")

	// Create GRPC inbounds
	gt := rpc.NewTransport()
	gl, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		log.WithError(err).Fatal("failed to listen to gRPC port")
	}
	inbounds := []transport.Inbound{
		gt.NewInbound(gl),
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:     "mock-cqos",
		Inbounds: inbounds,
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  yarpc.UnaryInboundMiddleware(),
			Oneway: yarpc.OnewayInboundMiddleware(),
			Stream: yarpc.StreamInboundMiddleware(),
		},
	})

	// Start YARPC dispatcher.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	// Block the main process.
	select {}
}
