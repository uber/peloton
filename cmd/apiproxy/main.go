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
	"os"

	"github.com/uber/peloton/pkg/apiproxy"
	"github.com/uber/peloton/pkg/auth"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
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

	httpPort = app.Flag(
		"http-port", "API Proxy HTTP port (apiproxy.http_port override) "+
			"(set $PORT to override)").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "API Proxy gRPC port (apiproxy.grpc_port override) "+
			"(set $PORT to override)").
		Envar("GRPC_PORT").
		Int()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	authType = app.Flag(
		"auth-type",
		"Define the auth type used, default to NOOP").
		Default("NOOP").
		Envar("AUTH_TYPE").
		Enum("NOOP", "BASIC")

	authConfigFile = app.Flag(
		"auth-config-file",
		"config file for the auth feature, which is specific to the auth type used").
		Default("").
		Envar("AUTH_CONFIG_FILE").
		String()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	// Setup logging.
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

	// Load and override API Proxy configurations.
	log.WithField("files", *cfgFiles).Info("Loading API Proxy config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	if *httpPort != 0 {
		cfg.APIProxy.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.APIProxy.GRPCPort = *grpcPort
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	// Parse and setup Peloton authentication.
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	log.WithField("config", cfg).Info("Loaded API Proxy configuration")

	// Configure tally metrics.
	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonAPIProxy,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	// Configure log and version handlers.
	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel))

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	// Create both HTTP and GRPC inbounds.
	inbounds := rpc.NewInbounds(
		cfg.APIProxy.HTTPPort,
		cfg.APIProxy.GRPCPort,
		mux,
	)

	// All leader discovery metrics share a scope (and will be tagged
	// with role={role}).
	discoveryScope := rootScope.SubScope("discovery")

	// Setup the discovery service to detect host leaders and
	// configure the YARPC Peer dynamically.
	t := rpc.NewTransport()

	// Setup hostmgr peer chooser.
	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		discoveryScope,
		common.HostManagerRole,
		t,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()

	// Create hostmgr outbound.
	hostmgrOutbound := t.NewOutbound(hostmgrPeerChooser)

	// Add all required outbounds.
	outbounds := yarpc.Outbounds{
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	// Create security manager for inbound authentication middleware.
	securityManager, err := auth_impl.CreateNewSecurityManager(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not enable security feature")
	}

	// Setup inbound rate limit middleware.
	rateLimitMiddleware, err := inbound.NewRateLimitInboundMiddleware(cfg.RateLimit)
	if err != nil {
		log.WithError(err).
			Fatal("Could not create rate limit middleware")
	}

	// Setup inbound authentication middleware.
	authInboundMiddleware := inbound.NewAuthInboundMiddleware(securityManager)

	// Create security client for outbound authentication middleware.
	securityClient, err := auth_impl.CreateNewSecurityClient(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not establish secure inter-component communication")
	}

	// Setup outbound authentication middleware.
	authOutboundMiddleware := outbound.NewAuthOutboundMiddleware(securityClient)

	// Create YARPC dispatcher.
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonAPIProxy,
		Inbounds:  inbounds,
		Outbounds: outbounds,
		Metrics: yarpc.MetricsConfig{
			Tally: rootScope,
		},
		InboundMiddleware: yarpc.InboundMiddleware{
			Unary:  yarpc.UnaryInboundMiddleware(rateLimitMiddleware, authInboundMiddleware),
			Stream: yarpc.StreamInboundMiddleware(rateLimitMiddleware, authInboundMiddleware),
			Oneway: yarpc.OnewayInboundMiddleware(rateLimitMiddleware, authInboundMiddleware),
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authOutboundMiddleware,
			Stream: authOutboundMiddleware,
			Oneway: authOutboundMiddleware,
		},
	})

	// Register host service procedures in dispatcher.
	dispatcher.Register(apiproxy.BuildHostServiceProcedures(hostmgrOutbound))

	// Start YARPC dispatcher.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}
	defer dispatcher.Stop()

	// Start collecting runtime metrics。
	defer metrics.StartCollectingRuntimeMetrics(
		rootScope,
		cfg.Metrics.RuntimeMetrics.Enabled,
		cfg.Metrics.RuntimeMetrics.CollectInterval)()

	// Block the main process.
	select {}
}
