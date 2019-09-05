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
	"net/http"
	"os"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	"github.com/uber/peloton/.gen/thrift/aurora/api/auroraschedulermanagerserver"
	"github.com/uber/peloton/.gen/thrift/aurora/api/readonlyschedulerserver"
	"github.com/uber/peloton/pkg/aurorabridge/cache"
	auth_impl "github.com/uber/peloton/pkg/auth/impl"

	"github.com/uber/peloton/pkg/aurorabridge"
	bridgecommon "github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/auth"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/buildversion"
	"github.com/uber/peloton/pkg/common/config"
	"github.com/uber/peloton/pkg/common/health"
	"github.com/uber/peloton/pkg/common/leader"
	"github.com/uber/peloton/pkg/common/logging"
	"github.com/uber/peloton/pkg/common/metrics"
	"github.com/uber/peloton/pkg/common/rpc"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/peer"
	"github.com/uber/peloton/pkg/middleware/inbound"
	"github.com/uber/peloton/pkg/middleware/outbound"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	version string
	app     = kingpin.New("peloton-aurorabridge", "Peloton Aurora bridge")

	enableSentry = app.Flag(
		"enable-sentry", "enable logging hook up to sentry").
		Default("false").
		Envar("ENABLE_SENTRY_LOGGING").
		Bool()

	cfgFiles = app.Flag(
		"config",
		"YAML config files (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	datacenter = app.Flag(
		"datacenter", "Datacenter name").
		Default("").
		Envar("DATACENTER").
		String()

	httpPort = app.Flag(
		"http-port", "Aurora Bridge HTTP port (aurorabridge.http_port override) "+
			"(set $PORT to override)").
		Default("5396").
		Envar("HTTP_PORT").
		Int()

	grpcPort = app.Flag(
		"grpc-port", "Aurora Bridge gRPC port (aurorabridge.grpc_port override) "+
			"(set $PORT to override)").
		Envar("GRPC_PORT").
		Int()

	respoolPath = app.Flag(
		"respool-path", "Aurora Bridge Resource Pool path").
		Envar("RESPOOL_PATH").
		String()

	gpuRespoolPath = app.Flag(
		"gpu-respool-path", "Aurora Bridge GPU Resource Pool path").
		Envar("GPU_RESPOOL_PATH").
		String()

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

	enableInPlace = app.Flag(
		"enable-inplace-update", "enable in-place update").
		Default("false").
		Envar("ENABLE_INPLACE_UPDATE").
		Bool()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(
		&logging.LogFieldFormatter{
			Formatter: &log.JSONFormatter{},
			Fields: log.Fields{
				common.AppLogField: app.Name,
			},
		},
	)

	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.Fatalf("Error parsing yaml config: %s", err)
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	if *httpPort != 0 {
		cfg.HTTPPort = *httpPort
	}

	if *grpcPort != 0 {
		cfg.GRPCPort = *grpcPort
	}

	if len(*respoolPath) > 0 {
		cfg.RespoolLoader.RespoolPath = *respoolPath
	}

	if len(*gpuRespoolPath) > 0 {
		cfg.RespoolLoader.GPURespoolPath = *gpuRespoolPath
	}

	// Parse and setup peloton auth
	if len(*authType) != 0 {
		cfg.Auth.AuthType = auth.Type(*authType)
		cfg.Auth.Path = *authConfigFile
	}

	if *enableInPlace {
		cfg.ServiceHandler.EnableInPlace = true
	}

	initialLevel := log.InfoLevel
	if cfg.Debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)

	log.WithField("config", cfg).Info("Loaded AuroraBridge configuration")

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonAuroraBridge,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(
		logging.LevelOverwrite,
		logging.LevelOverwriteHandler(initialLevel))

	mux.HandleFunc(buildversion.Get, buildversion.Handler(version))

	// Create both HTTP and GRPC inbounds
	inbounds := rpc.NewAuroraBridgeInbounds(
		cfg.HTTPPort,
		cfg.GRPCPort, // dummy grpc port for aurora bridge
		mux)

	discovery, err := leader.NewZkServiceDiscovery(
		cfg.Election.ZKServers, cfg.Election.Root)
	if err != nil {
		log.WithError(err).
			Fatal("Could not create zk service discovery")
	}

	clientRecvOption := grpc.ClientMaxRecvMsgSize(cfg.EventPublisher.GRPCMsgSize)
	serverRecvOption := grpc.ServerMaxRecvMsgSize(cfg.EventPublisher.GRPCMsgSize)

	t := grpc.NewTransport(
		clientRecvOption,
		serverRecvOption,
	)

	outbounds := yarpc.Outbounds{
		common.PelotonJobManager: transport.Outbounds{
			Unary: t.NewOutbound(
				peer.NewPeerChooser(t, 1*time.Second, discovery.GetAppURL, common.JobManagerRole),
			),
			Stream: t.NewOutbound(
				peer.NewPeerChooser(t, 1*time.Second, discovery.GetAppURL, common.JobManagerRole),
			),
		},
		common.PelotonResourceManager: transport.Outbounds{
			Unary: t.NewOutbound(
				peer.NewPeerChooser(t, 1*time.Second, discovery.GetAppURL, common.ResourceManagerRole),
			),
		},
	}

	securityManager, err := auth_impl.CreateNewSecurityManager(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not enable security feature")
	}

	rateLimitMiddleware, err := inbound.NewRateLimitInboundMiddleware(cfg.RateLimit)
	if err != nil {
		log.WithError(err).
			Fatal("Could not create rate limit middleware")
	}

	authInboundMiddleware := inbound.NewAuthInboundMiddleware(securityManager)

	securityClient, err := auth_impl.CreateNewSecurityClient(&cfg.Auth)
	if err != nil {
		log.WithError(err).
			Fatal("Could not establish secure inter-component communication")
	}

	authOutboundMiddleware := outbound.NewAuthOutboundMiddleware(securityClient)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonAuroraBridge,
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

	jobClient := statelesssvc.NewJobServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonJobManager))

	jobmgrClient := jobmgrsvc.NewJobManagerServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonJobManager))

	podClient := podsvc.NewPodServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonJobManager))

	respoolClient := respool.NewResourceManagerYARPCClient(
		dispatcher.ClientConfig(common.PelotonResourceManager))

	watchClient := watchsvc.NewWatchServiceYARPCClient(
		dispatcher.ClientConfig(common.PelotonJobManager))

	// Start the dispatcher before we register the aurorabridge handler, since we'll
	// need to make some outbound requests to get things setup.
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	eventPublisher := aurorabridge.NewEventPublisher(
		cfg.EventPublisher.KafkaURL,
		jobClient,
		podClient,
		watchClient,
		&http.Client{},
		cfg.EventPublisher.PublishEvents,
	)

	server, err := aurorabridge.NewServer(
		cfg.HTTPPort,
		cfg.Election,
		eventPublisher,
		common.PelotonAuroraBridgeRole,
	)
	if err != nil {
		log.Fatalf("Unable to create server: %v", err)
	}

	candidate, err := leader.NewCandidate(
		cfg.Election,
		rootScope,
		common.PelotonAuroraBridgeRole,
		server,
	)
	if err != nil {
		log.Fatalf("Unable to create leader candidate: %v", err)
	}

	respoolLoader := aurorabridge.NewRespoolLoader(cfg.RespoolLoader, respoolClient)

	handler, err := aurorabridge.NewServiceHandler(
		cfg.ServiceHandler,
		rootScope,
		jobClient,
		jobmgrClient,
		podClient,
		respoolLoader,
		bridgecommon.RandomImpl{},
		cache.NewJobIDCache(),
	)
	if err != nil {
		log.Fatalf("Unable to create service handler: %v", err)
	}

	dispatcher.Register(auroraschedulermanagerserver.New(handler))
	dispatcher.Register(readonlyschedulerserver.New(handler))

	if err := candidate.Start(); err != nil {
		log.Fatalf("Unable to start leader candidate: %v", err)
	}
	defer candidate.Stop()

	log.WithFields(log.Fields{
		"httpPort": cfg.HTTPPort,
	}).Info("Started Aurora Bridge")

	// we can *honestly* say the server is booted up now
	health.InitHeartbeat(rootScope, cfg.Health, candidate)

	select {}
}
