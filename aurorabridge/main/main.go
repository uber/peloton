package main

import (
	"fmt"
	"os"

	"code.uber.internal/infra/peloton/aurorabridge"
	"code.uber.internal/infra/peloton/aurorabridge/aurora/api"
	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"github.com/apache/thrift/lib/go/thrift"
	kingpin "gopkg.in/alecthomas/kingpin.v2"

	log "github.com/sirupsen/logrus"
)

var (
	version string
	app     = kingpin.New("peloton-aurorabridge", "Peloton Aurora bridge")

	debug = app.Flag(
		"debug", "enable debug mode (print full json responses)").
		Short('d').
		Default("false").
		Envar("ENABLE_DEBUG_LOGGING").
		Bool()

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

	port = app.Flag(
		"port", "aurora api port").
		Default("8082").
		Int()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})

	initialLevel := log.InfoLevel
	if *debug {
		initialLevel = log.DebugLevel
	}
	log.SetLevel(initialLevel)

	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.Fatalf("Error parsing yaml config: %s", err)
	}

	if *enableSentry {
		logging.ConfigureSentry(&cfg.SentryConfig)
	}

	rootScope, scopeCloser, _ := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonAuroraBridge,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	addr := fmt.Sprintf(":%d", *port)

	handler := aurorabridge.NewServiceHandler(rootScope)
	processor := api.NewAuroraSchedulerManagerProcessor(handler)
	transport, err := thrift.NewTServerSocket(addr)
	if err != nil {
		log.Fatalf("Error creating thrift socket: %s", err)
	}
	server := thrift.NewTSimpleServer6(
		processor,
		transport,
		thrift.NewTTransportFactory(),                 // Input transport.
		thrift.NewTTransportFactory(),                 // Output transport.
		thrift.NewTJSONProtocolFactory(),              // Input protocol.
		thrift.NewTBinaryProtocolFactory(false, true), // Output protocol.
	)
	log.Infof("Starting Aurora server on %s...", addr)
	log.Fatal(server.Serve())
}
