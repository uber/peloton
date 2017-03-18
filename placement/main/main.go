package main

import (
	"fmt"
	"net/url"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/logging"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/placement"
	"code.uber.internal/infra/peloton/yarpc/peer"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

var (
	version string
	app     = kingpin.New("peloton-placement", "Peloton Placement Engine")

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

	zkPath = app.Flag(
		"zk-path",
		"Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").
		Envar("MESOS_ZK_PATH").
		String()

	electionZkServers = app.Flag(
		"election-zk-server",
		"Election Zookeeper servers. Specify multiple times for multiple servers "+
			"(election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
		Envar("ELECTION_ZK_SERVERS").
		Strings()

	placementPort = app.Flag(
		"port",
		"Placement engine port (placement.port override) (set $PORT to override)").
		Envar("PORT").
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

	log.WithField("files", *cfgFiles).Info("Loading Placement Engnine config")
	var cfg Config
	if err := config.Parse(&cfg, *cfgFiles...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse yaml config")
	}

	// now, override any CLI flags in the loaded config.Config
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}
	log.WithField("config", cfg).Info("Loaded Placement Engine config")

	if *placementPort != 0 {
		cfg.Placement.Port = *placementPort
	}

	rootScope, scopeCloser, mux := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	mux.HandleFunc(logging.LevelOverwrite, logging.LevelOverwriteHandler(initialLevel))

	hostmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.HostManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	if err := hostmgrPeerChooser.Start(); err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.HostManagerRole}).
			Fatal("Could not start smart peer chooser")
	}
	defer hostmgrPeerChooser.Stop()
	hostmgrOutbound := http.NewChooserOutbound(
		hostmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointPath,
		},
	)

	resmgrPeerChooser, err := peer.NewSmartChooser(
		cfg.Election,
		rootScope,
		common.ResourceManagerRole,
	)
	if err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not create smart peer chooser")
	}
	if err := resmgrPeerChooser.Start(); err != nil {
		log.WithFields(log.Fields{"error": err, "role": common.ResourceManagerRole}).
			Fatal("Could not start smart peer chooser")
	}
	defer resmgrPeerChooser.Stop()
	resmgrOutbound := http.NewChooserOutbound(
		resmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointPath,
		},
	)

	// Now attempt to setup the dispatcher
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	inbounds := []transport.Inbound{
		http.NewInbound(
			fmt.Sprintf(":%d", cfg.Placement.Port),
			http.Mux(common.PelotonEndpointPath, mux)),
	}

	log.Debug("Creating new YARPC dispatcher")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonPlacement,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	log.Debug("Starting YARPC dispatcher")
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start dispatcher: %v", err)
	}

	// Initialize and start placement engine
	placementEngine := placement.New(
		dispatcher,
		rootScope,
		&cfg.Placement,
		common.PelotonResourceManager,
		common.PelotonHostManager,
	)
	placementEngine.Start()
	defer placementEngine.Stop()
	// we can *honestly* say the server is booted up now
	rootScope.Counter("boot").Inc(1)

	select {}
}
