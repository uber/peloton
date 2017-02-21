package main

import (
	"net/url"
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/config"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/placement"
	placementconfig "code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/yarpc/peer"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
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

	zkPath = app.
		Flag("zk-path", "Zookeeper path (mesos.zk_host override) (set $MESOS_ZK_PATH to override)").
		Envar("MESOS_ZK_PATH").
		String()

	dbHost = app.
		Flag("db-host", "Database host (db.host override) (set $DB_HOST to override)").
		Envar("DB_HOST").
		String()

	electionZkServers = app.
				Flag("election-zk-server", "Election Zookeeper servers. Specify multiple times for multiple servers (election.zk_servers override) (set $ELECTION_ZK_SERVERS to override)").
				Envar("ELECTION_ZK_SERVERS").
				Strings()
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	var cfg placementconfig.Config

	if err := config.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse placement engine config")
	}

	// now, override any CLI flags in the loaded config.Config
	if *zkPath != "" {
		cfg.Mesos.ZkPath = *zkPath
	}

	if *dbHost != "" {
		cfg.Storage.MySQL.Host = *dbHost
	}

	if len(*electionZkServers) > 0 {
		cfg.Election.ZKServers = *electionZkServers
	}

	rootScope, scopeCloser, _ := metrics.InitMetricScope(
		&cfg.Metrics,
		common.PelotonPlacement,
		metrics.TallyFlushInterval,
	)
	defer scopeCloser.Close()

	discoveryMetricScope := rootScope.SubScope("discovery")

	hostmgrPeerChooser, err := peer.NewSmartChooser(cfg.Election, discoveryMetricScope, common.HostManagerRole)
	if err != nil {
		log.Fatalf("Could not create smart peer chooser for %s: %v", common.HostManagerRole, err)
	}
	if err := hostmgrPeerChooser.Start(); err != nil {
		log.Fatalf("Could not start smart peer chooser for %s: %v", common.HostManagerRole, err)
	}
	defer hostmgrPeerChooser.Stop()
	hostmgrOutbound := http.NewChooserOutbound(
		hostmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointURL,
		},
	)

	resmgrPeerChooser, err := peer.NewSmartChooser(cfg.Election, discoveryMetricScope, common.ResourceManagerRole)
	if err != nil {
		log.Fatalf("Could not create smart peer chooser for %s: %v", common.ResourceManagerRole, err)
	}
	if err := resmgrPeerChooser.Start(); err != nil {
		log.Fatalf("Could not start smart peer chooser for %s: %v", common.ResourceManagerRole, err)
	}
	defer resmgrPeerChooser.Stop()
	resmgrOutbound := http.NewChooserOutbound(
		resmgrPeerChooser,
		&url.URL{
			Scheme: "http",
			Path:   common.PelotonEndpointURL,
		},
	)

	// now, attempt to setup the dispatcher
	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	log.Debug("Creating new YARPC dispatcher")
	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonPlacement,
		Outbounds: outbounds,
	})

	log.Debug("Starting YARPC dispatcher")
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Unable to start dispatcher: %v", err)
	}

	// Initialize and start placement engine
	placementEngine := placement.New(
		dispatcher,
		&cfg.Placement,
		rootScope.SubScope("placement"),
		common.PelotonResourceManager,
		common.PelotonHostManager,
	)
	placementEngine.Start()
	defer placementEngine.Stop()
	// we can *honestly* say the server is booted up now
	rootScope.Counter("boot").Inc(1)

	select {}
}
