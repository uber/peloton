package main

import (
	"fmt"
	"os"
	"time"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"

	"net/url"
	"runtime"

	cconfig "code.uber.internal/infra/peloton/common/config"
	log "github.com/Sirupsen/logrus"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
)

var (
	version string
	app     = kingpin.New(common.PelotonJobManager, "Peloton Job Manager")
	configs = app.
		Flag("config", "YAML framework configuration (can be provided multiple times to merge configs)").
		Short('c').
		Required().
		ExistingFiles()
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)

	// TODO: Move to jobmgr.Config
	var cfg jobmgr.Config

	if err := cconfig.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse host manager config")
	}

	rootScope, scopeCloser, mux := metrics.InitMetricScope(&cfg.Metrics, common.PelotonJobManager, metricFlushInterval)
	defer scopeCloser.Close()
	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// TODO: fix metric scope
	store := mysql.NewJobStore(cfg.Storage.MySQL, rootScope.SubScope("storage"))
	store.DB.SetMaxOpenConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetMaxIdleConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.JobManager.Port), http.Mux(common.PelotonEndpointURL, mux)),
	}

	// TODO: hook up with service discovery code to auto-detect the resmgr/hostmgr leader address
	resmgrPeerChooser := peer.NewPeerChooser()
	resmgrAddr := fmt.Sprintf("http://%s:%d", cfg.JobManager.ResmgrHost, cfg.JobManager.ResmgrPort)
	resmgrPeerChooser.UpdatePeer(resmgrAddr)
	// TODO: change FrameworkURLPath to resource manager URL path
	resmgrOutbound := http.NewChooserOutbound(resmgrPeerChooser, &url.URL{Scheme: "http", Path: common.PelotonEndpointURL})

	// TODO: hookup with service discovery for hostmgr leader
	// As of now, peloton-jobmgr does not talk to hostmgr, hostmgrOutbound is placeholder
	hostmgrPeerChooser := peer.NewPeerChooser()
	hostmgrOutbound := http.NewChooserOutbound(hostmgrPeerChooser, &url.URL{Scheme: "http", Path: common.PelotonEndpointURL})

	outbounds := yarpc.Outbounds{
		common.PelotonResourceManager: transport.Outbounds{
			Unary: resmgrOutbound,
		},
		common.PelotonHostManager: transport.Outbounds{
			Unary: hostmgrOutbound,
		},
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      common.PelotonJobManager,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Init service handler.
	// TODO: change to updated jobmgr.Config
	jobManagerMetrics := jobmgr.NewMetrics(rootScope)
	job.InitServiceHandler(dispatcher, &cfg, store, store, &jobManagerMetrics, common.PelotonJobManager)
	task.InitServiceHandler(dispatcher, store, store, &jobManagerMetrics)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	select {}
}
