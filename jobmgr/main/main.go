package main

import (
	"code.uber.internal/infra/peloton/jobmgr"
	"code.uber.internal/infra/peloton/jobmgr/job"
	"code.uber.internal/infra/peloton/jobmgr/task"
	"code.uber.internal/infra/peloton/master/metrics"
	"code.uber.internal/infra/peloton/storage/mysql"
	"code.uber.internal/infra/peloton/yarpc/peer"
	"fmt"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
	"gopkg.in/alecthomas/kingpin.v2"
	nethttp "net/http"
	"os"
	"time"

	cconfig "code.uber.internal/infra/peloton/common/config"
	resmgr_config "code.uber.internal/infra/peloton/resmgr/config"
	log "github.com/Sirupsen/logrus"
	"github.com/uber-go/tally"
	"net/url"
	"runtime"
)

const (
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
	serviceName         = "peloton-jobmgr"
	resmgrServiceName   = "peloton-resmgr"
)

var (
	version string
	app     = kingpin.New(serviceName, "Peloton Job Manager")
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

	// TODO: Add metrics introspection to scope.

	// TODO: Move to jobmgr.Config
	var cfg jobmgr.Config

	if err := cconfig.Parse(&cfg, *configs...); err != nil {
		log.WithField("error", err).Fatal("Cannot parse host manager config")
	}

	// Connect to mysql DB
	if err := cfg.Storage.MySQL.Connect(); err != nil {
		log.Fatalf("Could not connect to database: %+v", err)
	}
	// TODO: fix metric scope
	store := mysql.NewJobStore(cfg.Storage.MySQL, tally.NoopScope)
	store.DB.SetMaxOpenConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetMaxIdleConns(cfg.JobManager.DbWriteConcurrency)
	store.DB.SetConnMaxLifetime(cfg.Storage.MySQL.ConnLifeTime)

	urlPath := "/jobmgr/v1"

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)

	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()
	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", cfg.JobManager.Port), http.Mux(urlPath, mux)),
	}

	resourceManagerPeerChooser := peer.NewPeerChooser()
	// TODO: change FrameworkURLPath to resource manager URL path
	// TODO: hook up with service discovery code, point to the leader of the resource manager
	pOutbound := http.NewChooserOutbound(resourceManagerPeerChooser, &url.URL{Scheme: "http", Path: resmgr_config.FrameworkURLPath})
	pOutbounds := transport.Outbounds{
		Unary: pOutbound,
	}
	outbounds := yarpc.Outbounds{
		resmgrServiceName: pOutbounds,
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      serviceName,
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Init service handler.
	// TODO: change to updated jobmgr.Config
	jobManagerMetrics := metrics.New(tally.NoopScope)
	taskManagerMetrics := metrics.New(tally.NoopScope)
	job.InitServiceHandler(dispatcher, &cfg, store, store, &jobManagerMetrics, resmgrServiceName)
	task.InitServiceHandler(dispatcher, store, store, &taskManagerMetrics)

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	select {}
}
