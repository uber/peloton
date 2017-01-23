package main

import (
	"fmt"
	nethttp "net/http"
	"os"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"code.uber.internal/infra/peloton/hostmgr"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"

	log "github.com/Sirupsen/logrus"
)

const (
	productionEnvValue = "production"
	// metricFlushInterval is the flush interval for metrics buffered in Tally to be flushed to the backend
	metricFlushInterval = 1 * time.Second
)

var (
	version string
	app     = kingpin.New("peloton-hostmgr", "Peloton Host Manager")
)

func main() {
	app.Version(version)
	app.HelpFlag.Short('h')
	kingpin.MustParse(app.Parse(os.Args[1:]))

	// TODO(zhitao): Add metrics introspection to scope.

	// TODO(zhitao): Add leader election.

	// TODO(zhitao): Move each config to an kingpin option.
	port := 8084
	urlPath := "/hostmgr/v1"

	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.DebugLevel)

	// mux is used to mux together other (non-RPC) handlers, like metrics exposition endpoints, etc
	mux := nethttp.NewServeMux()

	inbounds := []transport.Inbound{
		http.NewInbound(fmt.Sprintf(":%d", port), http.Mux(urlPath, mux)),
	}

	outbounds := yarpc.Outbounds{
	// TODO(zhitao): Add mesos-master outbound because host manager will eventually be the only
	// component talking to mesos master.
	}

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name:      "peloton-hostmgr",
		Inbounds:  inbounds,
		Outbounds: outbounds,
	})

	// Init the managers driven by the mesos callbacks.
	// They are driven by the leader who will subscribe to
	// mesos callbacks
	hostmgr.InitManager(dispatcher)

	log.WithFields(log.Fields{
		"port":     port,
		"url_path": urlPath,
	}).Info("HostManager initialized")

	// Start dispatch loop
	if err := dispatcher.Start(); err != nil {
		log.Fatalf("Could not start rpc server: %v", err)
	}

	select {}
}
