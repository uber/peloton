package engine

import (
	"code.uber.internal/infra/peloton/archiver"
	"code.uber.internal/infra/peloton/cli"
	"code.uber.internal/infra/peloton/leader"

	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

// Engine defines the interface used to query a peloton component
// for data and then archive that data to secondary storage using
// message queue
type Engine interface {
	// Start starts the archiver goroutines
	Start() error
	// Stop stops the archiver goroutines
	Stop() error
}

type engine struct {
	client  *cli.Client
	config  archiver.Config
	metrics *Metrics
}

// New creates a new Archiver Engine.
func New(
	zkCfg leader.ElectionConfig,
	config archiver.Config,
	parent tally.Scope,
	debug bool,
) (Engine, error) {
	config.Normalize()
	discovery, err := leader.NewZkServiceDiscovery(zkCfg.ZKServers, zkCfg.Root)
	if err != nil {
		log.WithError(err).
			Error("Could not create zk service discovery")
		return nil, err
	}

	client, err := cli.New(discovery, config.PelotonClientTimeout, debug)
	if err != nil {
		log.WithError(err).
			Fatal("Could not create peloton client")
		return nil, err
	}

	return &engine{
		client:  client,
		config:  config,
		metrics: NewMetrics(parent.SubScope("archiver")),
	}, nil
}

// Start starts archiver thread
func (e *engine) Start() error {
	log.Info("archiver.Engine started")
	// TODO (adityacb):
	// for {
	// time.Sleep(ArchiveInterval)
	// Make JobQuery API call to get last MaxArchiveEntries entries
	// of completed jobs that are older than ArchiveAge from DB
	// Stream these entries out to a message queue (abstraction for Kafka)
	// }
	return nil
}

// Stop stops archiver thread
func (e *engine) Stop() error {
	log.Info("archiver.Engine stopped")
	e.client.Cleanup()
	return nil
}
