package peer

import (
	"context"
	"errors"
	"sync"

	"code.uber.internal/infra/peloton/leader"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
)

type smartChooser struct {
	sync.Mutex
	chooser  Chooser
	running  bool
	role     string
	observer leader.Observer
}

// NewSmartChooser creates a new SmartChooser with dynamic peer update support.
// It embeds a peer.chooser, but includes the ability to react to leadership
// changes in zookeeper and reconfigure the peer
func NewSmartChooser(
	cfg leader.ElectionConfig,
	scope tally.Scope,
	role string,
	opts ...http.TransportOption) (Chooser, error) {
	sc := smartChooser{
		chooser:  NewSimpleChooser(role, opts...),
		role:     role,
		observer: nil,
	}

	observer, err := leader.NewObserver(
		cfg,
		scope.SubScope("discovery"),
		role,
		func(leader string) error {
			log.WithFields(log.Fields{"leader": leader, "role": sc.role}).
				Info("New leader observed; updating peer")
			return sc.UpdatePeer(leader)
		},
	)
	if err != nil {
		return nil, err
	}
	sc.observer = observer
	return &sc, nil
}

// Start interface method will start the observer and respond to leadership
// election changes
func (c *smartChooser) Start() error {
	c.Lock()
	defer c.Unlock()
	if c.running {
		return errors.New("Already started")
	}
	log.WithFields(log.Fields{"role": c.role}).Debug("Starting peer chooser")
	c.running = true
	c.observer.Start()
	return nil
}

// Stop interface method will stop the observer and no longer respond to
// leadership election changes
func (c *smartChooser) Stop() error {
	c.Lock()
	defer c.Unlock()
	if !c.running {
		return errors.New("Already stopped")
	}
	log.WithFields(log.Fields{"role": c.role}).Debug("Stopping peer chooser")
	c.running = false
	c.observer.Stop()
	return nil
}

// IsRunning interface method will return true if it's running.
func (c *smartChooser) IsRunning() bool {
	c.Lock()
	defer c.Unlock()
	return c.running
}

// Choose is called when a request is sent. See
// go.uber.org/yarpc/transport/http/outbound. Here it returns the current peer
// (the leader peloton master).
func (c *smartChooser) Choose(
	ctx context.Context,
	req *transport.Request) (peer.Peer, func(error), error) {
	return c.chooser.Choose(ctx, req)
}

// UpdatePeer updates the current url
func (c *smartChooser) UpdatePeer(urlString string) error {
	return c.chooser.UpdatePeer(urlString)
}
