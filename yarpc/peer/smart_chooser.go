package peer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"code.uber.internal/infra/peloton/leader"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
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
	transport peer.Transport) (Chooser, error) {
	sc := smartChooser{
		chooser:  NewSimpleChooser(role, transport),
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

// UpdatePeer updates the current peer address of leader
func (c *smartChooser) UpdatePeer(peer string) error {
	id := leader.ID{}
	if err := json.Unmarshal([]byte(peer), &id); err != nil {
		log.WithField("leader", peer).Error("Failed to parse leader json")
		return err
	}
	log.WithFields(log.Fields{
		"role": c.role,
		"peer": id,
	}).Info("Updating peer with the new leader address")

	hostPort := fmt.Sprintf("%s:%d", id.IP, id.GRPCPort)
	return c.chooser.UpdatePeer(hostPort)
}
