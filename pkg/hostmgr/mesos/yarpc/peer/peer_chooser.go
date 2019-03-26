package peer

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/hostport"
)

// discoverFunc signature of the choosers discovery callback.
type discoverFunc func(role string) (*url.URL, error)

// NewPeerChooser create a new Chooser for dynamically updating the remote peer.
// When called, discover will be called once to set initial peer, such that
// if the discover callback succeeds, it's guaranteed to have a peer.
func NewPeerChooser(transport peer.Transport, discoverInterval time.Duration, f discoverFunc, r string) *PeerChooser {
	c := &PeerChooser{
		transport:        transport,
		discoverInterval: discoverInterval,
		discover:         f,
		role:             r,
	}

	c.performDiscover()

	return c
}

// PeerChooser is a yarpc-compatible Chooser, that periodically updates the peer
// url, by invoking the discover callback.
type PeerChooser struct {
	sync.Mutex

	p                peer.Peer
	transport        peer.Transport
	discoverInterval time.Duration
	role             string
	discover         discoverFunc
	stopChan         chan struct{}
	url              atomic.Value
}

func (c *PeerChooser) performDiscover() error {
	peerURL, err := c.discover(c.role)
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Warn("Failed to discover peer")
		return err
	}

	// Store using atomic instead of mutex, so we don't deadlock on start/stop.
	c.url.Store(peerURL)

	url, ok := c.url.Load().(*url.URL)
	if !ok {
		return fmt.Errorf("no remote peer available")
	}

	c.Lock()
	defer c.Unlock()

	if c.p != nil && c.p.Identifier() == url.Host {
		return nil
	}

	p, err := c.transport.RetainPeer(hostport.PeerIdentifier(url.Host), c)
	if err != nil {
		return err
	}

	c.updatePeer(p)

	return nil
}

func (c *PeerChooser) updatePeer(p peer.Peer) {
	if c.p != nil {
		c.transport.ReleasePeer(hostport.PeerIdentifier(c.p.Identifier()), c)
		c.p = nil
	}

	c.p = p
}

func (c *PeerChooser) run(stop <-chan struct{}) {
	t := time.NewTicker(c.discoverInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			c.performDiscover()

		case <-stop:
			return
		}
	}
}

// Start starts a peer chooser
func (c *PeerChooser) Start() error {
	log.Debug("Starting peer chooser")
	c.Lock()
	defer c.Unlock()
	if c.stopChan != nil {
		return errors.New("already started")
	}
	c.stopChan = make(chan struct{})
	go c.run(c.stopChan)
	return nil
}

// Stop stops a peer chooser
func (c *PeerChooser) Stop() error {
	c.Lock()
	defer c.Unlock()
	if c.stopChan == nil {
		return errors.New("already stopped")
	}
	log.Debug("Stopping peer chooser")
	close(c.stopChan)
	return nil
}

// IsRunning returns true if a peer chooser is running
func (c *PeerChooser) IsRunning() bool {
	c.Lock()
	defer c.Unlock()
	return c.stopChan != nil
}

// Choose returns a peer
func (c *PeerChooser) Choose(ctx context.Context, req *transport.Request) (peer.Peer, func(error), error) {
	c.Lock()
	defer c.Unlock()

	if c.p != nil {
		return c.p, func(error) {}, nil
	}
	return nil, nil, fmt.Errorf("no remote peer available for request")
}

// NotifyStatusChanged receives notifications from the transport when the peer
// connects, disconnects, accepts a request, and so on.
func (c *PeerChooser) NotifyStatusChanged(id peer.Identifier) {}
