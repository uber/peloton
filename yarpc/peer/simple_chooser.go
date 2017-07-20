package peer

import (
	"context"
	"net/url"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/hostport"
	"go.uber.org/yarpc/transport/http"
)

type simpleChooser struct {
	sync.Mutex
	p         peer.Peer
	role      string
	transport peer.Transport
}

// NewSimpleChooser creates a new Chooser, with no bells and whistles. Just a
// peer that can be updated manually.
// role is the string identifier for what this peer represents (mesos-master, hostmgr, etc)
func NewSimpleChooser(role string, opts ...http.TransportOption) Chooser {
	return &simpleChooser{
		role:      role,
		transport: http.NewTransport(opts...),
	}
}

// Start interface method. No-op
func (c *simpleChooser) Start() error {
	return nil
}

// Stop interface method. No-op
func (c *simpleChooser) Stop() error {
	return nil
}

// IsRunning interface method. No-op
func (c *simpleChooser) IsRunning() bool {
	return true
}

// Choose is called when a request is sent. See go.uber.org/yarpc/transport/http/outbound.
// Here it returns the current peer (the leader peloton master).
func (c *simpleChooser) Choose(context.Context, *transport.Request) (peer.Peer, func(error), error) {
	c.Lock()
	defer c.Unlock()
	return c.p, func(error) {}, nil
}

// UpdatePeer updates the current url for app
func (c *simpleChooser) UpdatePeer(urlString string) error {
	c.Lock()
	defer c.Unlock()
	log.Infof("Updating %v peer address to %v", c.role, urlString)
	url, err := url.Parse(urlString)
	if err != nil {
		log.Errorf("Failed to parse url %v, err = %v", urlString, err)
		return err
	}
	if c.p != nil {
		c.transport.ReleasePeer(hostport.PeerIdentifier(c.p.Identifier()), c)
	}

	if c.p, err = c.transport.RetainPeer(hostport.PeerIdentifier(url.Host), c); err != nil {
		return err
	}
	log.Infof("New %v peer is %v", c.role, c.p)
	return nil
}

func (c *simpleChooser) NotifyStatusChanged(id peer.Identifier) {}
