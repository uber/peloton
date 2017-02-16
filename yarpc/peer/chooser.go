package peer

import (
	"context"
	"net/url"
	"sync"

	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc/peer"
	"go.uber.org/yarpc/peer/hostport"
	"go.uber.org/yarpc/transport"
	"go.uber.org/yarpc/transport/http"
)

// Chooser is the interface for a YARPC endpoint that can be updated dynamically.
// This is an implementation of go.uber.org/yarpc/peer/Chooser
type Chooser interface {
	Start() error
	Stop() error
	Choose(context.Context, *transport.Request) (peer.Peer, error)
	UpdatePeer(urlString string) error
}

type chooser struct {
	sync.Mutex
	p         peer.Peer
	role      string
	transport peer.Transport
}

// NewPeerChooser creates a new Chooser
// role is the string identifier for what this peer represents (mesos-master, hostmgr, etc)
func NewPeerChooser(role string, opts ...http.TransportOption) Chooser {
	return &chooser{
		role:      role,
		transport: http.NewTransport(opts...),
	}
}

// Start interface method. No-op
func (c *chooser) Start() error {
	return nil
}

// Stop interface method. No-op
func (c *chooser) Stop() error {
	return nil
}

// Choose is called when a request is sent. See go.uber.org/yarpc/transport/http/outbound.
// Here it returns the current peer (the leader peloton master).
func (c *chooser) Choose(context.Context, *transport.Request) (peer.Peer, error) {
	c.Lock()
	defer c.Unlock()
	return c.p, nil
}

// UpdatePeer updates the current url for app
func (c *chooser) UpdatePeer(urlString string) error {
	c.Lock()
	defer c.Unlock()
	log.Infof("Updating %v peer address to %v", c.role, urlString)
	url, err := url.Parse(urlString)
	if err != nil {
		log.Errorf("Failed to parse url %v, err = %v", urlString, err)
		return err
	}
	c.p = hostport.NewPeer(hostport.PeerIdentifier(url.Host), c.transport)
	log.Infof("New %v peer is %v", c.role, c.p)
	return nil
}
