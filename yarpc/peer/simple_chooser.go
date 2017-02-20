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

// Choose is called when a request is sent. See go.uber.org/yarpc/transport/http/outbound.
// Here it returns the current peer (the leader peloton master).
func (c *simpleChooser) Choose(context.Context, *transport.Request) (peer.Peer, error) {
	c.Lock()
	defer c.Unlock()
	return c.p, nil
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
	c.p = hostport.NewPeer(hostport.PeerIdentifier(url.Host), c.transport)
	log.Infof("New %v peer is %v", c.role, c.p)
	return nil
}
