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

// Chooser holds the peloton manager master RPC server address
// It is updated via zk leader change callback by all peloton masters
// when there is a leader change. It implements go.uber.org/yarpc/peer/Chooser
type Chooser struct {
	p         peer.Peer
	transport peer.Transport
	mutex     *sync.Mutex
}

// NewPeerChooser creates a new Chooser
func NewPeerChooser(opts ...http.TransportOption) *Chooser {
	return &Chooser{
		mutex:     &sync.Mutex{},
		transport: http.NewTransport(opts...),
	}
}

// Start interface method. No-op
func (c *Chooser) Start() error {
	return nil
}

// Stop interface method. No-op
func (c *Chooser) Stop() error {
	return nil
}

// Choose is called when a request is sent. See go.uber.org/yarpc/transport/http/outbound.
// Here it returns the current peer (the leader peloton master).
func (c *Chooser) Choose(context.Context, *transport.Request) (peer.Peer, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.p, nil
}

// UpdatePeer updates the current url for component
func (c *Chooser) UpdatePeer(urlString string, component string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Infof("Updating %v peer address to %v", component, urlString)
	url, err := url.Parse(urlString)
	if err != nil {
		log.Errorf("Failed to parse url %v, err = %v", urlString, err)
		return err
	}
	c.p = hostport.NewPeer(hostport.PeerIdentifier(url.Host), c.transport)
	log.Infof("New %v peer is %v", component, c.p)
	return nil
}
