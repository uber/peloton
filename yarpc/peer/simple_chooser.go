// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package peer

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"
	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/peer/hostport"
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
func NewSimpleChooser(role string, transport peer.Transport) Chooser {
	return &simpleChooser{
		role:      role,
		transport: transport,
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

// UpdatePeer updates the current peer address to the given host:port
func (c *simpleChooser) UpdatePeer(hostPort string) error {
	c.Lock()
	defer c.Unlock()

	if c.p != nil {
		c.transport.ReleasePeer(hostport.PeerIdentifier(c.p.Identifier()), c)
	}

	var err error
	peer := hostport.PeerIdentifier(hostPort)
	if c.p, err = c.transport.RetainPeer(peer, c); err != nil {
		return err
	}
	log.WithFields(log.Fields{
		"role": c.role,
		"peer": c.p,
	}).Info("Updated peer to new address")
	return nil
}

func (c *simpleChooser) NotifyStatusChanged(id peer.Identifier) {}
