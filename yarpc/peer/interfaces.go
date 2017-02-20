package peer

import (
	"context"

	"go.uber.org/yarpc/peer"
	"go.uber.org/yarpc/transport"
)

// Chooser is the interface for a YARPC endpoint that can be updated dynamically.
// This is an implementation of go.uber.org/yarpc/peer/Chooser
type Chooser interface {
	Start() error
	Stop() error
	Choose(context.Context, *transport.Request) (peer.Peer, error)
	UpdatePeer(urlString string) error
}
