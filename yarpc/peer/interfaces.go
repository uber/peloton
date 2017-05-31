package peer

import (
	"context"

	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
)

// Chooser is the interface for a YARPC endpoint that can be updated dynamically.
// This is an implementation of go.uber.org/yarpc/peer/Chooser
type Chooser interface {
	Start() error
	Stop() error
	Choose(context.Context, *transport.Request) (peer.Peer, func(error), error)
	IsRunning() bool
	UpdatePeer(urlString string) error
}
