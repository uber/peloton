package prune

import "github.com/uber-go/atomic"

// HostPruner is the interface to prune hosts set to PlacingOffer status for
// too long by resetting status to ReadyOffer
type HostPruner interface {
	Prune(_ *atomic.Bool)
}
