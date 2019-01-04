package plugins

import (
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/placement/models"
)

// Strategy is a placment strategy that will do all the placement logic of
// assigning tasks to offers.
type Strategy interface {
	// PlaceOnce takes a list of assignments without any assigned offers and
	// will assign offers to the task in each assignment.
	PlaceOnce(assignments []*models.Assignment, hosts []*models.HostOffers)

	// Filters will take a list of assignments and group them into groups that
	// should use the same host filter to acquire offers from the host manager.
	Filters(assignments []*models.Assignment) map[*hostsvc.HostFilter][]*models.Assignment

	// ConcurrencySafe returns true iff the strategy is concurrency safe. If
	// the strategy is concurrency safe then it is safe for multiple
	// go-routines to run the PlaceOnce method concurrently, else only one
	// go-routine is allowed to run the PlaceOnce method at a time.
	ConcurrencySafe() bool
}
