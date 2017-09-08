package models

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
)

// NewOffer will create a placement offer from a host manager offer and all the resource manager tasks on it.
func NewOffer(hostOffer *hostsvc.HostOffer, tasks []*resmgr.Task, claimed time.Time) *Offer {
	return &Offer{
		offer:   hostOffer,
		tasks:   tasks,
		claimed: claimed,
	}
}

// Offer represents a Peloton offer and the tasks running on it and a Mimir placement group also be obtained from it.
type Offer struct {
	offer   *hostsvc.HostOffer
	tasks   []*resmgr.Task
	claimed time.Time
	group   *placement.Group
	lock    sync.Mutex
}

// Offer will return the host offer of the offer.
func (offer *Offer) Offer() *hostsvc.HostOffer {
	return offer.offer
}

// Tasks will return the tasks running on the host of the offer.
func (offer *Offer) Tasks() []*resmgr.Task {
	return offer.tasks
}

// Group will return the Mimir group corresponding to the offer.
func (offer *Offer) Group() *placement.Group {
	offer.lock.Lock()
	defer offer.lock.Unlock()
	if offer.group != nil {
		return offer.group
	}
	offer.group = OfferToGroup(offer.offer)
	return offer.group
}

// Age will return the age of the offer, which is the time since it was dequeued from the host manager.
func (offer *Offer) Age(now time.Time) time.Duration {
	return now.Sub(offer.claimed)
}
