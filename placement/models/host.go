package models

import (
	"sync"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
)

// NewHost will create a placement host from a host manager host and all the resource manager tasks on it.
func NewHost(hostOffer *hostsvc.HostOffer, tasks []*resmgr.Task, claimed time.Time) *Host {
	return &Host{
		offer:   hostOffer,
		tasks:   tasks,
		claimed: claimed,
	}
}

// Host represents a Peloton host and the tasks running on it and a Mimir placement group also be obtained from it.
type Host struct {
	// host offer of the host.
	offer *hostsvc.HostOffer
	// tasks running on the host.
	tasks []*resmgr.Task
	// claimed is the time when the host was acquired from the host manager.
	claimed time.Time
	// data is used by placement strategies to transfer state between calls to the
	// place once method.
	data interface{}
	lock sync.Mutex
}

// Offer will return the host offer of the host.
func (host *Host) Offer() *hostsvc.HostOffer {
	return host.offer
}

// Tasks will return the tasks running on the host of the host.
func (host *Host) Tasks() []*resmgr.Task {
	return host.tasks
}

// SetData will set the data transfer object on the host.
func (host *Host) SetData(data interface{}) {
	host.lock.Lock()
	defer host.lock.Unlock()
	host.data = data
}

// Data will return the data transfer object of the host.
func (host *Host) Data() interface{} {
	host.lock.Lock()
	defer host.lock.Unlock()
	return host.data
}

// Age will return the age of the host, which is the time since it was dequeued from the host manager.
func (host *Host) Age(now time.Time) time.Duration {
	return now.Sub(host.claimed)
}
