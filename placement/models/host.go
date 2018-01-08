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
		Offer:   hostOffer,
		Tasks:   tasks,
		Claimed: claimed,
	}
}

// Host represents a Peloton host and the tasks running on it and a Mimir placement group also be obtained from it.
type Host struct {
	// host offer of the host.
	Offer *hostsvc.HostOffer `json:"offer"`
	// tasks running on the host.
	Tasks []*resmgr.Task `json:"tasks"`
	// Claimed is the time when the host was acquired from the host manager.
	Claimed time.Time `json:"claimed"`
	// data is used by placement strategies to transfer state between calls to the
	// place once method.
	data interface{}
	lock sync.Mutex
}

// GetOffer returns the host offer of the host.
func (host *Host) GetOffer() *hostsvc.HostOffer {
	return host.Offer
}

// SetOffer sets the host offer of the host.
func (host *Host) SetOffer(offer *hostsvc.HostOffer) {
	host.Offer = offer
}

// GetTasks returns the tasks of the host.
func (host *Host) GetTasks() []*resmgr.Task {
	return host.Tasks
}

// SetTasks sets the tasks of the host.
func (host *Host) SetTasks(tasks []*resmgr.Task) {
	host.Tasks = tasks
}

// GetClaimed returns the time when the host was claimed.
func (host *Host) GetClaimed() time.Time {
	return host.Claimed
}

// SetClaimed sets the time when the host was claimed.
func (host *Host) SetClaimed(claimed time.Time) {
	host.Claimed = claimed
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
	return now.Sub(host.Claimed)
}
