package models

import (
	"sync"

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
)

// NewHosts will create a placement host from a host manager host and all the resource manager tasks on it.
func NewHosts(hostInfo *hostsvc.HostInfo, tasks []*resmgr.Task) *Host {
	return &Host{
		Host:  hostInfo,
		Tasks: tasks,
	}
}

// Host represents a Peloton hostinfo from hostmanager and the tasks running on it.
type Host struct {
	// mutex
	lock sync.Mutex

	// host info from host manager
	Host *hostsvc.HostInfo `json:"hostinfo"`
	// tasks running on the host.
	Tasks []*resmgr.Task `json:"tasks"`
}

// GetHost returns the host info of the host.
func (h *Host) GetHost() *hostsvc.HostInfo {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.Host
}

// GetTasks returns the tasks of the host.
func (h *Host) GetTasks() []*resmgr.Task {
	h.lock.Lock()
	defer h.lock.Unlock()
	return h.Tasks
}
