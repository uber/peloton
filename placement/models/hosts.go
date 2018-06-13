package models

import (
	"sync"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
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
	sync.Mutex

	// host info from host manager
	Host *hostsvc.HostInfo `json:"hostinfo"`
	// tasks running on the host.
	Tasks []*resmgr.Task `json:"tasks"`
}

// GetHost returns the host info of the host.
func (h *Host) GetHost() *hostsvc.HostInfo {
	h.Lock()
	defer h.Unlock()
	return h.Host
}

// GetTasks returns the tasks of the host.
func (h *Host) GetTasks() []*resmgr.Task {
	h.Lock()
	defer h.Unlock()
	return h.Tasks
}
