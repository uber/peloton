package models

import (
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"github.com/stretchr/testify/assert"
	"testing"
)

// setupHosts sets up the host info and task which all tests will use
func setupHosts() (*Host, *resmgr.Task, *hostsvc.HostInfo) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	hostInfo := &hostsvc.HostInfo{
		Hostname: "hostname",
	}
	host := NewHosts(hostInfo, []*resmgr.Task{resmgrTask})

	return host, resmgrTask, hostInfo
}

func TestHosts(t *testing.T) {
	host, _, _ := setupHosts()
	assert.Equal(t, "hostname", host.GetHost().Hostname)
}

func TestHosts_Tasks(t *testing.T) {
	host, task, _ := setupHosts()
	assert.Equal(t, task.Name, host.GetTasks()[0].Name)
}
