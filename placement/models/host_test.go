package models

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/stretchr/testify/assert"
)

func setupHostVariables() (time.Time, *hostsvc.HostOffer, *resmgrsvc.Gang, *resmgr.Task, *Host, *Task, *Assignment) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	hostOffer := &hostsvc.HostOffer{
		Hostname: "hostname",
	}
	now := time.Now()
	offer := NewHost(hostOffer, []*resmgr.Task{resmgrTask}, now)
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	task := NewTask(resmgrGang, resmgrTask, now.Add(5*time.Second), 3)
	assignment := NewAssignment(task)
	return now, hostOffer, resmgrGang, resmgrTask, offer, task, assignment
}

func TestHost_Offer(t *testing.T) {
	_, hostOffer, _, _, host, _, _ := setupHostVariables()
	assert.Equal(t, hostOffer, host.Offer())
}

func TestHost_Tasks(t *testing.T) {
	_, _, resmgrGang, _, host, _, _ := setupHostVariables()
	assert.Equal(t, resmgrGang.GetTasks(), host.Tasks())
}

func TestHost_DataAndSetData(t *testing.T) {
	_, _, _, _, host, _, _ := setupHostVariables()
	assert.Nil(t, host.Data())
	host.SetData(42)
	assert.NotNil(t, host.Data())
}

func TestHost_Age(t *testing.T) {
	now, _, _, _, host, _, _ := setupHostVariables()
	assert.Equal(t, time.Duration(0), host.Age(now))
	assert.Equal(t, 2*time.Second, host.Age(now.Add(2*time.Second)))
}
