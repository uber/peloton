package models

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/stretchr/testify/assert"
)

func setupAssignmentVariables() (*hostsvc.HostOffer, *resmgrsvc.Gang, *resmgr.Task, *Host, *Task, *Assignment) {
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
	return hostOffer, resmgrGang, resmgrTask, offer, task, assignment
}

func TestAssignment_Task(t *testing.T) {
	_, _, _, _, task, assignment := setupAssignmentVariables()
	assert.Equal(t, task, assignment.Task())
}

func TestAssignment_Offer(t *testing.T) {
	_, _, _, _, _, assignment := setupAssignmentVariables()
	assert.Nil(t, assignment.Host())
}

func TestAssignment_SetOffer(t *testing.T) {
	_, _, _, offer, _, assignment := setupAssignmentVariables()
	assignment.SetHost(offer)
	assert.Equal(t, offer, assignment.Host())
}
