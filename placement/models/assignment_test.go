package models

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	log "github.com/sirupsen/logrus"
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
	assert.Equal(t, task, assignment.GetTask())
}

func TestAssignment_Offer(t *testing.T) {
	_, _, _, _, _, assignment := setupAssignmentVariables()
	assert.Nil(t, assignment.GetHost())
}

func TestAssignment_SetOffer(t *testing.T) {
	_, _, _, host, _, assignment := setupAssignmentVariables()
	assignment.SetHost(host)
	assert.Equal(t, host, assignment.GetHost())
}

func TestTest(t *testing.T) {
	log.SetFormatter(&log.JSONFormatter{})
	initialLevel := log.DebugLevel
	log.SetLevel(initialLevel)

	_, _, _, host, _, assignment := setupAssignmentVariables()
	assignment.SetHost(host)
	entry, err := log.WithField("foo", assignment).String()
	assert.NoError(t, err)
	assert.Contains(t, entry, "foo")
	assert.Contains(t, entry, "host")
	assert.Contains(t, entry, "offer")
	assert.Contains(t, entry, "tasks")
	assert.Contains(t, entry, "claimed")
	assert.Contains(t, entry, "deadline")
	assert.Contains(t, entry, "max_rounds")
	assert.Contains(t, entry, "rounds")
}
