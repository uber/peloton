package models

import (
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
	"github.com/stretchr/testify/assert"
)

func setupOfferVariables() (time.Time, *hostsvc.HostOffer, *resmgrsvc.Gang, *resmgr.Task, *Offer, *Task, *Assignment) {
	resmgrTask := &resmgr.Task{
		Name: "task",
	}
	hostOffer := &hostsvc.HostOffer{
		Hostname: "hostname",
	}
	now := time.Now()
	offer := NewOffer(hostOffer, []*resmgr.Task{resmgrTask}, now)
	resmgrGang := &resmgrsvc.Gang{
		Tasks: []*resmgr.Task{
			resmgrTask,
		},
	}
	task := NewTask(resmgrGang, resmgrTask, now.Add(5*time.Second), 3)
	assignment := NewAssignment(task)
	return now, hostOffer, resmgrGang, resmgrTask, offer, task, assignment
}

func TestOffer_Offer(t *testing.T) {
	_, hostOffer, _, _, offer, _, _ := setupOfferVariables()
	assert.Equal(t, hostOffer, offer.Offer())
}

func TestOffer_Tasks(t *testing.T) {
	_, _, resmgrGang, _, offer, _, _ := setupOfferVariables()
	assert.Equal(t, resmgrGang.GetTasks(), offer.Tasks())
}

func TestOffer_Group(t *testing.T) {
	_, _, _, _, offer, _, _ := setupOfferVariables()
	assert.NotNil(t, offer.Group())
	assert.NotNil(t, offer.Group())
}

func TestOffer_Age(t *testing.T) {
	now, _, _, _, offer, _, _ := setupOfferVariables()
	assert.Equal(t, time.Duration(0), offer.Age(now))
	assert.Equal(t, 2*time.Second, offer.Age(now.Add(2*time.Second)))
}
