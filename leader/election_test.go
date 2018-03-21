package leader

import (
	"strings"
	"testing"

	"github.com/docker/leadership"
	libkvmock "github.com/docker/libkv/store/mock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
)

type testComponent struct {
	host   string
	port   string
	events chan string
}

func (x testComponent) GainedLeadershipCallback() error {
	log.Info("GainedLeadershipCallback called")
	x.events <- "leadership_gained"
	return nil
}
func (x testComponent) LostLeadershipCallback() error {
	log.Info("LostLeadershipCallback called")
	x.events <- "leadership_lost"
	return nil
}
func (x testComponent) NewLeaderCallback(leader string) error {
	log.Info("NewLeaderCallback called")
	x.events <- "new_leader"
	return nil
}
func (x testComponent) ShutDownCallback() error {
	log.Info("ShutdownCallback called")
	x.events <- "shutdown"
	return nil
}
func (x testComponent) GetID() string { return x.host + ":" + x.port }

func TestLeaderElection(t *testing.T) {
	// the zkservers will be replaced with the mock libkv client, dont worry :)
	role := "testrole"
	zkpath := "/peloton/fake"
	key := strings.TrimPrefix(zkpath, "/")

	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	mockStore := kv.(*libkvmock.Mock)
	mockLock := &libkvmock.Lock{}
	mockStore.On("NewLock", key, mock.Anything).Return(mockLock, nil)

	// Lock and unlock always succeeds.
	lostCh := make(chan struct{})
	var mockLostCh <-chan struct{} = lostCh
	mockLock.On("Lock", mock.Anything).Return(mockLostCh, nil)
	mockLock.On("Unlock").Return(nil)

	nomination := &testComponent{
		host:   "testhost",
		port:   "666",
		events: make(chan string, 100),
	}

	el := election{
		role:       role,
		metrics:    newElectionMetrics(tally.NoopScope, "hostname"),
		candidate:  leadership.NewCandidate(mockStore, key, "testhost:666", ttl),
		nomination: nomination,
		stopChan:   make(chan struct{}, 1),
	}

	log.Info("About to start")
	err = el.Start()
	log.Info("started")
	assert.NoError(t, err)

	// Should issue a false upon start, no matter what.
	assert.Equal(t, "leadership_lost", <-nomination.events)

	// Since the lock always succeeeds, we should get elected.
	assert.Equal(t, "leadership_gained", <-nomination.events)
	assert.Equal(t, el.IsLeader(), true)

	// When we resign, unlock will get called, we'll be notified of the
	// de-election and we'll try to get the lock again.
	go el.Resign()
	assert.Equal(t, "leadership_lost", <-nomination.events)
	assert.Equal(t, "leadership_gained", <-nomination.events)
	assert.Equal(t, true, el.IsLeader())

	log.Info("about to stop election")
	err = el.Stop()
	log.Info("stopped election")
	assert.NoError(t, err)
	// make sure abdicating triggers shutdown handler
	assert.Equal(t, "shutdown", <-nomination.events)
	// and then you are no longer leader
	assert.Equal(t, false, el.IsLeader())

}
