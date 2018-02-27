package leader

import (
	"strings"
	"testing"

	"github.com/docker/leadership"
	libkvmock "github.com/docker/libkv/store/mock"
	"github.com/samuel/go-zookeeper/zk"
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

func TestLeaderElectionZKDisconnection(t *testing.T) {
	// Testing ZooKeeper leader election in the scope of a disconnection
	// with ZK quorum: an elected leader should declare the leadership
	// as lost.

	// Mock ZK store
	kv, err := libkvmock.New([]string{}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)
	mockStore := kv.(*libkvmock.Mock)

	// Mock Lock
	mockLock := &libkvmock.Lock{}
	ZKkey := "peloton/fake"
	mockStore.On("NewLock", ZKkey, mock.Anything).Return(mockLock, nil)

	// Mock Lock() and Unlock() as succeeding to simulate
	// successful leadership gain
	mockLock.On("Lock", mock.Anything).Return(make(<-chan struct{}), nil)
	mockLock.On("Unlock").Return(nil)

	// ZK event channel used to listen on ZK disconnection events
	zkEventChan := make(chan zk.Event)

	nomination := &testComponent{
		host:   "testhost",
		port:   "666",
		events: make(chan string, 100),
	}

	// Define election and run election
	el := election{
		candidate:   leadership.NewCandidate(mockStore, ZKkey, "testhost:666", ttl),
		metrics:     newElectionMetrics(tally.NoopScope, "hostname"),
		nomination:  nomination,
		role:        "testrole",
		stopChan:    make(chan struct{}, 1),
		zkEventChan: zkEventChan,
	}

	// Start leader election
	err = el.Start()
	assert.NoError(t, err)

	// By default start by not being a leader
	assert.Equal(t, <-nomination.events, "leadership_lost")

	// Since the lock always succeeeds, leadership is gained
	assert.Equal(t, <-nomination.events, "leadership_gained")
	assert.Equal(t, true, el.IsLeader())

	// Simulate disconnection with ZK by sending an ZK disconnection event
	zkEventChan <- zk.Event{
		Type:  zk.EventSession,
		State: zk.StateDisconnected,
	}

	// Once ZK disconnection is detected,
	// leadership should be declared lost
	assert.Equal(t, "leadership_lost", <-nomination.events)
}
