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

var (
	events = make(chan string, 100)
)

type testComponent struct {
	host string
	port string
}

func (x testComponent) GainedLeadershipCallback() error {
	log.Info("GainedLeadershipCallback called")
	events <- "leadership_gained"
	return nil
}
func (x testComponent) LostLeadershipCallback() error {
	log.Info("LostLeadershipCallback called")
	events <- "leadership_lost"
	return nil
}
func (x testComponent) NewLeaderCallback(leader string) error {
	log.Info("NewLeaderCallback called")
	events <- "new_leader"
	return nil
}
func (x testComponent) ShutDownCallback() error {
	log.Info("ShutdownCallback called")
	events <- "shutdown"
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

	el := election{
		role:       role,
		metrics:    newElectionMetrics(tally.NoopScope, "role", "testhost:666"),
		candidate:  leadership.NewCandidate(mockStore, key, "testhost:666", ttl),
		nomination: testComponent{host: "testhost", port: "666"},
	}

	log.Info("About to start")
	err = el.Start()
	log.Info("started")
	assert.NoError(t, err)

	// Should issue a false upon start, no matter what.
	assert.Equal(t, "leadership_lost", <-events)

	// Since the lock always succeeeds, we should get elected.
	assert.Equal(t, "leadership_gained", <-events)
	assert.Equal(t, el.IsLeader(), true)

	// When we resign, unlock will get called, we'll be notified of the
	// de-election and we'll try to get the lock again.
	go el.Resign()
	assert.Equal(t, "leadership_lost", <-events)
	assert.Equal(t, "leadership_gained", <-events)
	assert.Equal(t, true, el.IsLeader())

	log.Info("about to stop election")
	err = el.Stop()
	log.Info("stopped election")
	assert.NoError(t, err)
	// make sure abdicating triggers shutdown handler
	assert.Equal(t, "shutdown", <-events)
	// and then you are no longer leader
	assert.Equal(t, false, el.IsLeader())

}
