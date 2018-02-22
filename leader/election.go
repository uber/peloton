package leader

import (
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
)

const (
	// ttl is the election ttl for docker/leadership
	ttl = 15 * time.Second
	// zkConnErrRetry how long to wait before restarting campaigning
	// for leadership on connection error
	zkConnErrRetry = 1 * time.Second
	// _metricsUpdateTick is the period between consecutive emissions
	// of leader election metrics
	_metricsUpdateTick = 10 * time.Second
)

// ElectionConfig is config related to leader election of this service.
type ElectionConfig struct {
	// A comma separated list of ZK servers to use for leader election
	ZKServers []string `yaml:"zk_servers"`
	// The root path in ZK to use for role leader election. This will
	// be something like /peloton/YOURCLUSTERHERE
	Root string `yaml:"root"`
}

// election holds the state of the zkelection
type election struct {
	sync.Mutex
	metrics    electionMetrics
	running    bool
	leader     string
	role       string
	candidate  *leadership.Candidate
	nomination Nomination
	stopChan   chan struct{}
}

// NewCandidate creates new election object to control participation
// in leader election.
func NewCandidate(
	cfg ElectionConfig,
	parent tally.Scope,
	role string,
	nomination Nomination) (Candidate, error) {

	log.WithFields(log.Fields{"id": nomination.GetID(), "role": role}).
		Debug("Creating new Candidate")

	if role == "" {
		return nil, errors.New("You need to specify a role to campaign " +
			"for that isnt the empty string")
	}

	client, err := zookeeper.New(
		cfg.ZKServers,
		&store.Config{ConnectionTimeout: zkConnErrRetry},
	)
	if err != nil {
		return nil, err
	}
	candidate := leadership.NewCandidate(
		client,
		leaderZkPath(cfg.Root, role),
		nomination.GetID(),
		ttl,
	)
	scope := parent.SubScope("election")
	hostname, err := os.Hostname()
	if err != nil {
		log.WithError(err).Fatal("failed to get hostname")
	}
	el := election{
		running:    false,
		metrics:    newElectionMetrics(scope, hostname),
		role:       role,
		nomination: nomination,
		candidate:  candidate,
		stopChan:   make(chan struct{}, 1),
	}

	return &el, nil

}

// updateLeaderElectionMetric emits leader election
// metrics at constant interval
func (el *election) updateLeaderElectionMetrics(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	select {
	case <-el.stopChan:
		log.Info("Stopped leader election metrics emission")
		return
	case <-ticker.C:
		if el.IsLeader() {
			el.metrics.IsLeader.Update(1)
		} else {
			el.metrics.IsLeader.Update(0)
		}
	}
}

// Start begins running election for leadership
// and calls your callbacks when you gain/lose leadership.
// NOTE: this handles connection errors and retries, and runs until you
// call Stop()
func (el *election) Start() error {
	el.Lock()
	defer el.Unlock()
	if el.running {
		return errors.New("Already running election")
	}
	el.running = true
	el.metrics.Start.Inc(1)
	el.metrics.Running.Update(1)

	log.WithFields(log.Fields{"role": el.role}).Info("Joining election")
	// this will repeatedly call waitForEvent(), and retry when errors
	// are encountered
	go func() {
		for el.running {
			err := el.waitForEvent()
			if err != nil {
				log.WithFields(log.Fields{"role": el.role}).
					Errorf("Failure running election; retrying: %v", err)
			}
			time.Sleep(zkConnErrRetry)
		}
		log.Info("Stopped running election")
	}()

	// Update leader election metrics
	go el.updateLeaderElectionMetrics(_metricsUpdateTick)

	return nil
}

// waitForEvent handles events like this host gaining or losing
// leadership, or a connectivity error occurring.

// NOTE: this function blocks until an event is handled from either
// the error channel or the event channel. It should be called by a
// wrapper function that handles retries
func (el *election) waitForEvent() error {
	electionCh, errCh := el.candidate.RunForElection()

	for {
		select {
		case isElected := <-electionCh:
			if isElected {
				log.WithFields(log.Fields{
					"id":   el.nomination.GetID(),
					"role": el.role,
				}).Info("Leadership gained")
				el.metrics.GainedLeadership.Inc(1)
				el.metrics.IsLeader.Update(1)
				err := el.nomination.GainedLeadershipCallback()
				if err != nil {
					log.WithFields(log.Fields{
						"id":    el.nomination.GetID(),
						"role":  el.role,
						"error": err,
					}).Error("GainedLeadershipCallback failed")
					return err
				}
			} else {
				log.WithFields(log.Fields{
					"id":   el.nomination.GetID(),
					"role": el.role,
				}).Info("Leadership lost")
				el.metrics.LostLeadership.Inc(1)
				el.metrics.IsLeader.Update(0)
				err := el.nomination.LostLeadershipCallback()
				if err != nil {
					log.WithFields(log.Fields{
						"id":    el.nomination.GetID(),
						"role":  el.role,
						"error": err,
					}).Error("LostLeadershipCallback failed")
					return err
				}
			}
		case err := <-errCh:
			if err != nil {
				log.WithFields(log.Fields{
					"role":  el.role,
					"error": err,
				}).Error("Error participating in election")
				el.metrics.Error.Inc(1)
				return err
			}
			// just a shutdown signal from the docker/leadership lib,
			// we can propogate this and let the caller decide if we
			// should continue to run, or terminate
			return nil
		}
	}
}

// Stop stops campaigning for leadership, calls shutdown.
// NOTE: dont call this more than once, or you will panic trying to
// close a closed channel
func (el *election) Stop() error {
	el.Lock()
	defer el.Unlock()
	if el.running {
		el.stopChan <- struct{}{}
		el.running = false
		el.metrics.Stop.Inc(1)
		el.metrics.Running.Update(0)
		el.candidate.Stop()
		// resign asynchronously to avoid deadlocking
		go el.Resign()
	}
	return el.nomination.ShutDownCallback()
}

// Resign gives up leadership
func (el *election) Resign() {
	el.metrics.Resigned.Inc(1)
	el.candidate.Resign()
}

// IsLeader returns whether this candidate is the current leader
func (el *election) IsLeader() bool {
	el.Lock()
	defer el.Unlock()
	// interestingly, the candidate reports leader even if we have
	// resigned, so gate delegating to isLeader on whether we are
	// actively campaigning for the leadership
	return el.running && el.candidate.IsLeader()
}

// leaderZkPath returns the full ZK path to the leader node given a
// election config (the path root) and a component
func leaderZkPath(rootPath string, role string) string {
	// NOTE: remember, there cannot be a leading / for libkv
	return strings.TrimPrefix(path.Join(rootPath, role, "leader"), "/")
}
