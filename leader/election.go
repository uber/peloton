package leader

import (
	"sync"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/infra/uns.git/net/zk/election"
	"code.uber.internal/infra/uns.git/zk"
)

const leaderElectionZKPath = "/peloton/master/leader"

// Node represents the peloton node which takes part in the election
type Node interface {
	// GainedLeadershipCallBack is the callback when the current node becomes the leader
	GainedLeadershipCallBack() error
	// NewLeaderCallBack is the callback when some other node becomes the leader
	NewLeaderCallBack() error
	// ShutDownCallback is the callback to shut down gracefully if possible
	ShutDownCallback() error
}

// ElectionConfig is config related to leader election of this service
type ElectionConfig struct {
	// A comma separated list of ZK servers to use for leader election
	ZKServers []string `yaml:"zkServers" validate:"min=1"`
}

// LeaderElection holds the state of the election
type LeaderElection struct {
	election        election.Election
	electionStateMu sync.RWMutex
	electionState   election.Event // protected by electionStateMu
	node            Node
	leader          string
}

// GetElectionState returns the current state of the election
func (el *LeaderElection) getElectionState() election.Event {
	el.electionStateMu.RLock()
	defer el.electionStateMu.RUnlock()
	return el.electionState
}

// GetCurrentLeader returns the current leader
func (el *LeaderElection) GetCurrentLeader() string {
	// The data provided by the current leader
	return el.getElectionState().Data
}

// NewZkElection creates new election object to control participation in leader election
func NewZkElection(cfg ElectionConfig, instanceID string, peloton Node) (*LeaderElection, error) {
	connectionFactory, err := zk.NewConnectionFactory(cfg.ZKServers)
	if err != nil {
		return nil, err
	}
	conn, err := connectionFactory.GetConnection("election-conn")
	if err != nil {
		return nil, err
	}
	return newZKElection(conn, leaderElectionZKPath, instanceID, peloton)
}

func newZKElection(conn zk.StatefulConnection, path string, instanceID string, peloton Node, options ...election.Option) (*LeaderElection, error) {
	el := &LeaderElection{
		node: peloton,
	}
	election, err := election.NewElection(
		conn,
		leaderElectionZKPath,
		instanceID,
		el.electionCallback,
		options...,
	)
	if err != nil {
		return nil, err
	}
	el.election = election
	el.election.Start()

	return el, nil
}

func (el *LeaderElection) electionCallback(ev election.Event) {
	el.electionStateMu.Lock()
	el.electionState = ev
	el.electionStateMu.Unlock()
	log.Infof("election callback called with event:%v", ev)

	switch ev.State {
	case election.GainedLeadership:
		// we're now the election!
		// do whatever the leader does.
		el.node.GainedLeadershipCallBack()
	case election.NewLeader:
		// someone else is the leader.
		el.node.NewLeaderCallBack()
	case election.Withdrawn:
		// no longer participating in the election
		// shutting down
		el.node.ShutDownCallback()
	case election.Abdicated:
		// we gave up the leadership
		// no-op
		// wait for NewLeader or GainedLeadership
	case election.InJeopardy:
		// we may no longer be the election
		// wait for NewLeader or GainedLeadership
	}
}
