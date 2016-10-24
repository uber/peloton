package leader

import (
	"code.uber.internal/infra/uns.git/net/zk/election"
	"code.uber.internal/infra/uns.git/zk/testutils"
	"fmt"
	uzk "github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
	"time"
)

const (
	_testData           = "my-instance-%d"
	_testConnection     = "election-conn-%d"
	_testNodeIdentifier = "test"
)

// testNode represents a node taking part in the election
type testNode struct {
	name string
	c    chan string
}

func (tn testNode) GainedLeadershipCallBack() error {
	fmt.Printf("%s is leader!\n", tn.name)
	tn.c <- tn.name
	return nil
}

func (tn testNode) NewLeaderCallBack() error {
	return nil
}

func (tn testNode) ShutDownCallback() error {
	return nil
}

// Test the case where initially we are the leader and when we
// as the leader disappear, the other node becomes the new leader.
func TestLeaderElection(t *testing.T) {
	fakeZK := testutils.NewFakeZookeeper(t)
	defer fakeZK.Stop()

	fakeZK.ForceSetNode(leaderElectionZKPath, nil)

	leader := make(chan string, 1)
	option := election.WithIdentifier(_testNodeIdentifier)
	nodes := []testNode{}

	// simulate 2 peloton masters
	for i := 0; i < 2; i++ {
		// create fake zk
		conn, err := fakeZK.GetConnection(fmt.Sprintf(_testConnection, i))
		// start of connected to zk
		fakeZK.SetState(fmt.Sprintf(_testConnection, i), uzk.StateHasSession)

		// fake node
		node := testNode{name: fmt.Sprintf(_testData, i), c: leader}

		// new election
		if _, err = newZKElection(conn, leaderElectionZKPath, fmt.Sprintf(_testData, i), node, option); err != nil {
			t.Fatal(err)
		}

		nodes = append(nodes, node)
		time.Sleep(time.Second * 1)
	}

	// wait for leader
	l := <-leader

	// node1 should be the leader
	assert.Equal(t, nodes[0].name, l)

	// disconnect node2 from zk
	znode := path.Join(leaderElectionZKPath, fmt.Sprintf("%s_leader_", _testNodeIdentifier))
	fakeZK.ForceDelNode(znode)

	// wait for leader
	l = <-leader

	// node2 should become the leader
	assert.Equal(t, nodes[1].name, l)
}
