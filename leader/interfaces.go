package leader

// Nomination represents the set of callbacks to handle leadership election
type Nomination interface {
	// GainedLeadershipCallback is the callback when the current node becomes the leader
	GainedLeadershipCallback() error
	// ShutDownCallback is the callback to shut down gracefully if possible
	ShutDownCallback() error
	// LostLeadershipCallback is the callback when the leader lost leadership
	LostLeadershipCallback() error
	// GetID returns the host:master_port of the node running for leadership (i.e. the ID)
	GetID() string
}

// Candidate is an interface representing both a candidate campaigning to become a leader, and
// the observer that watches state changes in leadership to be notified about changes
type Candidate interface {
	IsLeader() bool
	Start() error
	Stop() error
	Resign()
}
