package config

import (
	"code.uber.internal/infra/peloton/common/metrics"
	"code.uber.internal/infra/peloton/hostmgr/mesos"
	"code.uber.internal/infra/peloton/leader"
	sc "code.uber.internal/infra/peloton/storage/config"
)

// Config holds all configuration necessary to run a placement engine.
type Config struct {
	Metrics metrics.Config `yaml:"metrics"`
	// TaskDequeueLimit is the max number of tasks to dequeue in a request
	Placement PlacementConfig `yaml:"placement"`

	// TODO: remove when we switch to hostMgr launch task api
	Storage sc.StorageConfig `yaml:"storage"`
	Mesos   mesos.Config     `yaml:"mesos"`
}

// PlacementConfig is Placement engine specific config
type PlacementConfig struct {
	TaskDequeueLimit int `yaml:"task_dequeue_limit"`
	// OfferDequeueLimit is the max Number of Offers to dequeue in a request
	OfferDequeueLimit int `yaml:"offer_dequeue_limit"`
	// ResmgrElection and HostmgrElection are needed for placement engine to
	// find out the leader address of peloton-resmgr and peloton-hostmgr
	ResmgrElection  leader.ElectionConfig `yaml:"resmgr_election"`
	HostmgrElection leader.ElectionConfig `yaml:"hostmgr_election"`

	// Temp config for resmgr address
	ResmgrHost string `yaml:"resmgr_host"`
	ResmgrPort int    `yaml:"resmgr_port"`

	// Temp config for hostmgr address
	HostmgrHost string `yaml:"hostmgr_host"`
	HostmgrPort int    `yaml:"hostmgr_port"`
}
