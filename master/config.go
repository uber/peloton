package main

import (
	"os"
	"strconv"

	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/scheduler"
	"code.uber.internal/infra/peloton/storage/mysql"
)

const (
	mesosZkPath       = "MESOS_ZK_PATH"
	leaderHost        = "MASTER_LEADER_HOST"
	dbHost            = "DB_HOST"
	taskDequeueLimit  = "SCHEDULER_TASK_DEQUEUE_LIMIT"
	electionZkServers = "ELECTION_ZK_SERVERS"
	loggingLevel      = "LOGGING_LEVEL"
)

// Configuration encapulates the master runtime config
type AppConfig struct {
	Logging   log.Configuration
	Metrics   metrics.Configuration
	Sentry    log.SentryConfiguration
	Verbose   bool
	DbConfig  mysql.Config          `yaml:"db"`
	Master    MasterConfig          `yaml:"master"`
	Mesos     mesos.Config          `yaml:"mesos"`
	Scheduler scheduler.Config      `yaml:"scheduler"`
	Election  leader.ElectionConfig `yaml:"election"`
}

// Peloton master specific configuration
type MasterConfig struct {
	Port     int            `yaml:"port"`
	Leader   LeaderConfig   `yaml:"leader"`
	Follower FollowerConfig `yaml:"follower"`
}

// Peloton master leader specific configuration
type LeaderConfig struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
}

// Peloton master follower specific configuration
type FollowerConfig struct {
	Port int `yaml:"port"`
}

// Override configs with environment vars if set, otherwise values
// from yaml files will be used.
// TODO: use reflection to override any YAML configurations from ENV
func LoadConfigFromEnv(cfg *AppConfig) {
	if v := os.Getenv(mesosZkPath); v != "" {
		log.Infof("Override mesos.zk_path with '%v'", v)
		cfg.Mesos.ZkPath = v
	}

	if v := os.Getenv(leaderHost); v != "" {
		log.Infof("Override master.leader.host with '%v'", v)
		cfg.Master.Leader.Host = v
	}

	if v := os.Getenv(dbHost); v != "" {
		log.Infof("Override db.host with '%v'", v)
		cfg.DbConfig.Host = v
	}

	if v := os.Getenv(taskDequeueLimit); v != "" {
		log.Infof("Override scheduler.task_dequeue_limit with '%v'", v)
		cfg.Scheduler.TaskDequeueLimit, _ = strconv.Atoi(v)
	}

	// TODO: combine mesosZkPath and electionZkServers to share same zk
	if v := os.Getenv(electionZkServers); v != "" {
		log.Infof("Override election.ZKServers with '%v'", v)
		cfg.Election.ZKServers = []string{v}
	}

    if v := os.Getenv(loggingLevel); v != "" {
		log.Infof("Override logging.level with '%v'", v)
		cfg.Logging.Level, _ = log.ParseLevel(v)
    }
}
