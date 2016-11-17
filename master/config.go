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
	mesosZkPath           = "MESOS_ZK_PATH"
	dbHost                = "DB_HOST"
	taskDequeueLimit      = "SCHEDULER_TASK_DEQUEUE_LIMIT"
	electionZkServers     = "ELECTION_ZK_SERVERS"
	loggingLevel          = "LOGGING_LEVEL"
	masterPort            = "MASTER_PORT"
	OfferHoldTimeSec      = "OFFER_HOLD_TIME_SEC"
	OfferPruningPeriodSec = "OFFER_PRUNING_PERIOD_SEC"
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
	Port                  int `yaml:"port"`
	OfferHoldTimeSec      int `yaml:"offer_hold_time_sec"`      // Time to hold offer for in seconds
	OfferPruningPeriodSec int `yaml:"offer_pruning_period_sec"` // Frequency of running offer pruner
}

// Override configs with environment vars if set, otherwise values
// from yaml files will be used.
// TODO: use reflection to override any YAML configurations from ENV
func LoadConfigFromEnv(cfg *AppConfig) {
	if v := os.Getenv(mesosZkPath); v != "" {
		log.Infof("Override mesos.zk_path with '%v'", v)
		cfg.Mesos.ZkPath = v
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

	if v := os.Getenv(masterPort); v != "" {
		log.Infof("Override master port with %v", v)
		cfg.Master.Port, _ = strconv.Atoi(v)
	}

	if v := os.Getenv(OfferHoldTimeSec); v != "" {
		log.Infof("Override OfferHoldTimeSec with %v", v)
		cfg.Master.OfferHoldTimeSec, _ = strconv.Atoi(v)
	}

	if v := os.Getenv(OfferPruningPeriodSec); v != "" {
		log.Infof("Override OfferPruningPeriodSec with %v", v)
		cfg.Master.OfferPruningPeriodSec, _ = strconv.Atoi(v)
	}
}
