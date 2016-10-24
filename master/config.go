package main

import (
	"code.uber.internal/go-common.git/x/log"
	"code.uber.internal/go-common.git/x/metrics"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/storage/mysql"
)

// Configuration encapulates the master runtime config
type AppConfig struct {
	Logging  log.Configuration
	Metrics  metrics.Configuration
	Sentry   log.SentryConfiguration
	Verbose  bool
	DbConfig mysql.Config `yaml:"db"`
	Master   MasterConfig `yaml:"master"`
	Mesos    mesos.Config `yaml:"mesos"`
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

// Peloton master followerspecific configuration
type FollowerConfig struct {
	Port int `yaml:"port"`
}
