package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"

	"code.uber.internal/infra/peloton/leader"
	"code.uber.internal/infra/peloton/master/mesos"
	"code.uber.internal/infra/peloton/scheduler"
	"code.uber.internal/infra/peloton/storage/mysql"
	"gopkg.in/validator.v2"
	"gopkg.in/yaml.v2"
)

// ValidationError is the returned when a configuration fails to pass validation
type ValidationError struct {
	errorMap validator.ErrorMap
}

// ErrForField returns the validation error for the given field
func (e ValidationError) ErrForField(name string) error {
	return e.errorMap[name]
}

// Error returns the error string from a ValidationError
func (e ValidationError) Error() string {
	var w bytes.Buffer

	fmt.Fprintf(&w, "validation failed")
	for f, err := range e.errorMap {
		fmt.Fprintf(&w, "   %s: %v\n", f, err)
	}

	return w.String()
}

// AppConfig encapulates the master runtime config
type AppConfig struct {
	Metrics   metricsConfiguration  `yaml:"metrics"`
	DbConfig  mysql.Config          `yaml:"db"`
	Master    MasterConfig          `yaml:"master"`
	Mesos     mesos.Config          `yaml:"mesos"`
	Scheduler scheduler.Config      `yaml:"scheduler"`
	Election  leader.ElectionConfig `yaml:"election"`
}

// MasterConfig is framework specific configuration
type MasterConfig struct {
	Port                  int `yaml:"port"`
	OfferHoldTimeSec      int `yaml:"offer_hold_time_sec"`      // Time to hold offer for in seconds
	OfferPruningPeriodSec int `yaml:"offer_pruning_period_sec"` // Frequency of running offer pruner
}

type metricsConfiguration struct {
	Prometheus *prometheusConfiguration `yaml:"prometheus"`
	Statsd     *statsdConfiguration     `yaml:"statsd"`
}
type prometheusConfiguration struct {
	Enable bool `yaml:"enable"`
}
type statsdConfiguration struct {
	Enable   bool   `yaml:"enable"`
	Endpoint string `yaml:"endpoint"`
}

// NewAppConfig loads the given configs in order, merges them together, and returns
// the AppConfig
func NewAppConfig(configs ...string) (*AppConfig, error) {
	var config *AppConfig
	if len(configs) == 0 {
		return config, errors.New("no files to load")
	}
	config = &AppConfig{}
	for _, fname := range configs {
		data, err := ioutil.ReadFile(fname)
		if err != nil {
			return nil, err
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, err
		}
	}

	// Validate on the merged config at the end.
	if err := validator.Validate(config); err != nil {
		return nil, ValidationError{
			errorMap: err.(validator.ErrorMap),
		}
	}
	return config, nil
}
