// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
)

// PolicyFunc is the factory for different backoff policies
type PolicyFunc func(config *Config) (Policy, error)

// PolicyFactory is the factory for different backoff policies
// Any backoff policy can be implemented through this way.
// Policy object can be obtained via CreateBackOffPolicyeate call
type PolicyFactory struct {
	// policyFactory is a map for name to policy function pointer
	policyFactories map[string]PolicyFunc
}

// Factory is global singelton variable for the factory
var Factory *PolicyFactory

// NewExponentialPolicy is the create object for exponentialPolicy
func (pf *PolicyFactory) NewExponentialPolicy(config *Config) (Policy, error) {
	if config.PlacingTimeout.Seconds() < 0 {
		return nil, fmt.Errorf("placing timeout is invalid")
	}
	return &exponentialPolicy{
		// By default we are making it placing timeout.
		timeOut: config.PlacingTimeout.Seconds(),
	}, nil
}

// register creates the specific policy object and stores in the map
// if object is not present it keep the name to new object pointer mapping
func (pf *PolicyFactory) register(name string, factory PolicyFunc) error {
	if factory == nil {
		err := fmt.Errorf("policy factory %s does not exist", name)
		log.Error(err)
		return err
	}
	_, registered := pf.policyFactories[name]
	if registered {
		log.Errorf("policy factory %s already registered. Ignoring", name)
		return nil
	}
	pf.policyFactories[name] = factory

	return nil
}

// InitPolicyFactory method registers all the policies in the factory
// If registration fails it returns the error , which should be handeled at
// the caller end.
func InitPolicyFactory() error {
	Factory = &PolicyFactory{
		policyFactories: make(map[string]PolicyFunc),
	}
	err := Factory.register(ExponentialBackOffPolicy, Factory.NewExponentialPolicy)
	if err != nil {
		return err
	}
	return nil
}

// GetFactory returns the factory object. Expectation is InitFactory should have been
// called before this function call otherwise , Factory will be nil
func GetFactory() *PolicyFactory {
	return Factory
}

// CreateBackOffPolicy checks all the registered policies and return the
// policy object
func (pf *PolicyFactory) CreateBackOffPolicy(conf *Config) (Policy, error) {
	if conf == nil || conf.PolicyName == "" {
		return nil, errors.New("conf is not valid for creating backoff policy")
	}
	name := conf.PolicyName
	policyFactory, ok := pf.policyFactories[name]
	if !ok {
		// Policy is not present return nil with error
		return nil, errors.New("policy is not present")
	}
	// return actual policy object
	return policyFactory(conf)
}
