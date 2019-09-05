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

package mesos

import (
	"time"

	mesosmaster "github.com/uber/peloton/.gen/mesos/v1/master"
	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"

	log "github.com/sirupsen/logrus"
)

const agentChanSize = 10

// agentSyncer syncs agent info from mesos periodically
// and send the info via HostEvent through hostEventCh.
type agentSyncer struct {
	lf lifecycle.LifeCycle

	agentCh chan []*mesosmaster.Response_GetAgents_Agent

	operatorClient  mpb.MasterOperatorClient
	refreshInterval time.Duration
}

func newAgentSyncer(
	operatorClient mpb.MasterOperatorClient,
	refreshInterval time.Duration,
) *agentSyncer {
	return &agentSyncer{
		lf:              lifecycle.NewLifeCycle(),
		operatorClient:  operatorClient,
		refreshInterval: refreshInterval,
		agentCh:         make(chan []*mesosmaster.Response_GetAgents_Agent, agentChanSize),
	}
}

func (a *agentSyncer) Start() {
	if !a.lf.Start() {
		// already started,
		// skip the action
		return
	}

	// make sure it is run once before Start return
	a.runOnce()
	go a.run()
}

func (a *agentSyncer) Stop() {
	a.lf.Stop()
}

func (a *agentSyncer) AgentCh() <-chan []*mesosmaster.Response_GetAgents_Agent {
	return a.agentCh
}

func (a *agentSyncer) run() {
	ticker := time.NewTicker(a.refreshInterval)

	for {
		select {
		case <-ticker.C:
			a.runOnce()
		case <-a.lf.StopCh():
			ticker.Stop()
			a.lf.StopComplete()
			return
		}
	}
}

func (a *agentSyncer) runOnce() {
	agents, err := a.operatorClient.Agents()
	if err != nil {
		log.WithError(err).Warn("Cannot refresh agent map from master")
		return
	}

	select {
	case a.agentCh <- agents.GetAgents():
		return
	default:
		return
	}

}
