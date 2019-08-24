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

	"github.com/uber/peloton/pkg/common/lifecycle"
	"github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	log "github.com/sirupsen/logrus"
)

// agentSyncer syncs agent info from mesos periodically
// and send the info via HostEvent through hostEventCh.
type agentSyncer struct {
	lf lifecycle.LifeCycle

	operatorClient  mpb.MasterOperatorClient
	hostEventCh     chan<- *scalar.HostEvent
	refreshInterval time.Duration
}

func newAgentSyncer(
	operatorClient mpb.MasterOperatorClient,
	hostEventCh chan<- *scalar.HostEvent,
	refreshInterval time.Duration,
) *agentSyncer {
	return &agentSyncer{
		lf:              lifecycle.NewLifeCycle(),
		operatorClient:  operatorClient,
		hostEventCh:     hostEventCh,
		refreshInterval: refreshInterval,
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

	for _, agent := range agents.GetAgents() {
		a.hostEventCh <- scalar.BuildHostEventFromAgent(agent, scalar.UpdateAgent)
	}
}
