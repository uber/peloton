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
	"context"
	"github.com/pborman/uuid"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/pkg/common/util"
	"testing"

	"github.com/stretchr/testify/assert"
	mesos "github.com/uber/peloton/.gen/mesos/v1"
	sched "github.com/uber/peloton/.gen/mesos/v1/scheduler"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/p2k/scalar"

	"go.uber.org/yarpc"
)

func TestMesosManagerStart(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.Start()
}

func TestMesosManagerStop(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.Stop()
}

func TestMesosManagerLaunchPod(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	assert.NoError(t, testManager.LaunchPod(nil, "", ""))
}

func TestMesosManagerKillPod(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	assert.NoError(t, testManager.KillPod(""))
}

func TestMesosManagerAckPodEvent(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.AckPodEvent(context.Background(), nil)
}

func TestMesosManagerReoncileHosts(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.ReconcileHosts()
}

// TestNewMesosManagerOffersSingleOffer tests
// adding offers for the same host
func TestNewMesosManagerOffersSameHost(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	host := "hostname1"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
				{
					Resources: []*mesos.Resource{
						util.NewMesosResourceBuilder().
							WithName(common.MesosCPU).
							WithValue(2.0).
							Build(),
						util.NewMesosResourceBuilder().
							WithName(common.MesosMem).
							WithValue(300.0).
							Build(),
					},
					Hostname: &host,
					Id:       &mesos.OfferID{Value: &uuid2},
				},
			},
		},
	})

	he := <-hostEventCh
	assert.Equal(t, he.GetEventType(), scalar.UpdateHostAvailableRes)
	assert.Equal(t, he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   3.0,
		MemMb: 400.0,
	})
	assert.Equal(t, he.GetHostInfo().GetHostName(), host)
}

// TestNewMesosManagerOffersSingleOffer tests
// adding offers for multiple hosts
func TestNewMesosManagerOffersMultipleHost(t *testing.T) {
	podEventCh := make(chan *scalar.PodEvent, 1000)
	hostEventCh := make(chan *scalar.HostEvent, 1000)
	d := testDispatcher()

	host1 := "hostname1"
	host2 := "hostname2"
	uuid1 := uuid.New()
	uuid2 := uuid.New()

	testManager := NewMesosManager(d, podEventCh, hostEventCh)
	testManager.Offers(context.Background(), &sched.Event{
		Offers: &sched.Event_Offers{
			Offers: []*mesos.Offer{
				{Resources: []*mesos.Resource{
					util.NewMesosResourceBuilder().
						WithName(common.MesosCPU).
						WithValue(1.0).
						Build(),
					util.NewMesosResourceBuilder().
						WithName(common.MesosMem).
						WithValue(100.0).
						Build(),
				},
					Hostname: &host1,
					Id:       &mesos.OfferID{Value: &uuid1},
				},
				{
					Resources: []*mesos.Resource{
						util.NewMesosResourceBuilder().
							WithName(common.MesosCPU).
							WithValue(2.0).
							Build(),
						util.NewMesosResourceBuilder().
							WithName(common.MesosMem).
							WithValue(300.0).
							Build(),
					},
					Hostname: &host2,
					Id:       &mesos.OfferID{Value: &uuid2},
				},
			},
		},
	})

	he := <-hostEventCh
	assert.Equal(t, he.GetEventType(), scalar.UpdateHostAvailableRes)
	assert.Equal(t, he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   1.0,
		MemMb: 100.0,
	})
	assert.Equal(t, he.GetHostInfo().GetHostName(), host1)

	he = <-hostEventCh
	assert.Equal(t, he.GetEventType(), scalar.UpdateHostAvailableRes)
	assert.Equal(t, he.GetHostInfo().GetAvailable(), &peloton.Resources{
		Cpu:   2.0,
		MemMb: 300.0,
	})
	assert.Equal(t, he.GetHostInfo().GetHostName(), host2)
}

func testDispatcher() *yarpc.Dispatcher {
	return yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonHostManager,
	})
}
