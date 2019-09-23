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

package hostmover

import (
	"context"
	"fmt"
	"time"

	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/hostmgr/hostpool/manager"
	ormobjects "github.com/uber/peloton/pkg/storage/objects"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/hostmgr/goalstate"
)

// HostMover provides abstraction to move host across pools
type HostMover interface {
	// MoveHosts moves hosts from source Pool to destination Pool
	MoveHosts(ctx context.Context,
		srcPool string,
		desiredSrcPoolHosts int32,
		destPool string,
		desiredDestPoolHosts int32) error
}

// error variables depicts various errors in host-move request
var (
	errSourcePoolNotExists = errors.New("source pool doesn't exist")
	errDestPoolNotExists   = errors.New("destination pool doesn't exist")
	errNotEnoughHosts      = errors.New("not enough hosts to move")
	errMoveNotSupported    = errors.New("move Not supported for source pool")
)

type hostMover struct {
	// HostPool hostPoolManager service
	hostPoolManager manager.HostPoolManager

	// DB ops for host_info table
	hostInfoOps ormobjects.HostInfoOps

	// Goalstate driver for hosts
	driver goalstate.Driver

	// resource manager client for getting hosts from scorer
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient

	// Metrics
	metrics *Metrics
}

// NewHostMover creates the HostMover object
func NewHostMover(
	manager manager.HostPoolManager,
	hostInfoOps ormobjects.HostInfoOps,
	driver goalstate.Driver,
	scope tally.Scope,
	resmgrClient resmgrsvc.ResourceManagerServiceYARPCClient,
) HostMover {
	return &hostMover{
		hostPoolManager: manager,
		metrics:         NewMetrics(scope),
		hostInfoOps:     hostInfoOps,
		driver:          driver,
		resmgrClient:    resmgrClient,
	}
}

// MoveHosts moves hosts from source Pool to destination Pool
func (hm *hostMover) MoveHosts(ctx context.Context,
	srcPool string,
	desiredSrcPoolHosts int32,
	destPool string,
	desiredDestPoolHosts int32) error {
	// validate source Pool
	if srcPool != common.SharedHostPoolID {
		hm.metrics.hostMoveFailure.Inc(1)
		return errMoveNotSupported
	}
	_, err := hm.hostPoolManager.GetPool(srcPool)
	if err != nil {
		log.WithField("pool", srcPool).
			WithError(err).
			Error("Source Pool doesn't exist")
		hm.metrics.hostMoveFailure.Inc(1)
		return errSourcePoolNotExists
	}

	// validate destination Pool
	_, err = hm.hostPoolManager.GetPool(destPool)
	if err != nil {
		log.WithField("pool", destPool).
			WithError(err).
			Error("Destination Pool doesn't exist")
		hm.metrics.hostMoveFailure.Inc(1)
		return errDestPoolNotExists
	}

	// get the hosts from the DB
	hostInfos, err := hm.syncHostsFromDB(ctx)
	if err != nil {
		hm.metrics.hostMoveFailure.Inc(1)
		return err
	}

	var srcPoolCurrentHosts, desiredPoolCurrentHosts, deltaSrcHosts, deltaDestHosts int32
	for _, h := range hostInfos {
		if h.GetDesiredPool() == srcPool {
			srcPoolCurrentHosts = srcPoolCurrentHosts + 1
		}
		if h.GetDesiredPool() == destPool {
			desiredPoolCurrentHosts = desiredPoolCurrentHosts + 1
		}
	}

	// Check if desired pool already have number of hosts
	// return nil if we already reached to desired state
	// This is to be done to bring atomicity into the request
	if desiredPoolCurrentHosts >= desiredDestPoolHosts {
		log.Info(fmt.Sprintf("Sufficiant hosts %d already present in %s "+
			"pool ", desiredPoolCurrentHosts, destPool))
		return nil
	}

	// delta how many hosts have to be moved from source pool
	deltaSrcHosts = srcPoolCurrentHosts - desiredSrcPoolHosts
	// delta how many hosts needed in destination pool
	deltaDestHosts = desiredDestPoolHosts - desiredPoolCurrentHosts

	// Desired pool in the table is the actual number of hosts for the pools
	// if delta is less then we dont have anything to move
	if deltaSrcHosts <= 0 {
		hm.metrics.hostMoveFailure.Inc(1)
		return errors.New(fmt.Sprintf("Not sufficiant hosts in source pool "+
			"%s have only %d hosts", srcPool, srcPoolCurrentHosts))
	}

	// Checking if same number of hosts been moved from one pool to another
	if deltaSrcHosts != deltaDestHosts {
		hm.metrics.hostMoveFailure.Inc(1)
		return errors.New(fmt.Sprintf("not valid request " +
			"src pool delta is not equivalent to dest pool delta"))
	}

	// call scorer to get the hosts
	resp, err := hm.resmgrClient.GetHostsByScores(ctx,
		&resmgrsvc.GetHostsByScoresRequest{
			Limit: uint32(deltaDestHosts),
		})
	if err != nil {
		hm.metrics.hostMoveFailure.Inc(1)
		return err
	}

	if len(resp.Hosts) != int(deltaDestHosts) {
		log.WithField("pool", srcPool).
			Info("Scorer not able to find desired number of hosts in the" +
				" sourcePool")
		hm.metrics.hostMoveFailure.Inc(1)
		return errors.New(errNotEnoughHosts.Error())
	}

	for _, host := range resp.Hosts {
		if err := hm.hostPoolManager.UpdateDesiredPool(host, destPool); err != nil {
			hm.metrics.hostMoveFailure.Inc(1)
			return err
		}

		hm.EnqueueHost(host, time.Now())
		hm.metrics.hostMoveSuccess.Inc(1)
	}
	return nil
}

// syncHostsFromDB fetches hostInfos from DB
func (hm *hostMover) syncHostsFromDB(ctx context.Context) ([]*hpb.HostInfo, error) {
	log.Debug("syncing host goal state engine from DB")
	hostInfos, err := hm.hostInfoOps.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	return hostInfos, nil
}

// EnqueueHost is used to enqueue a host into the goal state.
func (hm *hostMover) EnqueueHost(hostname string, deadline time.Time) {
	hm.driver.EnqueueHost(hostname, deadline)
}
