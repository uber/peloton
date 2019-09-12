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

package lifecyclemgr

import (
	"context"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbhostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	v1_hostsvc "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/util"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/tally"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

type v1LifecycleMgr struct {
	*lockState
	// v1 client hostmgr pod operations.
	hostManagerV1 v1_hostsvc.HostManagerServiceYARPCClient
	metrics       *Metrics
}

// newV1LifecycleMgr returns an instance of the v1 lifecycle manager.
func newV1LifecycleMgr(
	dispatcher *yarpc.Dispatcher,
	parent tally.Scope,
) *v1LifecycleMgr {
	return &v1LifecycleMgr{
		hostManagerV1: v1_hostsvc.NewHostManagerServiceYARPCClient(
			dispatcher.ClientConfig(
				common.PelotonHostManager,
			),
		),
		lockState: &lockState{state: 0},
		metrics:   NewMetrics(parent.SubScope("jobmgr").SubScope("pod")),
	}
}

// Launch launches the task using taskConfig. pod spec is ignored in this impl.
func (l *v1LifecycleMgr) Launch(
	ctx context.Context,
	leaseID string,
	hostname string,
	agentID string,
	pods map[string]*LaunchableTaskInfo,
	rateLimiter *rate.Limiter,
) (err error) {
	defer func() {
		if err != nil {
			if newErr := l.TerminateLease(
				ctx,
				hostname,
				agentID,
				leaseID,
			); newErr != nil {
				err = errors.Wrap(err, newErr.Error())
			}
		}
	}()

	if len(pods) == 0 {
		return errEmptyPods
	}
	// enforce rate limit
	if rateLimiter != nil && !rateLimiter.Allow() {
		l.metrics.LaunchRateLimit.Inc(1)
		return yarpcerrors.ResourceExhaustedErrorf(
			"rate limit reached for kill")
	}

	log.WithField("pods", pods).Debug("Launching Pods")
	callStart := time.Now()

	// convert LaunchableTaskInfo to v1alpha Hostsvc LaunchablePod
	var launchablePods []*pbhostmgr.LaunchablePod
	for _, pod := range pods {
		launchablePod := pbhostmgr.LaunchablePod{
			PodId: util.CreatePodIDFromMesosTaskID(
				pod.Runtime.GetMesosTaskId()),
			Spec:  pod.Spec,
			Ports: pod.Runtime.Ports,
		}

		// TODO: peloton system labels contain invalid characters for labels in
		// k8s for example '/' in resource pool. We should:
		// 1. validate spec when job is submitted.
		// 2. look into supporting k8s annotations.
		// launchablePod.Spec.Labels = append(
		// 	launchablePod.Spec.Labels,
		// 	api.ConvertLabels(pod.ConfigAddOn.GetSystemLabels())...)
		launchablePods = append(launchablePods, &launchablePod)
	}

	// Launch pods on Hostmgr using v1alpha LaunchPods
	ctx, cancel := context.WithTimeout(ctx, _defaultHostmgrAPITimeout)
	defer cancel()
	var request = &v1_hostsvc.LaunchPodsRequest{
		// This is because we do not change resmgr code to talk in terms
		// of HostLease yet. So OfferID here is the leaseID that resmgr
		// gets via placement engine.
		LeaseId:  &pbhostmgr.LeaseID{Value: leaseID},
		Hostname: hostname,
		Pods:     launchablePods,
	}

	_, err = l.hostManagerV1.LaunchPods(ctx, request)
	callDuration := time.Since(callStart)

	if err != nil {
		l.metrics.LaunchFail.Inc(int64(len(pods)))
		return err
	}

	l.metrics.Launch.Inc(int64(len(pods)))
	log.WithFields(log.Fields{
		"num_pods": len(pods),
		"hostname": hostname,
		"duration": callDuration.Seconds(),
	}).Debug("Launched pods")
	l.metrics.LaunchDuration.Record(callDuration)
	return nil
}

// Kill tries to kill the pod using podID. If a host is provided, it holds
// the host.
func (l *v1LifecycleMgr) Kill(
	ctx context.Context,
	podID string,
	hostToHold string,
	rateLimiter *rate.Limiter,
) error {
	// Check lock.
	if l.lockState.hasKillLock() {
		l.metrics.KillRateLimit.Inc(1)
		return yarpcerrors.InternalErrorf("kill op is locked")
	}

	// Enforce rate limit.
	if rateLimiter != nil && !rateLimiter.Allow() {
		l.metrics.KillFail.Inc(1)
		return yarpcerrors.ResourceExhaustedErrorf(
			"rate limit reached for kill")
	}

	var err error
	if len(hostToHold) != 0 {
		err = l.killAndHold(ctx, podID, hostToHold)
	} else {
		err = l.kill(ctx, podID)
	}
	if err != nil {
		l.metrics.KillFail.Inc(1)
		return err
	}
	l.metrics.Kill.Inc(1)
	return nil
}

func (l *v1LifecycleMgr) kill(ctx context.Context, podID string) error {
	req := &v1_hostsvc.KillPodsRequest{
		PodIds: []*peloton.PodID{{Value: podID}},
	}
	_, err := l.hostManagerV1.KillPods(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

func (l *v1LifecycleMgr) killAndHold(ctx context.Context, podID string, hostToHold string) error {
	req := &v1_hostsvc.KillAndHoldPodsRequest{
		Entries: []*v1_hostsvc.KillAndHoldPodsRequest_Entry{
			{
				PodId:      &peloton.PodID{Value: podID},
				HostToHold: hostToHold,
			},
		},
	}
	_, err := l.hostManagerV1.KillAndHoldPods(ctx, req)
	if err != nil {
		return err
	}
	return nil
}

// ShutdownExecutor is a no-op for v1 lifecyclemgr.
// This is a mesos specific call and will only be implemented for v0 case.
func (l *v1LifecycleMgr) ShutdownExecutor(
	ctx context.Context,
	taskID string,
	agentID string,
	rateLimiter *rate.Limiter,
) error {
	l.metrics.Shutdown.Inc(1)
	return nil
}

// TerminateLease returns the unused lease back to the hostmgr.
func (l *v1LifecycleMgr) TerminateLease(
	ctx context.Context,
	hostname string,
	agentID string,
	leaseID string,
) error {
	request := &v1_hostsvc.TerminateLeasesRequest{
		Leases: []*v1_hostsvc.TerminateLeasesRequest_LeasePair{{
			Hostname: hostname,
			LeaseId:  &pbhostmgr.LeaseID{Value: leaseID},
		}},
	}
	ctx, cancel := context.WithTimeout(ctx, _defaultHostmgrAPITimeout)
	defer cancel()
	_, err := l.hostManagerV1.TerminateLeases(ctx, request)
	if err != nil {
		l.metrics.TerminateLeaseFail.Inc(1)
		return errors.Wrapf(err,
			"failed to terminate lease: %v host %v", leaseID, hostname,
		)
	}
	l.metrics.TerminateLease.Inc(1)
	return nil
}

// GetTasksOnDrainingHosts gets the taskIDs of the tasks on the
// hosts in DRAINING state
func (l *v1LifecycleMgr) GetTasksOnDrainingHosts(
	ctx context.Context,
	limit uint32,
	timeout uint32,
) ([]string, error) {
	return nil, errors.New("not implemented")
}
