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

package cli

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/grpc"

	hostsvc "github.com/uber/peloton/.gen/peloton/api/v0/host/svc"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	updatesvc "github.com/uber/peloton/.gen/peloton/api/v0/update/svc"
	volume_svc "github.com/uber/peloton/.gen/peloton/api/v0/volume/svc"
	adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	statelesssvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	hostmgr_svc "github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	hostmgr_svc_v1 "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha/svc"
	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/cli/middleware"
	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/leader"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jobClient       job.JobManagerYARPCClient
	taskClient      task.TaskManagerYARPCClient
	podClient       podsvc.PodServiceYARPCClient
	statelessClient statelesssvc.JobServiceYARPCClient
	watchClient     watchsvc.WatchServiceYARPCClient
	resClient       respool.ResourceManagerYARPCClient
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	updateClient    updatesvc.UpdateServiceYARPCClient
	volumeClient    volume_svc.VolumeServiceYARPCClient
	hostMgrClient   hostmgr_svc.InternalHostServiceYARPCClient
	hostMgrClientV1 hostmgr_svc_v1.HostManagerServiceYARPCClient
	hostClient      hostsvc.HostServiceYARPCClient
	jobmgrClient    jobmgrsvc.JobManagerServiceYARPCClient
	adminClient     adminsvc.AdminServiceYARPCClient
	dispatcher      *yarpc.Dispatcher
	ctx             context.Context
	cancelFunc      context.CancelFunc
	// Debug is whether debug output is enabled
	Debug bool
}

// New returns a new RPC client given a framework URL and timeout and error
func New(
	discovery leader.Discovery,
	timeout time.Duration,
	authConfig *middleware.BasicAuthConfig,
	debug bool) (*Client, error) {

	jobmgrURL, err := discovery.GetAppURL(common.JobManagerRole)
	if err != nil {
		return nil, err
	}

	resmgrURL, err := discovery.GetAppURL(common.ResourceManagerRole)
	if err != nil {
		return nil, err
	}

	hostmgrURL, err := discovery.GetAppURL(common.HostManagerRole)
	if err != nil {
		return nil, err
	}

	t := grpc.NewTransport()

	authMiddleware := middleware.NewBasicAuthOutboundMiddleware(authConfig)

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonCLI,
		Outbounds: yarpc.Outbounds{
			common.PelotonJobManager: transport.Outbounds{
				Unary:  t.NewSingleOutbound(jobmgrURL.Host),
				Stream: t.NewSingleOutbound(jobmgrURL.Host),
			},
			common.PelotonResourceManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(resmgrURL.Host),
			},
			common.PelotonHostManager: transport.Outbounds{
				Unary:  t.NewSingleOutbound(hostmgrURL.Host),
				Stream: t.NewSingleOutbound(hostmgrURL.Host),
			},
		},
		OutboundMiddleware: yarpc.OutboundMiddleware{
			Unary:  authMiddleware,
			Oneway: authMiddleware,
			Stream: authMiddleware,
		},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	client := Client{
		Debug: debug,
		jobClient: job.NewJobManagerYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		taskClient: task.NewTaskManagerYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		resClient: respool.NewResourceManagerYARPCClient(
			dispatcher.ClientConfig(common.PelotonResourceManager),
		),
		resMgrClient: resmgrsvc.NewResourceManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonResourceManager)),
		updateClient: updatesvc.NewUpdateServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		volumeClient: volume_svc.NewVolumeServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		hostMgrClient: hostmgr_svc.NewInternalHostServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager),
		),
		hostMgrClientV1: hostmgr_svc_v1.NewHostManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager),
		),
		hostClient: hostsvc.NewHostServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager),
		),
		podClient: podsvc.NewPodServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		statelessClient: statelesssvc.NewJobServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		watchClient: watchsvc.NewWatchServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		jobmgrClient: jobmgrsvc.NewJobManagerServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		adminClient: adminsvc.NewAdminServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		dispatcher: dispatcher,
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	return &client, nil
}

// Cleanup ensures the client's YARPC dispatcher is stopped
func (c *Client) Cleanup() {
	defer c.cancelFunc()
	c.dispatcher.Stop()
}
