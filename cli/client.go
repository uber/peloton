package cli

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"

	hostsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/host/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	updatesvc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update/svc"
	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/v0/volume/svc"
	statlesssvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless/svc"
	podsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
	hostmgr_svc "code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jobClient       job.JobManagerYARPCClient
	taskClient      task.TaskManagerYARPCClient
	podClient       podsvc.PodServiceYARPCClient
	statelessClient statlesssvc.JobServiceYARPCClient
	resClient       respool.ResourceManagerYARPCClient
	resMgrClient    resmgrsvc.ResourceManagerServiceYARPCClient
	updateClient    updatesvc.UpdateServiceYARPCClient
	volumeClient    volume_svc.VolumeServiceYARPCClient
	hostMgrClient   hostmgr_svc.InternalHostServiceYARPCClient
	hostClient      hostsvc.HostServiceYARPCClient
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

	t := http.NewTransport()

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonCLI,
		Outbounds: yarpc.Outbounds{
			common.PelotonJobManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(jobmgrURL.String()),
			},
			common.PelotonResourceManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(resmgrURL.String()),
			},
			common.PelotonHostManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(hostmgrURL.String()),
			},
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
		hostClient: hostsvc.NewHostServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager),
		),
		podClient: podsvc.NewPodServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		statelessClient: statlesssvc.NewJobServiceYARPCClient(
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
