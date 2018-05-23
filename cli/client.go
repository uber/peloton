package cli

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	volume_svc "code.uber.internal/infra/peloton/.gen/peloton/api/volume/svc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/leader"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jobClient     job.JobManagerYARPCClient
	taskClient    task.TaskManagerYARPCClient
	resClient     respool.ResourceManagerYARPCClient
	resMgrClient  resmgrsvc.ResourceManagerServiceYARPCClient
	volumeClient  volume_svc.VolumeServiceYARPCClient
	hostMgrClient hostsvc.InternalHostServiceYARPCClient
	dispatcher    *yarpc.Dispatcher
	ctx           context.Context
	cancelFunc    context.CancelFunc
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
		volumeClient: volume_svc.NewVolumeServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonJobManager),
		),
		hostMgrClient: hostsvc.NewInternalHostServiceYARPCClient(
			dispatcher.ClientConfig(common.PelotonHostManager),
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
