package cli

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/leader"

	"code.uber.internal/infra/peloton/common"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/transport/http"
)

// Client is a JSON Client with associated dispatcher and context
type Client struct {
	jobClient  job.JobManagerYarpcClient
	taskClient task.TaskManagerYarpcClient
	resClient  respool.ResourceManagerYarpcClient
	dispatcher *yarpc.Dispatcher
	ctx        context.Context
	cancelFunc context.CancelFunc
	// Debug is whether debug output is enabled
	Debug bool
}

// New returns a new RPC client given a framework URL and timeout and error
func New(
	discovery leader.Discovery,
	timeout time.Duration,
	debug bool) (*Client, error) {

	jobmgrURL, err := discovery.GetJobMgrURL()
	if err != nil {
		return nil, err
	}

	resmgrURL, err := discovery.GetResMgrURL()
	if err != nil {
		return nil, err
	}

	t := http.NewTransport()

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: common.PelotonClient,
		Outbounds: yarpc.Outbounds{
			common.PelotonJobManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(jobmgrURL.String()),
			},
			common.PelotonResourceManager: transport.Outbounds{
				Unary: t.NewSingleOutbound(resmgrURL.String()),
			},
		},
	})

	if err := dispatcher.Start(); err != nil {
		return nil, fmt.Errorf("Unable to start dispatcher: %v", err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	client := Client{
		Debug:      debug,
		jobClient:  job.NewJobManagerYarpcClient(dispatcher.ClientConfig(common.PelotonJobManager)),
		taskClient: task.NewTaskManagerYarpcClient(dispatcher.ClientConfig(common.PelotonJobManager)),
		resClient:  respool.NewResourceManagerYarpcClient(dispatcher.ClientConfig(common.PelotonResourceManager)),
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
