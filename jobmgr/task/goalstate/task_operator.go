package goalstate

import (
	"context"
	"fmt"

	"go.uber.org/yarpc"

	"github.com/pkg/errors"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/util"
	"github.com/sirupsen/logrus"
)

// TaskOperator can perform operations on a task, potentially resulting in the
// task changes state.
type TaskOperator interface {
	// StopTask by issueing a kill request. Even if the call is succesfull, the
	// task is not guaranteed to be killed.
	StopTask(ctx context.Context, runtime *task.RuntimeInfo) error
}

// NewTaskOperator from the set of arguments..
func NewTaskOperator(d *yarpc.Dispatcher) TaskOperator {
	return &taskOperator{
		hostmgrClient: hostsvc.NewInternalHostServiceYARPCClient(d.ClientConfig(common.PelotonHostManager)),
		resmgrClient:  resmgrsvc.NewResourceManagerServiceYARPCClient(d.ClientConfig(common.PelotonResourceManager)),
	}
}

type taskOperator struct {
	hostmgrClient hostsvc.InternalHostServiceYARPCClient
	resmgrClient  resmgrsvc.ResourceManagerServiceYARPCClient
}

//
func (o *taskOperator) StopTask(ctx context.Context, runtime *task.RuntimeInfo) error {
	// We need to parse the peloton task id from Mesos taskId
	pelotonTaskID, err := util.ParseTaskIDFromMesosTaskID(
		runtime.MesosTaskId.GetValue())
	if err != nil {
		return err
	}

	taskID := &peloton.TaskID{
		Value: pelotonTaskID,
	}
	killReq := &resmgrsvc.KillTasksRequest{
		Tasks: []*peloton.TaskID{taskID},
	}
	// Calling resmgr Kill API
	killRes, err := o.resmgrClient.KillTasks(ctx, killReq)
	if err != nil {
		logrus.WithError(err).Error("Error in killing from resmgr")
	}

	if killRes.Error != nil {
		logrus.WithError(errors.New(killRes.GetError().Message)).
			Error("Error in killing from resmgr")
	}

	req := &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{runtime.GetMesosTaskId()},
	}
	res, err := o.hostmgrClient.KillTasks(ctx, req)
	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.KillFailure != nil:
			return fmt.Errorf(e.KillFailure.Message)
		case e.InvalidTaskIDs != nil:
			return fmt.Errorf(e.InvalidTaskIDs.Message)
		default:
			return fmt.Errorf(e.String())
		}
	}

	return nil
}
