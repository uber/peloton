package tracked

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"
)

func (t *task) stop(ctx context.Context) error {
	t.Lock()
	runtime := t.runtime
	t.Unlock()

	if runtime == nil {
		return fmt.Errorf("tracked task has no runtime info assigned")
	}

	// TODO: Add when we can detect initialized.
	if false {
		taskID := &peloton.TaskID{
			Value: fmt.Sprintf("%s-%d", t.job.ID(), t.ID()),
		}
		killReq := &resmgrsvc.KillTasksRequest{
			Tasks: []*peloton.TaskID{taskID},
		}

		// Calling resmgr Kill API
		killRes, err := t.job.m.resmgrClient.KillTasks(ctx, killReq)
		if err != nil {
			log.WithError(err).Error("Error in killing from resmgr")
		}

		if killRes.Error != nil {
			log.WithError(errors.New(killRes.GetError().Message)).
				Error("Error in killing from resmgr")
		}
	}

	req := &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{runtime.GetMesosTaskId()},
	}
	res, err := t.job.m.hostmgrClient.KillTasks(ctx, req)
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
