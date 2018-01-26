package tracked

import (
	"context"
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	log "github.com/sirupsen/logrus"
)

func (t *task) sendLaunchInfoToResMgr(ctx context.Context) error {
	var launchedTaskList []*peloton.TaskID

	taskID := &peloton.TaskID{
		Value: fmt.Sprintf("%s-%d", t.job.ID().GetValue(), t.ID()),
	}

	launchedTaskList = append(launchedTaskList, taskID)

	req := &resmgrsvc.MarkTasksLaunchedRequest{
		Tasks: launchedTaskList,
	}
	_, err := t.job.m.resmgrClient.MarkTasksLaunched(ctx, req)

	return err
}

func (t *task) launchRetry(ctx context.Context) error {
	log.WithField("job_id", t.job.ID().GetValue()).
		WithField("instance_id", t.ID()).
		Info("task launch timed out, reinitializing the task")
	t.job.m.mtx.taskMetrics.TaskLaunchTimeout.Inc(1)
	return t.initialize(ctx)
}
