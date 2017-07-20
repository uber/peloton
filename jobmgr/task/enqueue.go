package task

import (
	"context"
	"time"

	er "github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/util"
)

// EnqueueGangs enqueues all tasks organized in gangs to respool in resmgr.
func EnqueueGangs(ctx context.Context, tasks []*task.TaskInfo, config *job.JobConfig, client resmgrsvc.ResourceManagerServiceYARPCClient) error {
	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	gangs := util.ConvertToResMgrGangs(tasks, config)
	var request = &resmgrsvc.EnqueueGangsRequest{
		Gangs:   gangs,
		ResPool: config.RespoolID,
	}

	response, err := client.EnqueueGangs(ctxWithTimeout, request)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return err
	}
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"error": response.Error,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return er.New(response.Error.String())
	}
	log.WithField("Count", len(tasks)).Debug("Enqueued tasks as gangs to " +
		"Resource Manager")
	return nil
}
