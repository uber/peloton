package task

import (
	"context"
	"time"

	log "github.com/Sirupsen/logrus"
	er "github.com/pkg/errors"

	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/util"
)

// EnqueueTasks enqueues all tasks to respool in resmgr.
func EnqueueTasks(tasks []*task.TaskInfo, config *job.JobConfig, client json.Client) error {
	rootCtx := context.Background()
	ctx, cancelFunc := context.WithTimeout(rootCtx, 10*time.Second)
	defer cancelFunc()
	resmgrTasks := ConvertToResMgrTask(tasks, config)
	var response resmgrsvc.EnqueueTasksResponse
	var request = &resmgrsvc.EnqueueTasksRequest{
		Tasks:   resmgrTasks,
		ResPool: config.RespoolID,
	}
	_, err := client.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueTasks"),
		request,
		&response,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return err
	}
	if response.Error != nil {
		log.WithFields(log.Fields{
			"error": response.Error,
			"tasks": tasks,
		}).Error("Resource Manager Enqueue Failed")
		return er.New(response.Error.String())
	}
	log.WithField("Count", len(resmgrTasks)).Debug("Enqueued tasks to " +
		"Resource Manager")
	return nil
}

// ConvertToResMgrTask converts taskinfo to resmgr task
func ConvertToResMgrTask(
	tasks []*task.TaskInfo,
	config *job.JobConfig) []*resmgr.Task {

	var resmgrtasks []*resmgr.Task
	for _, t := range tasks {
		resmgrtasks = append(
			resmgrtasks,
			util.ConvertTaskToResMgrTask(t, config),
		)
	}
	return resmgrtasks
}
