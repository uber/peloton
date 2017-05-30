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
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/util"
)

// EnqueueGangs enqueues all tasks organized in gangs to respool in resmgr.
func EnqueueGangs(ctx context.Context, tasks []*task.TaskInfo, config *job.JobConfig, client json.Client) error {
	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	gangs := ConvertToResMgrGangs(tasks, config)
	var request = &resmgrsvc.EnqueueGangsRequest{
		Gangs:   gangs,
		ResPool: config.RespoolID,
	}
	var response resmgrsvc.EnqueueGangsResponse

	_, err := client.Call(
		ctxWithTimeout,
		yarpc.NewReqMeta().Procedure("ResourceManagerService.EnqueueGangs"),
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
	log.WithField("Count", len(tasks)).Debug("Enqueued tasks as gangs to " +
		"Resource Manager")
	return nil
}

// ConvertToResMgrGangs converts the taskinfo for the tasks comprising
// the config job to resmgr tasks and organizes them into gangs, each
// of which is a set of 1+ tasks to be admitted and placed as a group.
func ConvertToResMgrGangs(
	tasks []*task.TaskInfo,
	config *job.JobConfig) []*resmgrsvc.Gang {
	var gangs []*resmgrsvc.Gang

	// Gangs of multiple tasks are placed at the front of the returned list for
	// preferential treatment, since they are expected to be both more important
	// and harder to place than gangs comprising a single task.
	var multiTaskGangs []*resmgrsvc.Gang

	for _, t := range tasks {
		resmgrtask := util.ConvertTaskToResMgrTask(t, config)
		// Currently a job has at most 1 gang comprising multiple tasks;
		// those tasks have their MinInstances field set > 1.
		if resmgrtask.MinInstances > 1 {
			if len(multiTaskGangs) == 0 {
				var multiTaskGang resmgrsvc.Gang
				multiTaskGangs = append(multiTaskGangs, &multiTaskGang)
			}
			multiTaskGangs[0].Tasks = append(multiTaskGangs[0].Tasks, resmgrtask)
		} else {
			// Gang comprising one task
			var gang resmgrsvc.Gang
			gang.Tasks = append(gang.Tasks, resmgrtask)
			gangs = append(gangs, &gang)
		}
	}
	if len(multiTaskGangs) > 0 {
		gangs = append(multiTaskGangs, gangs...)
	}
	return gangs
}
