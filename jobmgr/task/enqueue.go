package task

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgrsvc"

	"code.uber.internal/infra/peloton/util"
)

// EnqueueGangs enqueues all tasks organized in gangs to respool in resmgr.
func EnqueueGangs(ctx context.Context, tasks []*task.TaskInfo, respoolID *peloton.ResourcePoolID, sla *job.SlaConfig, client resmgrsvc.ResourceManagerServiceYARPCClient) error {
	ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	gangs := util.ConvertToResMgrGangs(tasks, sla)
	var request = &resmgrsvc.EnqueueGangsRequest{
		Gangs:   gangs,
		ResPool: respoolID,
	}

	response, err := client.EnqueueGangs(ctx, request)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"request": request,
		}).Error("resource manager enqueue gangs failed")
		return err
	}
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"resp_error": response.GetError().String(),
			"request":    request,
		}).Error("resource manager enqueue gangs response error")
		return errors.New(response.GetError().String())
	}
	log.WithField("count", len(tasks)).
		Debug("Enqueued tasks as gangs to Resource Manager")
	return nil
}
