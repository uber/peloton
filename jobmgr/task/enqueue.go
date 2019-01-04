package task

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	jobmgrcommon "github.com/uber/peloton/jobmgr/common"
	taskutil "github.com/uber/peloton/util/task"
)

// EnqueueGangs enqueues all tasks organized in gangs to respool in resmgr.
func EnqueueGangs(
	ctx context.Context,
	tasks []*task.TaskInfo,
	jobConfig jobmgrcommon.JobConfig,
	client resmgrsvc.ResourceManagerServiceYARPCClient) error {
	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	gangs := taskutil.ConvertToResMgrGangs(tasks, jobConfig)
	var request = &resmgrsvc.EnqueueGangsRequest{
		Gangs:   gangs,
		ResPool: jobConfig.GetRespoolID(),
	}

	response, err := client.EnqueueGangs(ctxWithTimeout, request)
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
