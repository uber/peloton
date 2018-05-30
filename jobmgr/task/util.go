package task

import (
	"context"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

const (
	// timeout for the orphan task kill call
	_defaultKillTaskActionTimeout = 5 * time.Second
)

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) *task.RuntimeInfo {
	runtime := util.RegenerateMesosTaskID(jobID, instanceID, nil)
	runtime.ConfigVersion = jobConfig.GetChangeLog().GetVersion()
	runtime.DesiredConfigVersion = jobConfig.GetChangeLog().GetVersion()
	runtime.GoalState = GetDefaultTaskGoalState(jobConfig.GetType())
	return runtime
}

// GetDefaultTaskGoalState from the job type.
func GetDefaultTaskGoalState(jobType job.JobType) task.TaskState {
	switch jobType {
	case job.JobType_SERVICE:
		return task.TaskState_RUNNING

	default:
		return task.TaskState_SUCCEEDED
	}
}

// KillOrphanTask kills a non-stateful Mesos task with unterminated state
func KillOrphanTask(
	ctx context.Context,
	hostmgrClient hostsvc.InternalHostServiceYARPCClient,
	taskInfo *task.TaskInfo) error {

	// TODO(chunyang.shen): store the stateful info into cache instead of going to DB to fetch config
	if util.IsTaskHasValidVolume(taskInfo) {
		// Do not kill stateful orphan task.
		return nil
	}

	state := taskInfo.GetRuntime().GetState()
	mesosTaskID := taskInfo.GetRuntime().GetMesosTaskId()
	agentID := taskInfo.GetRuntime().GetAgentID()

	// Only kill task if state is not terminal.
	if !util.IsPelotonStateTerminal(state) && mesosTaskID != nil {
		var err error
		if state == task.TaskState_KILLING {
			err = ShutdownMesosExecutor(ctx, hostmgrClient, mesosTaskID, agentID)
		} else {
			err = KillTask(ctx, hostmgrClient, mesosTaskID)
		}
		if err != nil {
			log.WithError(err).
				WithField("orphan_task_id", mesosTaskID).
				Error("failed to kill orphan task")
		}
		return err
	}
	return nil
}

// KillTask kills a task given its mesos task ID
func KillTask(ctx context.Context, hostmgrClient hostsvc.InternalHostServiceYARPCClient, taskID *mesos_v1.TaskID) error {
	newCtx := ctx
	_, ok := ctx.Deadline()
	if !ok {
		var cancelFunc context.CancelFunc
		newCtx, cancelFunc = context.WithTimeout(context.Background(), _defaultKillTaskActionTimeout)
		defer cancelFunc()
	}
	req := &hostsvc.KillTasksRequest{
		TaskIds: []*mesos_v1.TaskID{taskID},
	}
	res, err := hostmgrClient.KillTasks(newCtx, req)
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

// ShutdownMesosExecutor shutdown a executor given its executor ID and agent ID
func ShutdownMesosExecutor(
	ctx context.Context,
	hostmgrClient hostsvc.InternalHostServiceYARPCClient,
	taskID *mesos_v1.TaskID,
	agentID *mesos_v1.AgentID) error {

	req := &hostsvc.ShutdownExecutorsRequest{
		Executors: []*hostsvc.ExecutorOnAgent{
			{
				ExecutorId: &mesos_v1.ExecutorID{Value: taskID.Value},
				AgentId:    agentID,
			},
		},
	}

	res, err := hostmgrClient.ShutdownExecutors(ctx, req)

	if err != nil {
		return err
	} else if e := res.GetError(); e != nil {
		switch {
		case e.ShutdownFailure != nil:
			return fmt.Errorf(e.ShutdownFailure.Message)
		case e.InvalidExecutors != nil:
			return fmt.Errorf(e.InvalidExecutors.Message)
		default:
			return fmt.Errorf(e.String())
		}
	}

	return nil
}

// CreateSecretVolume builds a mesos volume of type secret
// from the given secret path and secret value string
// This volume will be added to the job's default config
func CreateSecretVolume(secretPath string, secretStr string) *mesos_v1.Volume {
	volumeMode := mesos_v1.Volume_RO
	volumeSourceType := mesos_v1.Volume_Source_SECRET
	secretType := mesos_v1.Secret_VALUE
	return &mesos_v1.Volume{
		Mode:          &volumeMode,
		ContainerPath: &secretPath,
		Source: &mesos_v1.Volume_Source{
			Type: &volumeSourceType,
			Secret: &mesos_v1.Secret{
				Type: &secretType,
				Value: &mesos_v1.Secret_Value{
					Data: []byte(secretStr),
				},
			},
		},
	}
}
