package task

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"code.uber.internal/infra/peloton/.gen/mesos/v1"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"code.uber.internal/infra/peloton/common"
	"code.uber.internal/infra/peloton/common/taskconfig"
	"code.uber.internal/infra/peloton/util"

	log "github.com/sirupsen/logrus"
)

const (
	// timeout for the orphan task kill call
	_defaultKillTaskActionTimeout = 5 * time.Second
	_initialRunID                 = 1
)

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) *task.RuntimeInfo {
	mesosTaskID := util.CreateMesosTaskID(jobID, instanceID, _initialRunID)
	// Get the health check config
	healthCheckConfig := taskconfig.Merge(
		jobConfig.GetDefaultConfig(),
		jobConfig.GetInstanceConfig()[instanceID]).GetHealthCheck()

	var healthState task.HealthState

	// The initial health state is UNKNOWN or DISABLED
	// depends on health check is enabled or  not
	if healthCheckConfig != nil {
		healthState = task.HealthState_HEALTH_UNKNOWN
	} else {
		healthState = task.HealthState_DISABLED
	}

	runtime := &task.RuntimeInfo{
		MesosTaskId:          mesosTaskID,
		DesiredMesosTaskId:   mesosTaskID,
		State:                task.TaskState_INITIALIZED,
		ConfigVersion:        jobConfig.GetChangeLog().GetVersion(),
		DesiredConfigVersion: jobConfig.GetChangeLog().GetVersion(),
		GoalState:            GetDefaultTaskGoalState(jobConfig.GetType()),
		ResourceUsage:        CreateEmptyResourceUsageMap(),
		Healthy:              healthState,
	}
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

// CreateSecretsFromVolumes creates secret proto message list from the given
// list of secret volumes.
func CreateSecretsFromVolumes(
	secretVolumes []*mesos_v1.Volume) []*peloton.Secret {
	secrets := []*peloton.Secret{}
	for _, volume := range secretVolumes {
		secrets = append(secrets, CreateSecretProto(
			string(volume.GetSource().GetSecret().GetValue().GetData()),
			volume.GetContainerPath(), nil))
	}
	return secrets
}

// CreateSecretProto creates secret proto message from secret-id, path and data
func CreateSecretProto(id, path string, data []byte) *peloton.Secret {
	// base64 encode the secret data
	if len(data) > 0 {
		data = []byte(base64.StdEncoding.EncodeToString(data))
	}
	return &peloton.Secret{
		Id: &peloton.SecretID{
			Value: id,
		},
		Path: path,
		Value: &peloton.Secret_Value{
			Data: data,
		},
	}
}

// CreateEmptyResourceUsageMap creates a resource usage map with usage stats
// initialized to 0
func CreateEmptyResourceUsageMap() map[string]float64 {
	return map[string]float64{
		common.CPU:    float64(0),
		common.GPU:    float64(0),
		common.MEMORY: float64(0),
	}
}

// CreateResourceUsageMap creates a resource usage map with usage stats
// calculated as resource limit * duration
func CreateResourceUsageMap(
	resourceConfig *task.ResourceConfig,
	startTimeStr, completionTimeStr string) (map[string]float64, error) {
	cpulimit := resourceConfig.GetCpuLimit()
	gpulimit := resourceConfig.GetGpuLimit()
	memlimit := resourceConfig.GetMemLimitMb()
	resourceUsage := CreateEmptyResourceUsageMap()

	// if start time is "", it means the task did not start so resource usage
	// should be 0 for all resources
	if startTimeStr == "" {
		return resourceUsage, nil
	}

	startTime, err := time.Parse(time.RFC3339Nano, startTimeStr)
	if err != nil {
		return nil, err
	}
	completionTime, err := time.Parse(time.RFC3339Nano, completionTimeStr)
	if err != nil {
		return nil, err
	}

	startTimeUnix := float64(startTime.UnixNano()) /
		float64(time.Second/time.Nanosecond)
	completionTimeUnix := float64(completionTime.UnixNano()) /
		float64(time.Second/time.Nanosecond)

	// update the resource usage map for CPU, GPU and memory usage
	resourceUsage[common.CPU] = (completionTimeUnix - startTimeUnix) * cpulimit
	resourceUsage[common.GPU] = (completionTimeUnix - startTimeUnix) * gpulimit
	resourceUsage[common.MEMORY] =
		(completionTimeUnix - startTimeUnix) * memlimit
	return resourceUsage, nil
}
