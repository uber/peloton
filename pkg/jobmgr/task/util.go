// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"encoding/base64"
	"time"

	mesos_v1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/taskconfig"
	"github.com/uber/peloton/pkg/common/util"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
	taskutil "github.com/uber/peloton/pkg/jobmgr/util/task"

	log "github.com/sirupsen/logrus"
)

const _initialRunID = 1

// CreateInitializingTask for insertion into the storage layer, before being
// enqueued.
func CreateInitializingTask(jobID *peloton.JobID, instanceID uint32, jobConfig *job.JobConfig) *task.RuntimeInfo {
	mesosTaskID := util.CreateMesosTaskID(jobID, instanceID, _initialRunID)
	healthState := taskutil.GetInitialHealthState(taskconfig.Merge(
		jobConfig.GetDefaultConfig(),
		jobConfig.GetInstanceConfig()[instanceID]))

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

// GetDefaultPodGoalState from the job type.
func GetDefaultPodGoalState(jobType job.JobType) pod.PodState {
	switch jobType {
	case job.JobType_SERVICE:
		return pod.PodState_POD_STATE_RUNNING

	default:
		return pod.PodState_POD_STATE_SUCCEEDED
	}
}

// KillOrphanTask kills a non-stateful Mesos task with unterminated state
func KillOrphanTask(
	ctx context.Context,
	lm lifecyclemgr.Manager,
	taskInfo *task.TaskInfo,
) error {

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
			err = lm.ShutdownExecutor(
				ctx,
				mesosTaskID.GetValue(),
				agentID.GetValue(),
				nil,
			)
		} else {
			err = lm.Kill(
				ctx,
				mesosTaskID.GetValue(),
				"",
				nil,
			)
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

// CreateV1AlphaSecretProto creates v1alpha secret proto
// message from secret-id, path and data
func CreateV1AlphaSecretProto(id, path string, data []byte) *v1alphapeloton.Secret {
	// base64 encode the secret data
	if len(data) > 0 {
		data = []byte(base64.StdEncoding.EncodeToString(data))
	}
	return &v1alphapeloton.Secret{
		SecretId: &v1alphapeloton.SecretID{
			Value: id,
		},
		Path: path,
		Value: &v1alphapeloton.Secret_Value{
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
