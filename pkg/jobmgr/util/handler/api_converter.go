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

package handler

import (
	"reflect"

	mesosv1 "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/job"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pelotonv0query "github.com/uber/peloton/.gen/peloton/api/v0/query"
	pelotonv0respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/api/v0/update"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/apachemesos"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/query"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/volume"
	"github.com/uber/peloton/.gen/peloton/private/models"

	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"

	"go.uber.org/yarpc/yarpcerrors"
)

// ConvertTaskStateToPodState converts v0 task.TaskState to v1alpha pod.PodState
func ConvertTaskStateToPodState(state task.TaskState) pod.PodState {
	switch state {
	case task.TaskState_UNKNOWN:
		return pod.PodState_POD_STATE_INVALID
	case task.TaskState_INITIALIZED:
		return pod.PodState_POD_STATE_INITIALIZED
	case task.TaskState_PENDING:
		return pod.PodState_POD_STATE_PENDING
	case task.TaskState_READY:
		return pod.PodState_POD_STATE_READY
	case task.TaskState_PLACING:
		return pod.PodState_POD_STATE_PLACING
	case task.TaskState_PLACED:
		return pod.PodState_POD_STATE_PLACED
	case task.TaskState_LAUNCHING:
		return pod.PodState_POD_STATE_LAUNCHING
	case task.TaskState_LAUNCHED:
		return pod.PodState_POD_STATE_LAUNCHED
	case task.TaskState_STARTING:
		return pod.PodState_POD_STATE_STARTING
	case task.TaskState_RUNNING:
		return pod.PodState_POD_STATE_RUNNING
	case task.TaskState_SUCCEEDED:
		return pod.PodState_POD_STATE_SUCCEEDED
	case task.TaskState_FAILED:
		return pod.PodState_POD_STATE_FAILED
	case task.TaskState_LOST:
		return pod.PodState_POD_STATE_LOST
	case task.TaskState_PREEMPTING:
		return pod.PodState_POD_STATE_PREEMPTING
	case task.TaskState_KILLING:
		return pod.PodState_POD_STATE_KILLING
	case task.TaskState_KILLED:
		return pod.PodState_POD_STATE_KILLED
	case task.TaskState_DELETED:
		return pod.PodState_POD_STATE_DELETED
	case task.TaskState_RESERVED:
		return pod.PodState_POD_STATE_RESERVED
	}
	return pod.PodState_POD_STATE_INVALID
}

// ConvertPodStateToTaskState converts v0 task.TaskState to v1alpha pod.PodState
func ConvertPodStateToTaskState(state pod.PodState) task.TaskState {
	switch state {
	case pod.PodState_POD_STATE_INVALID:
		return task.TaskState_UNKNOWN
	case pod.PodState_POD_STATE_INITIALIZED:
		return task.TaskState_INITIALIZED
	case pod.PodState_POD_STATE_PENDING:
		return task.TaskState_PENDING
	case pod.PodState_POD_STATE_READY:
		return task.TaskState_READY
	case pod.PodState_POD_STATE_PLACING:
		return task.TaskState_PLACING
	case pod.PodState_POD_STATE_PLACED:
		return task.TaskState_PLACED
	case pod.PodState_POD_STATE_LAUNCHING:
		return task.TaskState_LAUNCHING
	case pod.PodState_POD_STATE_LAUNCHED:
		return task.TaskState_LAUNCHED
	case pod.PodState_POD_STATE_STARTING:
		return task.TaskState_STARTING
	case pod.PodState_POD_STATE_RUNNING:
		return task.TaskState_RUNNING
	case pod.PodState_POD_STATE_SUCCEEDED:
		return task.TaskState_SUCCEEDED
	case pod.PodState_POD_STATE_FAILED:
		return task.TaskState_FAILED
	case pod.PodState_POD_STATE_LOST:
		return task.TaskState_LOST
	case pod.PodState_POD_STATE_PREEMPTING:
		return task.TaskState_PREEMPTING
	case pod.PodState_POD_STATE_KILLING:
		return task.TaskState_KILLING
	case pod.PodState_POD_STATE_KILLED:
		return task.TaskState_KILLED
	case pod.PodState_POD_STATE_DELETED:
		return task.TaskState_DELETED
	case pod.PodState_POD_STATE_RESERVED:
		return task.TaskState_RESERVED
	}
	return task.TaskState_UNKNOWN
}

// ConvertV1InstanceRangeToV0InstanceRange converts from array of
// v1 pod.InstanceIDRange to array of v0 task.InstanceRange
func ConvertV1InstanceRangeToV0InstanceRange(
	instanceRange []*pod.InstanceIDRange) []*task.InstanceRange {
	var resp []*task.InstanceRange
	for _, inst := range instanceRange {
		r := &task.InstanceRange{
			From: inst.GetFrom(),
			To:   inst.GetTo(),
		}
		resp = append(resp, r)
	}
	return resp
}

// ConvertTaskRuntimeToPodStatus converts
// v0 task.RuntimeInfo to v1alpha pod.PodStatus
func ConvertTaskRuntimeToPodStatus(runtime *task.RuntimeInfo) *pod.PodStatus {
	return &pod.PodStatus{
		State:          ConvertTaskStateToPodState(runtime.GetState()),
		PodId:          &v1alphapeloton.PodID{Value: runtime.GetMesosTaskId().GetValue()},
		StartTime:      runtime.GetStartTime(),
		CompletionTime: runtime.GetCompletionTime(),
		Host:           runtime.GetHost(),
		ContainersStatus: []*pod.ContainerStatus{
			{
				Ports: runtime.GetPorts(),
				Healthy: &pod.HealthStatus{
					State: pod.HealthState(runtime.GetHealthy()),
				},
				StartTime:      runtime.GetStartTime(),
				CompletionTime: runtime.GetCompletionTime(),
				Message:        runtime.GetMessage(),
				Reason:         runtime.GetReason(),
				TerminationStatus: convertTaskTerminationStatusToPodTerminationStatus(
					runtime.TerminationStatus),
			},
		},
		DesiredState:   ConvertTaskStateToPodState(runtime.GetGoalState()),
		Message:        runtime.GetMessage(),
		Reason:         runtime.GetReason(),
		FailureCount:   runtime.GetFailureCount(),
		VolumeId:       &v1alphapeloton.VolumeID{Value: runtime.GetVolumeID().GetValue()},
		Version:        versionutil.GetPodEntityVersion(runtime.GetConfigVersion()),
		DesiredVersion: versionutil.GetPodEntityVersion(runtime.GetDesiredConfigVersion()),
		AgentId:        runtime.GetAgentID(),
		HostId:         runtime.GetAgentID().GetValue(),
		Revision: &v1alphapeloton.Revision{
			Version:   runtime.GetRevision().GetVersion(),
			CreatedAt: runtime.GetRevision().GetCreatedAt(),
			UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
			UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
		},
		PrevPodId:     &v1alphapeloton.PodID{Value: runtime.GetPrevMesosTaskId().GetValue()},
		ResourceUsage: runtime.GetResourceUsage(),
		DesiredPodId:  &v1alphapeloton.PodID{Value: runtime.GetDesiredMesosTaskId().GetValue()},
		DesiredHost:   runtime.GetDesiredHost(),
	}
}

// ConvertTaskConfigToPodSpec converts v0 task.TaskConfig to v1alpha pod.PodSpec
func ConvertTaskConfigToPodSpec(taskConfig *task.TaskConfig, jobID string, instanceID uint32) *pod.PodSpec {
	result := &pod.PodSpec{
		Controller:             taskConfig.GetController(),
		KillGracePeriodSeconds: taskConfig.GetKillGracePeriodSeconds(),
		Revocable:              taskConfig.GetRevocable(),
	}

	if len(jobID) != 0 {
		result.PodName = &v1alphapeloton.PodName{
			Value: util.CreatePelotonTaskID(jobID, instanceID),
		}
	}

	if taskConfig.GetConstraint() != nil {
		result.Constraint = ConvertTaskConstraintsToPodConstraints([]*task.Constraint{taskConfig.GetConstraint()})[0]
	}

	if taskConfig.GetVolume() != nil {
		result.Volume = &pod.PersistentVolumeSpec{
			ContainerPath: taskConfig.GetVolume().GetContainerPath(),
			SizeMb:        taskConfig.GetVolume().GetSizeMB(),
		}
	}

	if taskConfig.GetLabels() != nil {
		result.Labels = ConvertLabels(taskConfig.GetLabels())
	}

	if taskConfig.GetPreemptionPolicy() != nil {
		result.PreemptionPolicy = &pod.PreemptionPolicy{
			KillOnPreempt: taskConfig.GetPreemptionPolicy().GetKillOnPreempt(),
		}
	}

	if taskConfig.GetRestartPolicy() != nil {
		result.RestartPolicy = &pod.RestartPolicy{
			MaxFailures: taskConfig.GetRestartPolicy().GetMaxFailures(),
		}
	}

	container := &pod.ContainerSpec{}
	if len(taskConfig.GetName()) != 0 {
		container.Name = taskConfig.GetName()
	}

	if taskConfig.GetResource() != nil {
		container.Resource = &pod.ResourceSpec{
			CpuLimit:    taskConfig.GetResource().GetCpuLimit(),
			MemLimitMb:  taskConfig.GetResource().GetMemLimitMb(),
			DiskLimitMb: taskConfig.GetResource().GetDiskLimitMb(),
			FdLimit:     taskConfig.GetResource().GetFdLimit(),
			GpuLimit:    taskConfig.GetResource().GetGpuLimit(),
		}
	}

	if taskConfig.GetContainer() != nil {
		container.Container = taskConfig.GetContainer()
		ConvertMesosContainerToPodSpec(taskConfig.GetContainer(), result, container)
	}

	if taskConfig.GetCommand() != nil {
		container.Command = taskConfig.GetCommand()
		ConvertMesosCommandToPodSpec(taskConfig.GetCommand(), result, container)
	}

	if taskConfig.GetExecutor() != nil {
		container.Executor = taskConfig.GetExecutor()
		ConvertMesosExecutorInfoToPodSpec(taskConfig.GetExecutor(), result, container)
	}

	if taskConfig.GetPorts() != nil {
		container.Ports = ConvertPortConfigsToPortSpecs(taskConfig.GetPorts())
	}

	if taskConfig.GetHealthCheck() != nil {
		container.LivenessCheck = &pod.HealthCheckSpec{
			Enabled:                taskConfig.GetHealthCheck().GetEnabled(),
			InitialIntervalSecs:    taskConfig.GetHealthCheck().GetInitialIntervalSecs(),
			IntervalSecs:           taskConfig.GetHealthCheck().GetIntervalSecs(),
			MaxConsecutiveFailures: taskConfig.GetHealthCheck().GetMaxConsecutiveFailures(),
			TimeoutSecs:            taskConfig.GetHealthCheck().GetTimeoutSecs(),
			Type:                   pod.HealthCheckSpec_HealthCheckType(taskConfig.GetHealthCheck().GetType()),
		}

		if taskConfig.GetHealthCheck().GetCommandCheck() != nil {
			container.LivenessCheck.CommandCheck = &pod.HealthCheckSpec_CommandCheck{
				Command:             taskConfig.GetHealthCheck().GetCommandCheck().GetCommand(),
				UnshareEnvironments: taskConfig.GetHealthCheck().GetCommandCheck().GetUnshareEnvironments(),
			}
		}

		if taskConfig.GetHealthCheck().GetHttpCheck() != nil {
			container.LivenessCheck.HttpCheck = &pod.HealthCheckSpec_HTTPCheck{
				Scheme: taskConfig.GetHealthCheck().GetHttpCheck().GetScheme(),
				Port:   taskConfig.GetHealthCheck().GetHttpCheck().GetPort(),
				Path:   taskConfig.GetHealthCheck().GetHttpCheck().GetPath(),
			}
		}
	}

	if !reflect.DeepEqual(*container, pod.ContainerSpec{}) {
		result.Containers = []*pod.ContainerSpec{container}
	}

	return result
}

// ConvertLabels converts v0 peloton.Label array to
// v1alpha peloton.Label array
func ConvertLabels(labels []*peloton.Label) []*v1alphapeloton.Label {
	var podLabels []*v1alphapeloton.Label
	for _, l := range labels {
		podLabels = append(podLabels, &v1alphapeloton.Label{
			Key:   l.GetKey(),
			Value: l.GetValue(),
		})
	}
	return podLabels
}

// ConvertTaskConstraintsToPodConstraints converts v0 task.Constraint array to
// v1alpha pod.Constraint array
func ConvertTaskConstraintsToPodConstraints(constraints []*task.Constraint) []*pod.Constraint {
	var podConstraints []*pod.Constraint
	for _, constraint := range constraints {
		podConstraint := &pod.Constraint{
			Type: pod.Constraint_Type(constraint.GetType()),
		}

		if constraint.GetLabelConstraint() != nil {
			podConstraint.LabelConstraint = &pod.LabelConstraint{
				Kind: pod.LabelConstraint_Kind(
					constraint.GetLabelConstraint().GetKind(),
				),
				Condition: pod.LabelConstraint_Condition(
					constraint.GetLabelConstraint().GetCondition(),
				),
				Requirement: constraint.GetLabelConstraint().GetRequirement(),
			}

			if constraint.GetLabelConstraint().GetLabel() != nil {
				podConstraint.LabelConstraint.Label = &v1alphapeloton.Label{
					Key:   constraint.GetLabelConstraint().GetLabel().GetKey(),
					Value: constraint.GetLabelConstraint().GetLabel().GetValue(),
				}
			}
		}

		if constraint.GetAndConstraint() != nil {
			podConstraint.AndConstraint = &pod.AndConstraint{
				Constraints: ConvertTaskConstraintsToPodConstraints(constraint.GetAndConstraint().GetConstraints()),
			}
		}

		if constraint.GetOrConstraint() != nil {
			podConstraint.OrConstraint = &pod.OrConstraint{
				Constraints: ConvertTaskConstraintsToPodConstraints(constraint.GetOrConstraint().GetConstraints()),
			}
		}

		podConstraints = append(podConstraints, podConstraint)
	}
	return podConstraints
}

// ConvertPortConfigsToPortSpecs converts v0 task.PortConfig array to
// v1alpha pod.PortSpec array
func ConvertPortConfigsToPortSpecs(ports []*task.PortConfig) []*pod.PortSpec {
	var containerPorts []*pod.PortSpec
	for _, p := range ports {
		containerPorts = append(
			containerPorts,
			&pod.PortSpec{
				Name:    p.GetName(),
				Value:   p.GetValue(),
				EnvName: p.GetEnvName(),
			},
		)
	}
	return containerPorts
}

// ConvertV0SecretsToV1Secrets converts v0 peloton.Secret to v1alpha peloton.Secret
func ConvertV0SecretsToV1Secrets(secrets []*peloton.Secret) []*v1alphapeloton.Secret {
	var v1secrets []*v1alphapeloton.Secret
	for _, secret := range secrets {
		v1secret := &v1alphapeloton.Secret{
			SecretId: &v1alphapeloton.SecretID{
				Value: secret.GetId().GetValue(),
			},
			Path: secret.GetPath(),
			Value: &v1alphapeloton.Secret_Value{
				Data: secret.GetValue().GetData(),
			},
		}
		v1secrets = append(v1secrets, v1secret)
	}
	return v1secrets
}

// ConvertV1SecretsToV0Secrets converts v1alpha peloton.Secret to v0 peloton.Secret
func ConvertV1SecretsToV0Secrets(secrets []*v1alphapeloton.Secret) []*peloton.Secret {
	var v0secrets []*peloton.Secret
	for _, secret := range secrets {
		v0secret := &peloton.Secret{
			Id: &peloton.SecretID{
				Value: secret.GetSecretId().GetValue(),
			},
			Path: secret.GetPath(),
			Value: &peloton.Secret_Value{
				Data: secret.GetValue().GetData(),
			},
		}
		v0secrets = append(v0secrets, v0secret)
	}
	return v0secrets
}

// ConvertJobConfigToJobSpec converts v0 job.JobConfig to v1alpha stateless.JobSpec
func ConvertJobConfigToJobSpec(config *job.JobConfig) *stateless.JobSpec {
	instanceSpec := make(map[uint32]*pod.PodSpec)
	for instID, taskConfig := range config.GetInstanceConfig() {
		instanceSpec[instID] = ConvertTaskConfigToPodSpec(taskConfig, "", instID)
	}

	return &stateless.JobSpec{
		Revision: &v1alphapeloton.Revision{
			Version:   config.GetChangeLog().GetVersion(),
			CreatedAt: config.GetChangeLog().GetCreatedAt(),
			UpdatedAt: config.GetChangeLog().GetUpdatedAt(),
			UpdatedBy: config.GetChangeLog().GetUpdatedBy(),
		},
		Name:          config.GetName(),
		Owner:         config.GetOwner(),
		OwningTeam:    config.GetOwningTeam(),
		LdapGroups:    config.GetLdapGroups(),
		Description:   config.GetDescription(),
		Labels:        ConvertLabels(config.GetLabels()),
		InstanceCount: config.GetInstanceCount(),
		Sla:           ConvertSLAConfigToSLASpec(config.GetSLA()),
		DefaultSpec:   ConvertTaskConfigToPodSpec(config.GetDefaultConfig(), "", 0),
		InstanceSpec:  instanceSpec,
		RespoolId: &v1alphapeloton.ResourcePoolID{
			Value: config.GetRespoolID().GetValue()},
	}
}

// ConvertUpdateModelToWorkflowStatus converts private UpdateModel
// to v1alpha stateless.WorkflowStatus
func ConvertUpdateModelToWorkflowStatus(
	runtime *job.RuntimeInfo,
	updateInfo *models.UpdateModel,
) *stateless.WorkflowStatus {
	if updateInfo == nil {
		return nil
	}

	entityVersion := versionutil.GetJobEntityVersion(
		updateInfo.GetJobConfigVersion(),
		runtime.GetDesiredStateVersion(),
		runtime.GetWorkflowVersion(),
	)
	prevVersion := versionutil.GetJobEntityVersion(
		updateInfo.GetPrevJobConfigVersion(),
		runtime.GetDesiredStateVersion(),
		runtime.GetWorkflowVersion(),
	)

	return &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType(updateInfo.GetType()),
		State:                 stateless.WorkflowState(updateInfo.GetState()),
		PrevState:             stateless.WorkflowState(updateInfo.GetPrevState()),
		NumInstancesCompleted: updateInfo.GetInstancesDone(),
		NumInstancesRemaining: updateInfo.GetInstancesTotal() - updateInfo.GetInstancesDone() - updateInfo.GetInstancesFailed(),
		NumInstancesFailed:    updateInfo.GetInstancesFailed(),
		InstancesCurrent:      updateInfo.GetInstancesCurrent(),
		Version:               entityVersion,
		PrevVersion:           prevVersion,
		CreationTime:          updateInfo.GetCreationTime(),
		UpdateTime:            updateInfo.GetUpdateTime(),
		CompletionTime:        updateInfo.GetCompletionTime(),
	}
}

// ConvertRuntimeInfoToJobStatus converts v0 job.RuntimeInfo and private
// UpdateModel to v1alpha stateless.JobStatus
func ConvertRuntimeInfoToJobStatus(
	runtime *job.RuntimeInfo,
	updateInfo *models.UpdateModel,
) *stateless.JobStatus {
	result := &stateless.JobStatus{}
	podConfigVersionStats := make(map[string]*stateless.JobStatus_PodStateStats)
	result.Revision = &v1alphapeloton.Revision{
		Version:   runtime.GetRevision().GetVersion(),
		CreatedAt: runtime.GetRevision().GetCreatedAt(),
		UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
		UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
	}
	result.State = stateless.JobState(runtime.GetState())
	result.CreationTime = runtime.GetCreationTime()
	result.PodStats = ConvertTaskStatsToPodStats(runtime.TaskStats)
	result.DesiredState = stateless.JobState(runtime.GetGoalState())
	result.Version = versionutil.GetJobEntityVersion(
		runtime.GetConfigurationVersion(),
		runtime.GetDesiredStateVersion(),
		runtime.GetWorkflowVersion(),
	)
	result.WorkflowStatus = ConvertUpdateModelToWorkflowStatus(runtime, updateInfo)

	for configVersion, taskStats := range runtime.GetTaskStatsByConfigurationVersion() {
		entityVersion := versionutil.GetPodEntityVersion(configVersion)
		podConfigVersionStats[entityVersion.GetValue()] = &stateless.JobStatus_PodStateStats{
			StateStats: ConvertTaskStatsToPodStats(taskStats.GetStateStats()),
		}
	}
	result.PodStatsByConfigurationVersion = podConfigVersionStats
	return result
}

// ConvertJobSummary converts v0 job.JobSummary and private
// UpdateModel to v1alpha stateless.JobSummary
func ConvertJobSummary(
	summary *job.JobSummary,
	updateInfo *models.UpdateModel) *stateless.JobSummary {
	return &stateless.JobSummary{
		JobId:         &v1alphapeloton.JobID{Value: summary.GetId().GetValue()},
		Name:          summary.GetName(),
		OwningTeam:    summary.GetOwningTeam(),
		Owner:         summary.GetOwner(),
		Labels:        ConvertLabels(summary.GetLabels()),
		InstanceCount: summary.GetInstanceCount(),
		RespoolId: &v1alphapeloton.ResourcePoolID{
			Value: summary.GetRespoolID().GetValue()},
		Status: ConvertRuntimeInfoToJobStatus(summary.GetRuntime(), updateInfo),
		Sla:    ConvertSLAConfigToSLASpec(summary.GetSLA()),
	}
}

// ConvertSLAConfigToSLASpec convert job's sla config to sla spec
func ConvertSLAConfigToSLASpec(slaConfig *job.SlaConfig) *stateless.SlaSpec {
	return &stateless.SlaSpec{
		Priority:                    slaConfig.GetPriority(),
		Preemptible:                 slaConfig.GetPreemptible(),
		Revocable:                   slaConfig.GetRevocable(),
		MaximumUnavailableInstances: slaConfig.GetMaximumUnavailableInstances(),
	}
}

// ConvertSLASpecToSLAConfig converts job's sla spec to sla config
func ConvertSLASpecToSLAConfig(slaSpec *stateless.SlaSpec) *job.SlaConfig {
	return &job.SlaConfig{
		Priority:                    slaSpec.GetPriority(),
		Preemptible:                 slaSpec.GetPreemptible(),
		Revocable:                   slaSpec.GetRevocable(),
		MaximumUnavailableInstances: slaSpec.GetMaximumUnavailableInstances(),
	}
}

// ConvertUpdateModelToWorkflowInfo converts private UpdateModel
// to v1alpha stateless.WorkflowInfo
func ConvertUpdateModelToWorkflowInfo(
	runtime *job.RuntimeInfo,
	updateInfo *models.UpdateModel,
	workflowEvents []*stateless.WorkflowEvent,
	instanceWorkflowEvents []*stateless.WorkflowInfoInstanceWorkflowEvents,
) *stateless.WorkflowInfo {
	result := &stateless.WorkflowInfo{}
	result.Status = ConvertUpdateModelToWorkflowStatus(runtime, updateInfo)

	if updateInfo.GetType() == models.WorkflowType_UPDATE {
		result.InstancesAdded = util.ConvertInstanceIDListToInstanceRange(updateInfo.GetInstancesAdded())
		result.InstancesRemoved = util.ConvertInstanceIDListToInstanceRange(updateInfo.GetInstancesRemoved())
		result.InstancesUpdated = util.ConvertInstanceIDListToInstanceRange(updateInfo.GetInstancesUpdated())

		result.UpdateSpec = &stateless.UpdateSpec{
			BatchSize:                    updateInfo.GetUpdateConfig().GetBatchSize(),
			RollbackOnFailure:            updateInfo.GetUpdateConfig().GetRollbackOnFailure(),
			MaxInstanceRetries:           updateInfo.GetUpdateConfig().GetMaxInstanceAttempts(),
			MaxTolerableInstanceFailures: updateInfo.GetUpdateConfig().GetMaxFailureInstances(),
			StartPaused:                  updateInfo.GetUpdateConfig().GetStartPaused(),
			InPlace:                      updateInfo.GetUpdateConfig().GetInPlace(),
		}
	} else if updateInfo.GetType() == models.WorkflowType_RESTART {
		result.RestartSpec = &stateless.RestartSpec{
			BatchSize: updateInfo.GetUpdateConfig().GetBatchSize(),
			Ranges:    util.ConvertInstanceIDListToInstanceRange(updateInfo.GetInstancesUpdated()),
			InPlace:   updateInfo.GetUpdateConfig().GetInPlace(),
		}
	}

	result.OpaqueData = &v1alphapeloton.OpaqueData{
		Data: updateInfo.GetOpaqueData().GetData(),
	}

	result.Events = workflowEvents
	result.InstanceEvents = instanceWorkflowEvents
	return result
}

// ConvertStatelessQuerySpecToJobQuerySpec converts query spec for stateless svc to
// job query spec
func ConvertStatelessQuerySpecToJobQuerySpec(spec *stateless.QuerySpec) *job.QuerySpec {
	var labels []*peloton.Label
	var jobStates []job.JobState
	var creationTimeRange *peloton.TimeRange
	var completionTimeRange *peloton.TimeRange
	var respoolPath *pelotonv0respool.ResourcePoolPath
	var paginationSpec *pelotonv0query.PaginationSpec

	for _, label := range spec.GetLabels() {
		labels = append(labels, &peloton.Label{
			Key:   label.GetKey(),
			Value: label.GetValue(),
		})
	}

	for _, jobState := range spec.GetJobStates() {
		jobStates = append(jobStates, job.JobState(jobState))
	}

	if spec.GetCreationTimeRange() != nil {
		creationTimeRange = &peloton.TimeRange{
			Min: spec.GetCreationTimeRange().GetMin(),
			Max: spec.GetCreationTimeRange().GetMax(),
		}
	}

	if spec.GetCompletionTimeRange() != nil {
		completionTimeRange = &peloton.TimeRange{
			Min: spec.GetCompletionTimeRange().GetMin(),
			Max: spec.GetCompletionTimeRange().GetMax(),
		}
	}

	if spec.GetRespool() != nil {
		respoolPath = &pelotonv0respool.ResourcePoolPath{
			Value: spec.GetRespool().GetValue(),
		}
	}

	if spec.GetPagination() != nil {
		paginationSpec = convertV1AlphaPaginationSpecToV0PaginationSpec(
			spec.GetPagination(),
		)
	}

	return &job.QuerySpec{
		Pagination:          paginationSpec,
		Labels:              labels,
		Keywords:            spec.GetKeywords(),
		JobStates:           jobStates,
		Respool:             respoolPath,
		Owner:               spec.GetOwner(),
		Name:                spec.GetName(),
		CreationTimeRange:   creationTimeRange,
		CompletionTimeRange: completionTimeRange,
	}
}

// ConvertJobSpecToJobConfig converts stateless job spec to job config
func ConvertJobSpecToJobConfig(spec *stateless.JobSpec) (*job.JobConfig, error) {
	result := &job.JobConfig{
		Type:          job.JobType_SERVICE,
		Name:          spec.GetName(),
		Owner:         spec.GetOwner(),
		OwningTeam:    spec.GetOwningTeam(),
		LdapGroups:    spec.GetLdapGroups(),
		Description:   spec.GetDescription(),
		InstanceCount: spec.GetInstanceCount(),
	}

	if spec.GetRevision() != nil {
		result.ChangeLog = &peloton.ChangeLog{
			Version:   spec.GetRevision().GetVersion(),
			CreatedAt: spec.GetRevision().GetCreatedAt(),
			UpdatedAt: spec.GetRevision().GetUpdatedAt(),
			UpdatedBy: spec.GetRevision().GetUpdatedBy(),
		}
	}

	if len(spec.GetLabels()) != 0 {
		var labels []*peloton.Label
		for _, label := range spec.GetLabels() {
			labels = append(labels, &peloton.Label{
				Key: label.GetKey(), Value: label.GetValue(),
			})
		}
		result.Labels = labels
	}

	if spec.GetSla() != nil {
		result.SLA = ConvertSLASpecToSLAConfig(spec.GetSla())
	}

	if spec.GetDefaultSpec() != nil {
		defaultConfig, err := ConvertPodSpecToTaskConfig(spec.GetDefaultSpec())
		if err != nil {
			return nil, err
		}
		result.DefaultConfig = defaultConfig
	}

	if spec.GetSla() != nil && spec.GetDefaultSpec() != nil {
		result.DefaultConfig.Revocable = spec.GetSla().GetRevocable()
	}

	if len(spec.GetInstanceSpec()) != 0 {
		result.InstanceConfig = make(map[uint32]*task.TaskConfig)
		for instanceID, instanceSpec := range spec.GetInstanceSpec() {
			instanceConfig, err := ConvertPodSpecToTaskConfig(instanceSpec)
			if err != nil {
				return nil, err
			}
			if spec.GetSla() != nil && spec.GetDefaultSpec() != nil {
				instanceConfig.Revocable = spec.GetSla().GetRevocable()
			}
			result.InstanceConfig[instanceID] = instanceConfig
		}
	}

	if spec.GetRespoolId() != nil {
		result.RespoolID = &peloton.ResourcePoolID{
			Value: spec.GetRespoolId().GetValue(),
		}
	}

	return result, nil
}

// FindVolumeInPodSpec finds a volume of given name in the
// volume spec present in the pod spec
func FindVolumeInPodSpec(spec *pod.PodSpec, name string) *volume.VolumeSpec {
	for _, volume := range spec.GetVolumes() {
		if volume.GetName() == name {
			return volume
		}
	}
	return nil
}

// ConvertMesosContainerToPodSpec converts the mesos container info to PodSpec
func ConvertMesosContainerToPodSpec(
	containerInfo *mesosv1.ContainerInfo, // input
	spec *pod.PodSpec, // output
	container *pod.ContainerSpec, //output
) {
	if containerInfo == nil {
		return
	}

	if spec.GetMesosSpec() == nil {
		spec.MesosSpec = &apachemesos.PodSpec{}
	}

	// populate container type
	switch containerInfo.GetType() {
	case mesosv1.ContainerInfo_DOCKER:
		spec.MesosSpec.Type = apachemesos.PodSpec_CONTAINER_TYPE_DOCKER
	case mesosv1.ContainerInfo_MESOS:
		spec.MesosSpec.Type = apachemesos.PodSpec_CONTAINER_TYPE_MESOS
	}

	// Populate volumes
	var volumeSpecs []*volume.VolumeSpec
	var volumeMounts []*pod.VolumeMount

	for _, v := range containerInfo.GetVolumes() {
		// TBD original name is lost, store it with v0 api
		volumeSpec := FindVolumeInPodSpec(spec, v.GetHostPath())
		if volumeSpec == nil {
			volumeSpec = &volume.VolumeSpec{
				HostPath: &volume.VolumeSpec_HostPathVolumeSource{
					Path: v.GetHostPath(),
				},
				Type: volume.VolumeSpec_VOLUME_TYPE_HOST_PATH,
				Name: v.GetHostPath(),
			}
			volumeSpecs = append(volumeSpecs, volumeSpec)
		}

		readOnly := false
		if v.GetMode() == mesosv1.Volume_RO {
			readOnly = true
		}

		volumeMount := &pod.VolumeMount{
			Name:      v.GetHostPath(),
			ReadOnly:  readOnly,
			MountPath: v.GetContainerPath(),
		}
		volumeMounts = append(volumeMounts, volumeMount)
	}

	if len(volumeSpecs) > 0 {
		spec.Volumes = volumeSpecs
	}

	if len(volumeMounts) > 0 {
		container.VolumeMounts = volumeMounts
	}

	// Populate container type specific info
	if containerInfo.GetType() == mesosv1.ContainerInfo_DOCKER {
		dockerInfo := containerInfo.GetDocker()

		var parameters []*apachemesos.PodSpec_DockerParameter
		for _, parameter := range dockerInfo.GetParameters() {
			p := &apachemesos.PodSpec_DockerParameter{
				Key:   parameter.GetKey(),
				Value: parameter.GetValue(),
			}
			parameters = append(parameters, p)
		}

		if len(parameters) > 0 {
			spec.MesosSpec.DockerParameters = parameters
		}

		if len(dockerInfo.GetImage()) > 0 {
			container.Image = dockerInfo.GetImage()
		}
	} else {
		if len(containerInfo.GetMesos().GetImage().GetDocker().GetName()) > 0 {
			container.Image = containerInfo.GetMesos().GetImage().GetDocker().GetName()
		}
	}
}

// ConvertPodSpecToMesosContainer converts pod spec to mesos container info
func ConvertPodSpecToMesosContainer(spec *pod.PodSpec) *mesosv1.ContainerInfo {
	if len(spec.GetContainers()) == 0 {
		return nil
	}

	mainContainer := spec.GetContainers()[0]
	if mainContainer.GetContainer() != nil {
		return mainContainer.GetContainer()
	}

	if len(mainContainer.GetImage()) == 0 {
		// Container being launched without an image
		return nil
	}

	containerInfo := &mesosv1.ContainerInfo{}
	mesosPodSpec := spec.GetMesosSpec()

	// populate ContainerType
	if mesosPodSpec.GetType() == apachemesos.PodSpec_CONTAINER_TYPE_MESOS {
		containerType := mesosv1.ContainerInfo_MESOS
		containerInfo.Type = &containerType
	} else if mesosPodSpec.GetType() == apachemesos.PodSpec_CONTAINER_TYPE_DOCKER {
		containerType := mesosv1.ContainerInfo_DOCKER
		containerInfo.Type = &containerType
	}

	// Populate volumes
	var volumes []*mesosv1.Volume
	for _, volumeMount := range mainContainer.GetVolumeMounts() {
		volumeSpec := FindVolumeInPodSpec(spec, volumeMount.GetName())
		if volumeSpec.GetType() != volume.VolumeSpec_VOLUME_TYPE_HOST_PATH ||
			volumeSpec.GetHostPath() == nil {
			// unsupported volume
			continue
		}

		mode := mesosv1.Volume_RW
		if volumeMount.GetReadOnly() {
			mode = mesosv1.Volume_RO
		}
		mountPath := volumeMount.GetMountPath()
		containerPath := volumeSpec.GetHostPath().GetPath()

		mesosVolume := &mesosv1.Volume{
			Mode:          &mode,
			ContainerPath: &mountPath,
			HostPath:      &containerPath,
		}

		volumes = append(volumes, mesosVolume)
	}

	if len(volumes) > 0 {
		containerInfo.Volumes = volumes
	}

	// Populate container type specific info
	cached := true
	image := mainContainer.GetImage()
	if len(image) > 0 {
		if containerInfo.GetType() == mesosv1.ContainerInfo_DOCKER {
			var parameters []*mesosv1.Parameter

			for _, parameter := range mesosPodSpec.GetDockerParameters() {
				key := parameter.GetKey()
				value := parameter.GetValue()
				mesosParameter := &mesosv1.Parameter{
					Key:   &key,
					Value: &value,
				}
				parameters = append(parameters, mesosParameter)
			}

			hostNetwork := mesosv1.ContainerInfo_DockerInfo_HOST

			containerInfo.Docker = &mesosv1.ContainerInfo_DockerInfo{
				Image:      &image,
				Network:    &hostNetwork,
				Parameters: parameters,
			}
		} else {
			imageType := mesosv1.Image_DOCKER
			containerInfo.Mesos = &mesosv1.ContainerInfo_MesosInfo{
				Image: &mesosv1.Image{
					Type: &imageType,
					Docker: &mesosv1.Image_Docker{
						Name: &image,
					},
					Cached: &cached,
				},
			}
		}
	}

	return containerInfo
}

// ConvertMesosCommandToPodSpec converts the mesos command info to pod spec
func ConvertMesosCommandToPodSpec(
	commandInfo *mesosv1.CommandInfo, //input
	spec *pod.PodSpec, //output
	container *pod.ContainerSpec, // output
) {
	if commandInfo == nil {
		return
	}

	if spec.GetMesosSpec() == nil {
		spec.MesosSpec = &apachemesos.PodSpec{}
	}

	// populate uris
	var uris []*apachemesos.PodSpec_URI
	for _, uri := range commandInfo.GetUris() {
		u := &apachemesos.PodSpec_URI{
			Value:      uri.GetValue(),
			Executable: uri.GetExecutable(),
			Extract:    uri.GetExtract(),
			Cache:      uri.GetCache(),
			OutputFile: uri.GetOutputFile(),
		}
		uris = append(uris, u)
	}

	if len(uris) > 0 {
		spec.MesosSpec.Uris = uris
	}

	// populate environment
	if commandInfo.GetEnvironment() != nil {
		var environments []*pod.Environment
		for _, env := range commandInfo.GetEnvironment().GetVariables() {
			if env.GetType() != mesosv1.Environment_Variable_VALUE {
				// unsupported type
				continue
			}

			e := &pod.Environment{
				Name:  env.GetName(),
				Value: env.GetValue(),
			}
			environments = append(environments, e)
		}
		container.Environment = environments
	}

	// populate shell
	if commandInfo.Shell != nil {
		spec.MesosSpec.Shell = commandInfo.GetShell()
	}

	// populate entrypoint
	container.Entrypoint = &pod.CommandSpec{}
	if len(commandInfo.GetValue()) > 0 {
		container.Entrypoint.Value = commandInfo.GetValue()
	}

	if len(commandInfo.GetArguments()) > 0 {
		container.Entrypoint.Arguments = commandInfo.GetArguments()
	}
}

// ConvertPodSpecToMesosCommand converts pod spec to mesos command info
func ConvertPodSpecToMesosCommand(spec *pod.PodSpec) *mesosv1.CommandInfo {
	if len(spec.GetContainers()) == 0 {
		return nil
	}

	mainContainer := spec.GetContainers()[0]
	if mainContainer.GetCommand() != nil {
		return mainContainer.GetCommand()
	}

	commandInfo := &mesosv1.CommandInfo{}
	mesosPodSpec := spec.GetMesosSpec()

	// populate uris
	var uris []*mesosv1.CommandInfo_URI

	for _, uri := range mesosPodSpec.GetUris() {
		uriValue := uri.GetValue()
		uriExecutable := uri.GetExecutable()
		uriExtract := uri.GetExtract()
		uriCache := uri.GetCache()
		uriOutputFile := uri.GetOutputFile()
		mesosUri := &mesosv1.CommandInfo_URI{
			Value:      &uriValue,
			Executable: &uriExecutable,
			Extract:    &uriExtract,
			Cache:      &uriCache,
			OutputFile: &uriOutputFile,
		}
		uris = append(uris, mesosUri)
	}

	if len(uris) > 0 {
		commandInfo.Uris = uris
	}

	// populate environment
	var environments []*mesosv1.Environment_Variable

	for _, env := range mainContainer.GetEnvironment() {
		envName := env.GetName()
		envValue := env.GetValue()
		envType := mesosv1.Environment_Variable_VALUE
		mesosEnv := &mesosv1.Environment_Variable{
			Name:  &envName,
			Value: &envValue,
			Type:  &envType,
		}
		environments = append(environments, mesosEnv)
	}

	if len(environments) > 0 {
		environment := &mesosv1.Environment{
			Variables: environments,
		}
		commandInfo.Environment = environment
	}

	// populate shell
	if mesosPodSpec != nil {
		commandShell := mesosPodSpec.GetShell()
		commandInfo.Shell = &commandShell
	}

	// populate command
	if len(mainContainer.GetEntrypoint().GetValue()) > 0 {
		commandValue := mainContainer.GetEntrypoint().GetValue()
		commandInfo.Value = &commandValue
	}

	// populate arguments
	if len(mainContainer.GetEntrypoint().GetArguments()) > 0 {
		commandInfo.Arguments = mainContainer.GetEntrypoint().GetArguments()
	}

	return commandInfo
}

// ConvertMesosExecutorInfoToPodSpec converts mesos executor info to pod spec
func ConvertMesosExecutorInfoToPodSpec(
	executorInfo *mesosv1.ExecutorInfo, // input
	spec *pod.PodSpec, // output
	container *pod.ContainerSpec, //output
) {
	if executorInfo == nil {
		return
	}

	if spec.GetMesosSpec() == nil {
		spec.MesosSpec = &apachemesos.PodSpec{}
	}

	spec.MesosSpec.ExecutorSpec = &apachemesos.PodSpec_ExecutorSpec{}

	// populate the executor type
	switch executorInfo.GetType() {
	case mesosv1.ExecutorInfo_UNKNOWN:
		spec.MesosSpec.ExecutorSpec.Type = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_INVALID
	case mesosv1.ExecutorInfo_DEFAULT:
		spec.MesosSpec.ExecutorSpec.Type = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_DEFAULT
	case mesosv1.ExecutorInfo_CUSTOM:
		spec.MesosSpec.ExecutorSpec.Type = apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM
	}

	// ppopulate the executor ID
	if executorInfo.GetExecutorId() != nil {
		spec.MesosSpec.ExecutorSpec.ExecutorId = executorInfo.GetExecutorId().GetValue()
	}

	// populate the executor data
	if len(executorInfo.GetData()) > 0 {
		spec.MesosSpec.ExecutorSpec.Data = executorInfo.GetData()
	}
}

// ConvertPodSpecToMesosExecutorInfo converts pod spec to mesos executor info
func ConvertPodSpecToMesosExecutorInfo(spec *pod.PodSpec) *mesosv1.ExecutorInfo {
	if len(spec.GetContainers()) == 0 {
		return nil
	}

	mainContainer := spec.GetContainers()[0]
	if mainContainer.GetExecutor() != nil {
		return mainContainer.GetExecutor()
	}

	executorInfo := &mesosv1.ExecutorInfo{}
	mesosExecutorSpec := spec.GetMesosSpec().GetExecutorSpec()
	if mesosExecutorSpec == nil {
		return nil
	}

	// populate the executor type
	executorType := mesosv1.ExecutorInfo_UNKNOWN
	if mesosExecutorSpec.GetType() == apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_CUSTOM {
		executorType = mesosv1.ExecutorInfo_CUSTOM
	} else if mesosExecutorSpec.GetType() == apachemesos.PodSpec_ExecutorSpec_EXECUTOR_TYPE_DEFAULT {
		executorType = mesosv1.ExecutorInfo_DEFAULT
	}
	executorInfo.Type = &executorType

	// ppopulate the executor ID
	executorID := mesosExecutorSpec.GetExecutorId()
	if len(mesosExecutorSpec.GetExecutorId()) > 0 {
		executorInfo.ExecutorId = &mesosv1.ExecutorID{
			Value: &executorID,
		}
	}

	// populate the executor data
	if len(mesosExecutorSpec.GetData()) > 0 {
		executorInfo.Data = mesosExecutorSpec.GetData()
	}

	return executorInfo
}

// ConvertPodSpecToTaskConfig converts a pod spec to task config
func ConvertPodSpecToTaskConfig(spec *pod.PodSpec) (*task.TaskConfig, error) {
	if len(spec.GetContainers()) > 1 {
		return nil,
			yarpcerrors.UnimplementedErrorf("configuration of more than one container per pod is not supported")
	}

	if len(spec.GetInitContainers()) > 0 {
		return nil,
			yarpcerrors.UnimplementedErrorf("init containers are not supported")
	}

	result := &task.TaskConfig{
		Controller:             spec.GetController(),
		KillGracePeriodSeconds: spec.GetKillGracePeriodSeconds(),
		Revocable:              spec.GetRevocable(),
	}

	var mainContainer *pod.ContainerSpec
	if len(spec.GetContainers()) > 0 {
		mainContainer = spec.GetContainers()[0]
		result.Container = ConvertPodSpecToMesosContainer(spec)
		result.Command = ConvertPodSpecToMesosCommand(spec)
		result.Executor = ConvertPodSpecToMesosExecutorInfo(spec)
	}

	result.Name = mainContainer.GetName()

	if spec.GetLabels() != nil {
		var labels []*peloton.Label
		for _, label := range spec.GetLabels() {
			labels = append(labels, &peloton.Label{
				Key: label.GetKey(), Value: label.GetValue(),
			})
		}
		result.Labels = labels
	}

	if mainContainer.GetResource() != nil {
		result.Resource = &task.ResourceConfig{
			CpuLimit:    mainContainer.GetResource().GetCpuLimit(),
			MemLimitMb:  mainContainer.GetResource().GetMemLimitMb(),
			DiskLimitMb: mainContainer.GetResource().GetDiskLimitMb(),
			FdLimit:     mainContainer.GetResource().GetFdLimit(),
			GpuLimit:    mainContainer.GetResource().GetGpuLimit(),
		}
	}

	if mainContainer.GetLivenessCheck() != nil {
		healthCheck := &task.HealthCheckConfig{
			Enabled:                mainContainer.GetLivenessCheck().GetEnabled(),
			InitialIntervalSecs:    mainContainer.GetLivenessCheck().GetInitialIntervalSecs(),
			IntervalSecs:           mainContainer.GetLivenessCheck().GetIntervalSecs(),
			MaxConsecutiveFailures: mainContainer.GetLivenessCheck().GetMaxConsecutiveFailures(),
			TimeoutSecs:            mainContainer.GetLivenessCheck().GetTimeoutSecs(),
			Type:                   task.HealthCheckConfig_Type(mainContainer.GetLivenessCheck().GetType()),
		}

		if mainContainer.GetLivenessCheck().GetCommandCheck() != nil {
			healthCheck.CommandCheck = &task.HealthCheckConfig_CommandCheck{
				Command:             mainContainer.GetLivenessCheck().GetCommandCheck().GetCommand(),
				UnshareEnvironments: mainContainer.GetLivenessCheck().GetCommandCheck().GetUnshareEnvironments(),
			}
		}

		if mainContainer.GetLivenessCheck().GetHttpCheck() != nil {
			healthCheck.HttpCheck = &task.HealthCheckConfig_HTTPCheck{
				Scheme: mainContainer.GetLivenessCheck().GetHttpCheck().GetScheme(),
				Port:   mainContainer.GetLivenessCheck().GetHttpCheck().GetPort(),
				Path:   mainContainer.GetLivenessCheck().GetHttpCheck().GetPath(),
			}
		}

		result.HealthCheck = healthCheck
	}

	if len(mainContainer.GetPorts()) != 0 {
		var portConfigs []*task.PortConfig
		for _, port := range mainContainer.GetPorts() {
			portConfigs = append(portConfigs, &task.PortConfig{
				Name:    port.GetName(),
				Value:   port.GetValue(),
				EnvName: port.GetEnvName(),
			})
		}
		result.Ports = portConfigs
	}

	if spec.GetConstraint() != nil {
		result.Constraint = ConvertPodConstraintsToTaskConstraints(
			[]*pod.Constraint{spec.GetConstraint()},
		)[0]
	}

	if spec.GetRestartPolicy() != nil {
		result.RestartPolicy = &task.RestartPolicy{
			MaxFailures: spec.GetRestartPolicy().GetMaxFailures(),
		}
	}

	if spec.GetVolume() != nil {
		result.Volume = &task.PersistentVolumeConfig{
			ContainerPath: spec.GetVolume().GetContainerPath(),
			SizeMB:        spec.GetVolume().GetSizeMb(),
		}
	}

	if spec.GetPreemptionPolicy() != nil {
		result.PreemptionPolicy = &task.PreemptionPolicy{
			KillOnPreempt: spec.GetPreemptionPolicy().GetKillOnPreempt(),
		}
		if result.GetPreemptionPolicy().GetKillOnPreempt() {
			result.PreemptionPolicy.Type = task.PreemptionPolicy_TYPE_PREEMPTIBLE
		} else {
			result.PreemptionPolicy.Type = task.PreemptionPolicy_TYPE_NON_PREEMPTIBLE
		}
	}

	return result, nil
}

// ConvertPodConstraintsToTaskConstraints converts pod constraints to task constraints
func ConvertPodConstraintsToTaskConstraints(
	constraints []*pod.Constraint,
) []*task.Constraint {
	var result []*task.Constraint
	for _, podConstraint := range constraints {
		taskConstraint := &task.Constraint{
			Type: task.Constraint_Type(podConstraint.GetType()),
		}

		if podConstraint.GetLabelConstraint() != nil {
			taskConstraint.LabelConstraint = &task.LabelConstraint{
				Kind: task.LabelConstraint_Kind(
					podConstraint.GetLabelConstraint().GetKind(),
				),
				Condition: task.LabelConstraint_Condition(
					podConstraint.GetLabelConstraint().GetCondition(),
				),
				Requirement: podConstraint.GetLabelConstraint().GetRequirement(),
			}

			if podConstraint.GetLabelConstraint().GetLabel() != nil {
				taskConstraint.LabelConstraint.Label = &peloton.Label{
					Key:   podConstraint.GetLabelConstraint().GetLabel().GetKey(),
					Value: podConstraint.GetLabelConstraint().GetLabel().GetValue(),
				}
			}
		}

		if podConstraint.GetAndConstraint() != nil {
			taskConstraint.AndConstraint = &task.AndConstraint{
				Constraints: ConvertPodConstraintsToTaskConstraints(
					podConstraint.GetAndConstraint().GetConstraints()),
			}
		}

		if podConstraint.GetOrConstraint() != nil {
			taskConstraint.OrConstraint = &task.OrConstraint{
				Constraints: ConvertPodConstraintsToTaskConstraints(
					podConstraint.GetOrConstraint().GetConstraints()),
			}
		}

		result = append(result, taskConstraint)
	}

	return result
}

// ConvertUpdateSpecToUpdateConfig converts update spec to update config
func ConvertUpdateSpecToUpdateConfig(spec *stateless.UpdateSpec) *update.UpdateConfig {
	return &update.UpdateConfig{
		BatchSize:           spec.GetBatchSize(),
		RollbackOnFailure:   spec.GetRollbackOnFailure(),
		MaxInstanceAttempts: spec.GetMaxInstanceRetries(),
		MaxFailureInstances: spec.GetMaxTolerableInstanceFailures(),
		StartPaused:         spec.GetStartPaused(),
		InPlace:             spec.GetInPlace(),
		StartTasks:          spec.GetStartPods(),
	}
}

// ConvertCreateSpecToUpdateConfig converts create spec to update config
func ConvertCreateSpecToUpdateConfig(spec *stateless.CreateSpec) *update.UpdateConfig {
	return &update.UpdateConfig{
		BatchSize:           spec.GetBatchSize(),
		MaxInstanceAttempts: spec.GetMaxInstanceRetries(),
		MaxFailureInstances: spec.GetMaxTolerableInstanceFailures(),
		StartPaused:         spec.GetStartPaused(),
	}
}

// ConvertPodQuerySpecToTaskQuerySpec converts
// v1alpha pod.QuerySpec to v0 task.QuerySpec
func ConvertPodQuerySpecToTaskQuerySpec(spec *pod.QuerySpec) *task.QuerySpec {
	var taskStates []task.TaskState
	var taskNames []string
	if spec.GetPodStates() != nil {
		for _, state := range spec.GetPodStates() {
			taskStates = append(taskStates, ConvertPodStateToTaskState(state))
		}
	}

	if spec.GetNames() != nil {
		for _, podName := range spec.GetNames() {
			taskNames = append(taskNames, podName.GetValue())
		}
	}

	return &task.QuerySpec{
		Pagination: convertV1AlphaPaginationSpecToV0PaginationSpec(
			spec.GetPagination(),
		),
		TaskStates: taskStates,
		Names:      taskNames,
		Hosts:      spec.GetHosts(),
	}
}

// ConvertTaskInfosToPodInfos converts a list of
// v0 task info to a list of v1alpha pod info
func ConvertTaskInfosToPodInfos(taskInfos []*task.TaskInfo) []*pod.PodInfo {
	var podInfos []*pod.PodInfo
	for _, taskInfo := range taskInfos {
		podInfo := &pod.PodInfo{
			Spec: ConvertTaskConfigToPodSpec(
				taskInfo.GetConfig(),
				taskInfo.GetJobId().GetValue(),
				taskInfo.GetInstanceId(),
			),
			Status: ConvertTaskRuntimeToPodStatus(taskInfo.GetRuntime()),
		}
		podInfos = append(podInfos, podInfo)
	}

	return podInfos
}

// ConvertTaskStatsToPodStats converts v0 task stats to v1alpha pod stats
func ConvertTaskStatsToPodStats(taskStats map[string]uint32) map[string]uint32 {
	result := make(map[string]uint32)
	for stateStr, num := range taskStats {
		taskState := task.TaskState(task.TaskState_value[stateStr])
		result[ConvertTaskStateToPodState(taskState).String()] = num
	}

	return result
}

func convertV1AlphaPaginationSpecToV0PaginationSpec(
	pagination *query.PaginationSpec,
) *pelotonv0query.PaginationSpec {
	if pagination == nil {
		return nil
	}

	var orderBy []*pelotonv0query.OrderBy
	for _, ele := range pagination.GetOrderBy() {
		orderBy = append(orderBy, &pelotonv0query.OrderBy{
			Order: pelotonv0query.OrderBy_Order(ele.GetOrder()),
			Property: &pelotonv0query.PropertyPath{
				Value: ele.GetProperty().GetValue(),
			},
		})
	}

	return &pelotonv0query.PaginationSpec{
		Offset:   pagination.GetOffset(),
		Limit:    pagination.GetLimit(),
		OrderBy:  orderBy,
		MaxLimit: pagination.GetMaxLimit(),
	}
}

func convertTaskTerminationStatusToPodTerminationStatus(
	termStatus *task.TerminationStatus,
) *pod.TerminationStatus {
	if termStatus == nil {
		return nil
	}

	podReason := pod.TerminationStatus_TERMINATION_STATUS_REASON_INVALID
	switch termStatus.GetReason() {
	case task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_ON_REQUEST
	case task.TerminationStatus_TERMINATION_STATUS_REASON_FAILED:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_FAILED
	case task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE
	case task.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES
	case task.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_DEADLINE_TIMEOUT_EXCEEDED
	case task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_UPDATE
	case task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_RESTART
	case task.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART:
		podReason = pod.TerminationStatus_TERMINATION_STATUS_REASON_KILLED_FOR_SLA_AWARE_RESTART
	}
	return &pod.TerminationStatus{
		Reason:   podReason,
		ExitCode: termStatus.GetExitCode(),
		Signal:   termStatus.GetSignal(),
	}
}
