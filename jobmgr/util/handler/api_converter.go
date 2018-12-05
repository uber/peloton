package handler

import (
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"
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
	}
	return pod.PodState_POD_STATE_INVALID
}

// ConvertTaskRuntimeToPodStatus converts v0 task.RuntimeInfo to pod.PodStatus
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
			},
		},
		DesiredState:   ConvertTaskStateToPodState(runtime.GetGoalState()),
		Message:        runtime.GetMessage(),
		Reason:         runtime.GetReason(),
		FailureCount:   runtime.GetFailureCount(),
		VolumeId:       &v1alphapeloton.VolumeID{Value: runtime.GetVolumeID().GetValue()},
		Version:        jobutil.GetPodEntityVersion(runtime.GetConfigVersion()),
		DesiredVersion: jobutil.GetPodEntityVersion(runtime.GetDesiredConfigVersion()),
		AgentId:        runtime.GetAgentID(),
		Revision: &v1alphapeloton.Revision{
			Version:   runtime.GetRevision().GetVersion(),
			CreatedAt: runtime.GetRevision().GetCreatedAt(),
			UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
			UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
		},
		PrevPodId:     &v1alphapeloton.PodID{Value: runtime.GetPrevMesosTaskId().GetValue()},
		ResourceUsage: runtime.GetResourceUsage(),
		DesiredPodId:  &v1alphapeloton.PodID{Value: runtime.GetDesiredMesosTaskId().GetValue()},
	}
}

// ConvertTaskConfigToPodSpec converts v0 task.TaskConfig to v1alpha pod.PodSpec
func ConvertTaskConfigToPodSpec(taskConfig *task.TaskConfig) *pod.PodSpec {
	var constraint *pod.Constraint
	if taskConfig.GetConstraint() != nil {
		constraint = ConvertConstraints([]*task.Constraint{taskConfig.GetConstraint()})[0]
	}

	return &pod.PodSpec{
		PodName: &v1alphapeloton.PodName{Value: taskConfig.GetName()},
		Labels:  ConvertLabels(taskConfig.GetLabels()),
		Containers: []*pod.ContainerSpec{
			{
				Name: taskConfig.GetName(),
				Resource: &pod.ResourceSpec{
					CpuLimit:    taskConfig.GetResource().GetCpuLimit(),
					MemLimitMb:  taskConfig.GetResource().GetMemLimitMb(),
					DiskLimitMb: taskConfig.GetResource().GetDiskLimitMb(),
					FdLimit:     taskConfig.GetResource().GetFdLimit(),
					GpuLimit:    taskConfig.GetResource().GetGpuLimit(),
				},
				Container: taskConfig.GetContainer(),
				Command:   taskConfig.GetCommand(),
				LivenessCheck: &pod.HealthCheckSpec{
					Enabled:                taskConfig.GetHealthCheck().GetEnabled(),
					InitialIntervalSecs:    taskConfig.GetHealthCheck().GetInitialIntervalSecs(),
					IntervalSecs:           taskConfig.GetHealthCheck().GetIntervalSecs(),
					MaxConsecutiveFailures: taskConfig.GetHealthCheck().GetMaxConsecutiveFailures(),
					TimeoutSecs:            taskConfig.GetHealthCheck().GetTimeoutSecs(),
					Type:                   pod.HealthCheckSpec_HealthCheckType(taskConfig.GetHealthCheck().GetType()),
					CommandCheck: &pod.HealthCheckSpec_CommandCheck{
						Command:             taskConfig.GetHealthCheck().GetCommandCheck().GetCommand(),
						UnshareEnvironments: taskConfig.GetHealthCheck().GetCommandCheck().GetUnshareEnvironments(),
					},
				},
				Ports: ConvertContainerPorts(taskConfig.GetPorts()),
			},
		},
		Constraint: constraint,
		RestartPolicy: &pod.RestartPolicy{
			MaxFailures: taskConfig.GetRestartPolicy().GetMaxFailures(),
		},
		Volume: &pod.PersistentVolumeSpec{
			ContainerPath: taskConfig.GetVolume().GetContainerPath(),
			SizeMb:        taskConfig.GetVolume().GetSizeMB(),
		},
		PreemptionPolicy: &pod.PreemptionPolicy{
			KillOnPreempt: taskConfig.GetPreemptionPolicy().GetKillOnPreempt(),
		},
		Controller:             taskConfig.GetController(),
		KillGracePeriodSeconds: taskConfig.GetKillGracePeriodSeconds(),
		Revocable:              taskConfig.GetRevocable(),
	}
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

// ConvertConstraints converts v0 task.Constraint array to
// v1alpha pod.Constraint array
func ConvertConstraints(constraints []*task.Constraint) []*pod.Constraint {
	var podConstraints []*pod.Constraint
	for _, constraint := range constraints {
		podConstraints = append(podConstraints, &pod.Constraint{
			Type: pod.Constraint_Type(constraint.GetType()),
			LabelConstraint: &pod.LabelConstraint{
				Kind: pod.LabelConstraint_Kind(
					constraint.GetLabelConstraint().GetKind(),
				),
				Condition: pod.LabelConstraint_Condition(
					constraint.GetLabelConstraint().GetCondition(),
				),
				Label: &v1alphapeloton.Label{
					Value: constraint.GetLabelConstraint().GetLabel().GetValue(),
				},
				Requirement: constraint.GetLabelConstraint().GetRequirement(),
			},
			AndConstraint: &pod.AndConstraint{
				Constraints: ConvertConstraints(constraint.GetAndConstraint().GetConstraints()),
			},
			OrConstraint: &pod.OrConstraint{
				Constraints: ConvertConstraints(constraint.GetOrConstraint().GetConstraints()),
			},
		})
	}
	return podConstraints
}

// ConvertContainerPorts converts v0 task.PortConfig array to
// v1alpha pod.PortSpec array
func ConvertContainerPorts(ports []*task.PortConfig) []*pod.PortSpec {
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

// ConvertSecrets converts v0 peloton.Secret to v1alpha peloton.Secret
func ConvertSecrets(secrets []*peloton.Secret) []*v1alphapeloton.Secret {
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

// ConvertJobConfigToJobSpec converts v0 job.JobConfig to v1alpha stateless.JobSpec
func ConvertJobConfigToJobSpec(config *job.JobConfig) *stateless.JobSpec {
	instanceSpec := make(map[uint32]*pod.PodSpec)
	for instID, taskConfig := range config.GetInstanceConfig() {
		instanceSpec[instID] = ConvertTaskConfigToPodSpec(taskConfig)
	}

	return &stateless.JobSpec{
		Revision: &v1alphapeloton.Revision{
			Version:   config.GetChangeLog().GetVersion(),
			CreatedAt: config.GetChangeLog().GetCreatedAt(),
			UpdatedAt: config.GetChangeLog().GetUpdatedAt(),
			UpdatedBy: config.GetChangeLog().GetUpdatedBy(),
		},
		Name:          config.GetName(),
		OwningTeam:    config.GetOwningTeam(),
		LdapGroups:    config.GetLdapGroups(),
		Description:   config.GetDescription(),
		Labels:        ConvertLabels(config.GetLabels()),
		InstanceCount: config.GetInstanceCount(),
		Sla: &stateless.SlaSpec{
			Priority:    config.GetSLA().GetPriority(),
			Preemptible: config.GetSLA().GetPreemptible(),
			Revocable:   config.GetSLA().GetRevocable(),
		},
		DefaultSpec:  ConvertTaskConfigToPodSpec(config.GetDefaultConfig()),
		InstanceSpec: instanceSpec,
		RespoolId: &v1alphapeloton.ResourcePoolID{
			Value: config.GetRespoolID().GetValue()},
	}
}

// ConvertUpdateModelToWorkflowStatus converts private UpdateModel
// to v1alpha stateless.WorkflowStatus
func ConvertUpdateModelToWorkflowStatus(
	updateInfo *models.UpdateModel) *stateless.WorkflowStatus {
	if updateInfo == nil {
		return nil
	}

	return &stateless.WorkflowStatus{
		Type:                  stateless.WorkflowType(updateInfo.GetType()),
		State:                 stateless.WorkflowState(updateInfo.GetState()),
		NumInstancesCompleted: updateInfo.GetInstancesDone(),
		NumInstancesRemaining: updateInfo.GetInstancesTotal() - updateInfo.GetInstancesDone() - updateInfo.GetInstancesFailed(),
		NumInstancesFailed:    updateInfo.GetInstancesFailed(),
		InstancesCurrent:      updateInfo.GetInstancesCurrent(),
		Version:               jobutil.GetPodEntityVersion(updateInfo.GetJobConfigVersion()),
		PrevVersion:           jobutil.GetPodEntityVersion(updateInfo.GetPrevJobConfigVersion()),
	}
}

// ConvertRuntimeInfoToJobStatus converts v0 job.RuntimeInfo and private
// UpdateModel to v1alpha stateless.JobStatus
func ConvertRuntimeInfoToJobStatus(
	runtime *job.RuntimeInfo,
	updateInfo *models.UpdateModel,
) *stateless.JobStatus {
	result := &stateless.JobStatus{}
	result.Revision = &v1alphapeloton.Revision{
		Version:   runtime.GetRevision().GetVersion(),
		CreatedAt: runtime.GetRevision().GetCreatedAt(),
		UpdatedAt: runtime.GetRevision().GetUpdatedAt(),
		UpdatedBy: runtime.GetRevision().GetUpdatedBy(),
	}
	result.State = stateless.JobState(runtime.GetState())
	result.CreationTime = runtime.GetCreationTime()
	result.PodStats = runtime.TaskStats
	result.DesiredState = stateless.JobState(runtime.GetGoalState())
	result.Version = jobutil.GetJobEntityVersion(
		runtime.GetConfigurationVersion(),
		runtime.GetWorkflowVersion(),
	)
	result.WorkflowStatus = ConvertUpdateModelToWorkflowStatus(updateInfo)
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
	}
}

// ConvertUpdateModelToWorkflowInfo converts private UpdateModel
// to v1alpha stateless.WorkflowInfo
func ConvertUpdateModelToWorkflowInfo(
	updateInfo *models.UpdateModel) *stateless.WorkflowInfo {
	result := &stateless.WorkflowInfo{}
	result.Status = ConvertUpdateModelToWorkflowStatus(updateInfo)

	if updateInfo.GetType() == models.WorkflowType_UPDATE {
		result.UpdateSpec = &stateless.UpdateSpec{
			BatchSize:                    updateInfo.GetUpdateConfig().GetBatchSize(),
			RollbackOnFailure:            updateInfo.GetUpdateConfig().GetRollbackOnFailure(),
			MaxInstanceRetries:           updateInfo.GetUpdateConfig().GetMaxInstanceAttempts(),
			MaxTolerableInstanceFailures: updateInfo.GetUpdateConfig().GetMaxFailureInstances(),
			StartPaused:                  updateInfo.GetUpdateConfig().GetStartPaused(),
		}
	} else if updateInfo.GetType() == models.WorkflowType_RESTART {
		result.RestartBatchSize = updateInfo.GetUpdateConfig().GetBatchSize()
		// TODO store and implement the restart ranges provided in the configuration
	}
	return result
}
