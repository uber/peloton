package handler

import (
	"sort"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pelotonv0query "code.uber.internal/infra/peloton/.gen/peloton/api/v0/query"
	pelotonv0respool "code.uber.internal/infra/peloton/.gen/peloton/api/v0/respool"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/job/stateless"
	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod"
	"code.uber.internal/infra/peloton/.gen/peloton/private/models"

	jobutil "code.uber.internal/infra/peloton/jobmgr/util/job"

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
	}
	return pod.PodState_POD_STATE_INVALID
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
		constraint = ConvertTaskConstraintsToPodConstraints([]*task.Constraint{taskConfig.GetConstraint()})[0]
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
				Ports: ConvertPortConfigsToPortSpecs(taskConfig.GetPorts()),
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

// ConvertTaskConstraintsToPodConstraints converts v0 task.Constraint array to
// v1alpha pod.Constraint array
func ConvertTaskConstraintsToPodConstraints(constraints []*task.Constraint) []*pod.Constraint {
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
					Key:   constraint.GetLabelConstraint().GetLabel().GetKey(),
					Value: constraint.GetLabelConstraint().GetLabel().GetValue(),
				},
				Requirement: constraint.GetLabelConstraint().GetRequirement(),
			},
			AndConstraint: &pod.AndConstraint{
				Constraints: ConvertTaskConstraintsToPodConstraints(constraint.GetAndConstraint().GetConstraints()),
			},
			OrConstraint: &pod.OrConstraint{
				Constraints: ConvertTaskConstraintsToPodConstraints(constraint.GetOrConstraint().GetConstraints()),
			},
		})
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
		Type:  stateless.WorkflowType(updateInfo.GetType()),
		State: stateless.WorkflowState(updateInfo.GetState()),
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
		runtime.GetDesiredStateVersion(),
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
		result.RestartRanges = ConvertInstanceIDListToInstanceRange(updateInfo.GetInstancesCurrent())
	}

	result.OpaqueData = &v1alphapeloton.OpaqueData{
		Data: updateInfo.GetOpaqueData().GetData(),
	}
	return result
}

// ConvertStatelessQuerySpecToJobQuerySpec converts query spec for stateless svc to
// job query spec
func ConvertStatelessQuerySpecToJobQuerySpec(spec *stateless.QuerySpec) *job.QuerySpec {
	var orderBy []*pelotonv0query.OrderBy
	var labels []*peloton.Label
	var jobStates []job.JobState
	var creationTimeRange *peloton.TimeRange
	var completionTimeRange *peloton.TimeRange
	var respoolPath *pelotonv0respool.ResourcePoolPath
	var paginationSpec *pelotonv0query.PaginationSpec

	for _, ele := range spec.GetPagination().GetOrderBy() {
		orderBy = append(orderBy, &pelotonv0query.OrderBy{
			Order: pelotonv0query.OrderBy_Order(ele.GetOrder()),
			Property: &pelotonv0query.PropertyPath{
				Value: ele.GetProperty().GetValue(),
			},
		})
	}

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
		paginationSpec = &pelotonv0query.PaginationSpec{
			Offset:   spec.GetPagination().GetOffset(),
			Limit:    spec.GetPagination().GetLimit(),
			OrderBy:  orderBy,
			MaxLimit: spec.GetPagination().GetMaxLimit(),
		}
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
		result.SLA = &job.SlaConfig{
			Priority:                spec.GetSla().GetPriority(),
			Preemptible:             spec.GetSla().GetPreemptible(),
			Revocable:               spec.GetSla().GetRevocable(),
			MaximumRunningInstances: spec.GetSla().GetMaximumUnavailableInstances(),
		}
	}

	if spec.GetDefaultSpec() != nil {
		defaultConfig, err := ConvertPodSpecToTaskConfig(spec.GetDefaultSpec())
		if err != nil {
			return nil, err
		}
		result.DefaultConfig = defaultConfig
	}

	if len(spec.GetInstanceSpec()) != 0 {
		result.InstanceConfig = make(map[uint32]*task.TaskConfig)
		for instanceID, instanceSpec := range spec.GetInstanceSpec() {
			instanceConfig, err := ConvertPodSpecToTaskConfig(instanceSpec)
			if err != nil {
				return nil, err
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

	mainContainer := spec.GetContainers()[0]
	result := &task.TaskConfig{
		Name:                   spec.GetPodName().GetValue(),
		Container:              mainContainer.GetContainer(),
		Command:                mainContainer.GetCommand(),
		Controller:             spec.GetController(),
		KillGracePeriodSeconds: spec.GetKillGracePeriodSeconds(),
		Revocable:              spec.GetRevocable(),
	}

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
			label := &peloton.Label{
				Key:   podConstraint.GetLabelConstraint().GetLabel().GetKey(),
				Value: podConstraint.GetLabelConstraint().GetLabel().GetValue(),
			}
			taskConstraint.LabelConstraint = &task.LabelConstraint{
				Kind: task.LabelConstraint_Kind(
					podConstraint.GetLabelConstraint().GetKind(),
				),
				Condition: task.LabelConstraint_Condition(
					podConstraint.GetLabelConstraint().GetCondition(),
				),
				Label:       label,
				Requirement: podConstraint.GetLabelConstraint().GetRequirement(),
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
	}
}

// ConvertInstanceIDListToInstanceRange converts list
// of instance ids to list of instance ranges
func ConvertInstanceIDListToInstanceRange(instIDs []uint32) []*pod.InstanceIDRange {
	var instanceIDRange []*pod.InstanceIDRange
	var instanceRange *pod.InstanceIDRange
	var prevInstID uint32

	instIDSortLess := func(i, j int) bool {
		return instIDs[i] < instIDs[j]
	}

	sort.Slice(instIDs, instIDSortLess)

	for _, instID := range instIDs {
		if instanceRange == nil {
			// create a new range
			instanceRange = &pod.InstanceIDRange{
				From: instID,
			}
		} else {
			// range already exists
			if instID != prevInstID+1 {
				// finish the previous range and start a new one
				instanceRange.To = prevInstID
				instanceIDRange = append(instanceIDRange, instanceRange)
				instanceRange = &pod.InstanceIDRange{
					From: instID,
				}
			}
		}
		prevInstID = instID
	}

	// finish the last instance range
	if instanceRange != nil {
		instanceRange.To = prevInstID
		instanceIDRange = append(instanceIDRange, instanceRange)
	}
	return instanceIDRange
}
