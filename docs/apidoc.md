# Protocol Documentation
<a name="top"/>

## Table of Contents

- [timestamp.proto](#timestamp.proto)
    - [Timestamp](#google.protobuf.Timestamp)
  
  
  
  

- [peloton.proto](#peloton.proto)
    - [ChangeLog](#peloton.api.v0.peloton.ChangeLog)
    - [HostOfferID](#peloton.api.v0.peloton.HostOfferID)
    - [JobID](#peloton.api.v0.peloton.JobID)
    - [Label](#peloton.api.v0.peloton.Label)
    - [OpaqueData](#peloton.api.v0.peloton.OpaqueData)
    - [ResourcePoolID](#peloton.api.v0.peloton.ResourcePoolID)
    - [Secret](#peloton.api.v0.peloton.Secret)
    - [Secret.Value](#peloton.api.v0.peloton.Secret.Value)
    - [SecretID](#peloton.api.v0.peloton.SecretID)
    - [TaskID](#peloton.api.v0.peloton.TaskID)
    - [TimeRange](#peloton.api.v0.peloton.TimeRange)
    - [UpdateID](#peloton.api.v0.peloton.UpdateID)
    - [VolumeID](#peloton.api.v0.peloton.VolumeID)
  
  
  
  

- [errors.proto](#errors.proto)
    - [InvalidRespool](#peloton.api.v0.errors.InvalidRespool)
    - [JobGetRuntimeFail](#peloton.api.v0.errors.JobGetRuntimeFail)
    - [JobNotFound](#peloton.api.v0.errors.JobNotFound)
    - [UnknownError](#peloton.api.v0.errors.UnknownError)
  
  
  
  

- [query.proto](#query.proto)
    - [OrderBy](#peloton.api.v0.query.OrderBy)
    - [Pagination](#peloton.api.v0.query.Pagination)
    - [PaginationSpec](#peloton.api.v0.query.PaginationSpec)
    - [PropertyPath](#peloton.api.v0.query.PropertyPath)
  
    - [OrderBy.Order](#peloton.api.v0.query.OrderBy.Order)
  
  
  

- [task.proto](#task.proto)
    - [AndConstraint](#peloton.api.v0.task.AndConstraint)
    - [BrowseSandboxFailure](#peloton.api.v0.task.BrowseSandboxFailure)
    - [BrowseSandboxRequest](#peloton.api.v0.task.BrowseSandboxRequest)
    - [BrowseSandboxResponse](#peloton.api.v0.task.BrowseSandboxResponse)
    - [BrowseSandboxResponse.Error](#peloton.api.v0.task.BrowseSandboxResponse.Error)
    - [Constraint](#peloton.api.v0.task.Constraint)
    - [DeletePodEventsRequest](#peloton.api.v0.task.DeletePodEventsRequest)
    - [DeletePodEventsResponse](#peloton.api.v0.task.DeletePodEventsResponse)
    - [GetCacheRequest](#peloton.api.v0.task.GetCacheRequest)
    - [GetCacheResponse](#peloton.api.v0.task.GetCacheResponse)
    - [GetPodEventsRequest](#peloton.api.v0.task.GetPodEventsRequest)
    - [GetPodEventsResponse](#peloton.api.v0.task.GetPodEventsResponse)
    - [GetPodEventsResponse.Error](#peloton.api.v0.task.GetPodEventsResponse.Error)
    - [GetRequest](#peloton.api.v0.task.GetRequest)
    - [GetResponse](#peloton.api.v0.task.GetResponse)
    - [HealthCheckConfig](#peloton.api.v0.task.HealthCheckConfig)
    - [HealthCheckConfig.CommandCheck](#peloton.api.v0.task.HealthCheckConfig.CommandCheck)
    - [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange)
    - [InstanceRange](#peloton.api.v0.task.InstanceRange)
    - [LabelConstraint](#peloton.api.v0.task.LabelConstraint)
    - [ListRequest](#peloton.api.v0.task.ListRequest)
    - [ListResponse](#peloton.api.v0.task.ListResponse)
    - [ListResponse.Result](#peloton.api.v0.task.ListResponse.Result)
    - [ListResponse.Result.ValueEntry](#peloton.api.v0.task.ListResponse.Result.ValueEntry)
    - [OrConstraint](#peloton.api.v0.task.OrConstraint)
    - [PersistentVolumeConfig](#peloton.api.v0.task.PersistentVolumeConfig)
    - [PodEvent](#peloton.api.v0.task.PodEvent)
    - [PortConfig](#peloton.api.v0.task.PortConfig)
    - [PreemptionPolicy](#peloton.api.v0.task.PreemptionPolicy)
    - [QueryRequest](#peloton.api.v0.task.QueryRequest)
    - [QueryResponse](#peloton.api.v0.task.QueryResponse)
    - [QueryResponse.Error](#peloton.api.v0.task.QueryResponse.Error)
    - [QuerySpec](#peloton.api.v0.task.QuerySpec)
    - [RefreshRequest](#peloton.api.v0.task.RefreshRequest)
    - [RefreshResponse](#peloton.api.v0.task.RefreshResponse)
    - [ResourceConfig](#peloton.api.v0.task.ResourceConfig)
    - [RestartPolicy](#peloton.api.v0.task.RestartPolicy)
    - [RestartRequest](#peloton.api.v0.task.RestartRequest)
    - [RestartResponse](#peloton.api.v0.task.RestartResponse)
    - [RuntimeInfo](#peloton.api.v0.task.RuntimeInfo)
    - [RuntimeInfo.PortsEntry](#peloton.api.v0.task.RuntimeInfo.PortsEntry)
    - [RuntimeInfo.ResourceUsageEntry](#peloton.api.v0.task.RuntimeInfo.ResourceUsageEntry)
    - [StartRequest](#peloton.api.v0.task.StartRequest)
    - [StartResponse](#peloton.api.v0.task.StartResponse)
    - [StartResponse.Error](#peloton.api.v0.task.StartResponse.Error)
    - [StopRequest](#peloton.api.v0.task.StopRequest)
    - [StopResponse](#peloton.api.v0.task.StopResponse)
    - [StopResponse.Error](#peloton.api.v0.task.StopResponse.Error)
    - [TaskConfig](#peloton.api.v0.task.TaskConfig)
    - [TaskEvent](#peloton.api.v0.task.TaskEvent)
    - [TaskEventsError](#peloton.api.v0.task.TaskEventsError)
    - [TaskInfo](#peloton.api.v0.task.TaskInfo)
    - [TaskNotRunning](#peloton.api.v0.task.TaskNotRunning)
    - [TaskStartFailure](#peloton.api.v0.task.TaskStartFailure)
    - [TaskUpdateError](#peloton.api.v0.task.TaskUpdateError)
    - [TerminationStatus](#peloton.api.v0.task.TerminationStatus)
  
    - [Constraint.Type](#peloton.api.v0.task.Constraint.Type)
    - [HealthCheckConfig.Type](#peloton.api.v0.task.HealthCheckConfig.Type)
    - [HealthState](#peloton.api.v0.task.HealthState)
    - [LabelConstraint.Condition](#peloton.api.v0.task.LabelConstraint.Condition)
    - [LabelConstraint.Kind](#peloton.api.v0.task.LabelConstraint.Kind)
    - [PreemptionPolicy.Type](#peloton.api.v0.task.PreemptionPolicy.Type)
    - [TaskEvent.Source](#peloton.api.v0.task.TaskEvent.Source)
    - [TaskState](#peloton.api.v0.task.TaskState)
    - [TerminationStatus.Reason](#peloton.api.v0.task.TerminationStatus.Reason)
  
  
    - [TaskManager](#peloton.api.v0.task.TaskManager)
  

- [update.proto](#update.proto)
    - [UpdateConfig](#peloton.api.v0.update.UpdateConfig)
    - [UpdateInfo](#peloton.api.v0.update.UpdateInfo)
    - [UpdateStatus](#peloton.api.v0.update.UpdateStatus)
  
    - [State](#peloton.api.v0.update.State)
  
  
  

- [peloton.proto](#peloton.proto)
    - [EntityVersion](#peloton.api.v1alpha.peloton.EntityVersion)
    - [HostOfferID](#peloton.api.v1alpha.peloton.HostOfferID)
    - [JobID](#peloton.api.v1alpha.peloton.JobID)
    - [Label](#peloton.api.v1alpha.peloton.Label)
    - [OpaqueData](#peloton.api.v1alpha.peloton.OpaqueData)
    - [PodID](#peloton.api.v1alpha.peloton.PodID)
    - [PodName](#peloton.api.v1alpha.peloton.PodName)
    - [ResourcePoolID](#peloton.api.v1alpha.peloton.ResourcePoolID)
    - [Revision](#peloton.api.v1alpha.peloton.Revision)
    - [Secret](#peloton.api.v1alpha.peloton.Secret)
    - [Secret.Value](#peloton.api.v1alpha.peloton.Secret.Value)
    - [SecretID](#peloton.api.v1alpha.peloton.SecretID)
    - [TimeRange](#peloton.api.v1alpha.peloton.TimeRange)
    - [VolumeID](#peloton.api.v1alpha.peloton.VolumeID)
  
  
  
  

- [host.proto](#host.proto)
    - [HostInfo](#peloton.api.v0.host.HostInfo)
  
    - [HostState](#peloton.api.v0.host.HostState)
  
  
  

- [changelog.proto](#changelog.proto)
    - [ChangeLog](#peloton.api.v0.changelog.ChangeLog)
  
  
  
  

- [respool.proto](#respool.proto)
    - [ControllerLimit](#peloton.api.v0.respool.ControllerLimit)
    - [CreateRequest](#peloton.api.v0.respool.CreateRequest)
    - [CreateResponse](#peloton.api.v0.respool.CreateResponse)
    - [CreateResponse.Error](#peloton.api.v0.respool.CreateResponse.Error)
    - [DeleteRequest](#peloton.api.v0.respool.DeleteRequest)
    - [DeleteResponse](#peloton.api.v0.respool.DeleteResponse)
    - [DeleteResponse.Error](#peloton.api.v0.respool.DeleteResponse.Error)
    - [GetRequest](#peloton.api.v0.respool.GetRequest)
    - [GetResponse](#peloton.api.v0.respool.GetResponse)
    - [GetResponse.Error](#peloton.api.v0.respool.GetResponse.Error)
    - [InvalidResourcePoolConfig](#peloton.api.v0.respool.InvalidResourcePoolConfig)
    - [InvalidResourcePoolPath](#peloton.api.v0.respool.InvalidResourcePoolPath)
    - [LookupRequest](#peloton.api.v0.respool.LookupRequest)
    - [LookupResponse](#peloton.api.v0.respool.LookupResponse)
    - [LookupResponse.Error](#peloton.api.v0.respool.LookupResponse.Error)
    - [QueryRequest](#peloton.api.v0.respool.QueryRequest)
    - [QueryResponse](#peloton.api.v0.respool.QueryResponse)
    - [QueryResponse.Error](#peloton.api.v0.respool.QueryResponse.Error)
    - [ResourceConfig](#peloton.api.v0.respool.ResourceConfig)
    - [ResourcePoolAlreadyExists](#peloton.api.v0.respool.ResourcePoolAlreadyExists)
    - [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig)
    - [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo)
    - [ResourcePoolIsBusy](#peloton.api.v0.respool.ResourcePoolIsBusy)
    - [ResourcePoolIsNotLeaf](#peloton.api.v0.respool.ResourcePoolIsNotLeaf)
    - [ResourcePoolNotDeleted](#peloton.api.v0.respool.ResourcePoolNotDeleted)
    - [ResourcePoolNotFound](#peloton.api.v0.respool.ResourcePoolNotFound)
    - [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath)
    - [ResourcePoolPathNotFound](#peloton.api.v0.respool.ResourcePoolPathNotFound)
    - [ResourceUsage](#peloton.api.v0.respool.ResourceUsage)
    - [SlackLimit](#peloton.api.v0.respool.SlackLimit)
    - [UpdateRequest](#peloton.api.v0.respool.UpdateRequest)
    - [UpdateResponse](#peloton.api.v0.respool.UpdateResponse)
    - [UpdateResponse.Error](#peloton.api.v0.respool.UpdateResponse.Error)
  
    - [ReservationType](#peloton.api.v0.respool.ReservationType)
    - [SchedulingPolicy](#peloton.api.v0.respool.SchedulingPolicy)
  
  
    - [ResourceManager](#peloton.api.v0.respool.ResourceManager)
  

- [job.proto](#job.proto)
    - [CreateRequest](#peloton.api.v0.job.CreateRequest)
    - [CreateResponse](#peloton.api.v0.job.CreateResponse)
    - [CreateResponse.Error](#peloton.api.v0.job.CreateResponse.Error)
    - [DeleteRequest](#peloton.api.v0.job.DeleteRequest)
    - [DeleteResponse](#peloton.api.v0.job.DeleteResponse)
    - [DeleteResponse.Error](#peloton.api.v0.job.DeleteResponse.Error)
    - [GetActiveJobsRequest](#peloton.api.v0.job.GetActiveJobsRequest)
    - [GetActiveJobsResponse](#peloton.api.v0.job.GetActiveJobsResponse)
    - [GetCacheRequest](#peloton.api.v0.job.GetCacheRequest)
    - [GetCacheResponse](#peloton.api.v0.job.GetCacheResponse)
    - [GetRequest](#peloton.api.v0.job.GetRequest)
    - [GetResponse](#peloton.api.v0.job.GetResponse)
    - [GetResponse.Error](#peloton.api.v0.job.GetResponse.Error)
    - [InvalidJobConfig](#peloton.api.v0.job.InvalidJobConfig)
    - [InvalidJobId](#peloton.api.v0.job.InvalidJobId)
    - [JobAlreadyExists](#peloton.api.v0.job.JobAlreadyExists)
    - [JobConfig](#peloton.api.v0.job.JobConfig)
    - [JobConfig.InstanceConfigEntry](#peloton.api.v0.job.JobConfig.InstanceConfigEntry)
    - [JobInfo](#peloton.api.v0.job.JobInfo)
    - [JobNotFound](#peloton.api.v0.job.JobNotFound)
    - [JobSummary](#peloton.api.v0.job.JobSummary)
    - [QueryRequest](#peloton.api.v0.job.QueryRequest)
    - [QueryResponse](#peloton.api.v0.job.QueryResponse)
    - [QueryResponse.Error](#peloton.api.v0.job.QueryResponse.Error)
    - [QuerySpec](#peloton.api.v0.job.QuerySpec)
    - [RefreshRequest](#peloton.api.v0.job.RefreshRequest)
    - [RefreshResponse](#peloton.api.v0.job.RefreshResponse)
    - [RestartConfig](#peloton.api.v0.job.RestartConfig)
    - [RestartRequest](#peloton.api.v0.job.RestartRequest)
    - [RestartResponse](#peloton.api.v0.job.RestartResponse)
    - [RuntimeInfo](#peloton.api.v0.job.RuntimeInfo)
    - [RuntimeInfo.ResourceUsageEntry](#peloton.api.v0.job.RuntimeInfo.ResourceUsageEntry)
    - [RuntimeInfo.TaskConfigVersionStatsEntry](#peloton.api.v0.job.RuntimeInfo.TaskConfigVersionStatsEntry)
    - [RuntimeInfo.TaskStatsEntry](#peloton.api.v0.job.RuntimeInfo.TaskStatsEntry)
    - [SlaConfig](#peloton.api.v0.job.SlaConfig)
    - [StartConfig](#peloton.api.v0.job.StartConfig)
    - [StartRequest](#peloton.api.v0.job.StartRequest)
    - [StartResponse](#peloton.api.v0.job.StartResponse)
    - [StopConfig](#peloton.api.v0.job.StopConfig)
    - [StopRequest](#peloton.api.v0.job.StopRequest)
    - [StopResponse](#peloton.api.v0.job.StopResponse)
    - [UpdateRequest](#peloton.api.v0.job.UpdateRequest)
    - [UpdateResponse](#peloton.api.v0.job.UpdateResponse)
    - [UpdateResponse.Error](#peloton.api.v0.job.UpdateResponse.Error)
  
    - [JobState](#peloton.api.v0.job.JobState)
    - [JobType](#peloton.api.v0.job.JobType)
  
  
    - [JobManager](#peloton.api.v0.job.JobManager)
  

- [volume.proto](#volume.proto)
    - [PersistentVolumeInfo](#peloton.api.v0.volume.PersistentVolumeInfo)
  
    - [VolumeState](#peloton.api.v0.volume.VolumeState)
  
  
  

- [query.proto](#query.proto)
    - [OrderBy](#peloton.api.v1alpha.query.OrderBy)
    - [Pagination](#peloton.api.v1alpha.query.Pagination)
    - [PaginationSpec](#peloton.api.v1alpha.query.PaginationSpec)
    - [PropertyPath](#peloton.api.v1alpha.query.PropertyPath)
  
    - [OrderBy.Order](#peloton.api.v1alpha.query.OrderBy.Order)
  
  
  

- [pod.proto](#pod.proto)
    - [AndConstraint](#peloton.api.v1alpha.pod.AndConstraint)
    - [Constraint](#peloton.api.v1alpha.pod.Constraint)
    - [ContainerSpec](#peloton.api.v1alpha.pod.ContainerSpec)
    - [ContainerStatus](#peloton.api.v1alpha.pod.ContainerStatus)
    - [ContainerStatus.PortsEntry](#peloton.api.v1alpha.pod.ContainerStatus.PortsEntry)
    - [HealthCheckSpec](#peloton.api.v1alpha.pod.HealthCheckSpec)
    - [HealthCheckSpec.CommandCheck](#peloton.api.v1alpha.pod.HealthCheckSpec.CommandCheck)
    - [HealthStatus](#peloton.api.v1alpha.pod.HealthStatus)
    - [InstanceIDRange](#peloton.api.v1alpha.pod.InstanceIDRange)
    - [LabelConstraint](#peloton.api.v1alpha.pod.LabelConstraint)
    - [OrConstraint](#peloton.api.v1alpha.pod.OrConstraint)
    - [PersistentVolumeSpec](#peloton.api.v1alpha.pod.PersistentVolumeSpec)
    - [PodEvent](#peloton.api.v1alpha.pod.PodEvent)
    - [PodInfo](#peloton.api.v1alpha.pod.PodInfo)
    - [PodSpec](#peloton.api.v1alpha.pod.PodSpec)
    - [PodStatus](#peloton.api.v1alpha.pod.PodStatus)
    - [PodStatus.ResourceUsageEntry](#peloton.api.v1alpha.pod.PodStatus.ResourceUsageEntry)
    - [PodSummary](#peloton.api.v1alpha.pod.PodSummary)
    - [PortSpec](#peloton.api.v1alpha.pod.PortSpec)
    - [PreemptionPolicy](#peloton.api.v1alpha.pod.PreemptionPolicy)
    - [QuerySpec](#peloton.api.v1alpha.pod.QuerySpec)
    - [ResourceSpec](#peloton.api.v1alpha.pod.ResourceSpec)
    - [RestartPolicy](#peloton.api.v1alpha.pod.RestartPolicy)
    - [TerminationStatus](#peloton.api.v1alpha.pod.TerminationStatus)
  
    - [Constraint.Type](#peloton.api.v1alpha.pod.Constraint.Type)
    - [ContainerState](#peloton.api.v1alpha.pod.ContainerState)
    - [HealthCheckSpec.HealthCheckType](#peloton.api.v1alpha.pod.HealthCheckSpec.HealthCheckType)
    - [HealthState](#peloton.api.v1alpha.pod.HealthState)
    - [LabelConstraint.Condition](#peloton.api.v1alpha.pod.LabelConstraint.Condition)
    - [LabelConstraint.Kind](#peloton.api.v1alpha.pod.LabelConstraint.Kind)
    - [PodState](#peloton.api.v1alpha.pod.PodState)
    - [TerminationStatus.Reason](#peloton.api.v1alpha.pod.TerminationStatus.Reason)
  
  
  

- [host.proto](#host.proto)
    - [HostInfo](#peloton.api.v1alpha.host.HostInfo)
  
    - [HostState](#peloton.api.v1alpha.host.HostState)
  
  
  

- [respool.proto](#respool.proto)
    - [ControllerLimit](#peloton.api.v1alpha.respool.ControllerLimit)
    - [ResourcePoolInfo](#peloton.api.v1alpha.respool.ResourcePoolInfo)
    - [ResourcePoolPath](#peloton.api.v1alpha.respool.ResourcePoolPath)
    - [ResourcePoolSpec](#peloton.api.v1alpha.respool.ResourcePoolSpec)
    - [ResourceSpec](#peloton.api.v1alpha.respool.ResourceSpec)
    - [ResourceUsage](#peloton.api.v1alpha.respool.ResourceUsage)
    - [SlackLimit](#peloton.api.v1alpha.respool.SlackLimit)
  
    - [ReservationType](#peloton.api.v1alpha.respool.ReservationType)
    - [SchedulingPolicy](#peloton.api.v1alpha.respool.SchedulingPolicy)
  
  
  

- [watch.proto](#watch.proto)
    - [PodFilter](#peloton.api.v1alpha.watch.PodFilter)
    - [StatelessJobFilter](#peloton.api.v1alpha.watch.StatelessJobFilter)
  
  
  
  

- [volume.proto](#volume.proto)
    - [PersistentVolumeInfo](#peloton.api.v1alpha.volume.PersistentVolumeInfo)
  
    - [VolumeState](#peloton.api.v1alpha.volume.VolumeState)
  
  
  

- [host_svc.proto](#host_svc.proto)
    - [CompleteMaintenanceRequest](#peloton.api.v0.host.svc.CompleteMaintenanceRequest)
    - [CompleteMaintenanceResponse](#peloton.api.v0.host.svc.CompleteMaintenanceResponse)
    - [QueryHostsRequest](#peloton.api.v0.host.svc.QueryHostsRequest)
    - [QueryHostsResponse](#peloton.api.v0.host.svc.QueryHostsResponse)
    - [StartMaintenanceRequest](#peloton.api.v0.host.svc.StartMaintenanceRequest)
    - [StartMaintenanceResponse](#peloton.api.v0.host.svc.StartMaintenanceResponse)
  
  
  
    - [HostService](#peloton.api.v0.host.svc.HostService)
  

- [update_svc.proto](#update_svc.proto)
    - [AbortUpdateRequest](#peloton.api.v0.update.svc.AbortUpdateRequest)
    - [AbortUpdateResponse](#peloton.api.v0.update.svc.AbortUpdateResponse)
    - [CreateUpdateRequest](#peloton.api.v0.update.svc.CreateUpdateRequest)
    - [CreateUpdateResponse](#peloton.api.v0.update.svc.CreateUpdateResponse)
    - [GetUpdateCacheRequest](#peloton.api.v0.update.svc.GetUpdateCacheRequest)
    - [GetUpdateCacheResponse](#peloton.api.v0.update.svc.GetUpdateCacheResponse)
    - [GetUpdateRequest](#peloton.api.v0.update.svc.GetUpdateRequest)
    - [GetUpdateResponse](#peloton.api.v0.update.svc.GetUpdateResponse)
    - [ListUpdatesRequest](#peloton.api.v0.update.svc.ListUpdatesRequest)
    - [ListUpdatesResponse](#peloton.api.v0.update.svc.ListUpdatesResponse)
    - [PauseUpdateRequest](#peloton.api.v0.update.svc.PauseUpdateRequest)
    - [PauseUpdateResponse](#peloton.api.v0.update.svc.PauseUpdateResponse)
    - [ResumeUpdateRequest](#peloton.api.v0.update.svc.ResumeUpdateRequest)
    - [ResumeUpdateResponse](#peloton.api.v0.update.svc.ResumeUpdateResponse)
    - [RollbackUpdateRequest](#peloton.api.v0.update.svc.RollbackUpdateRequest)
    - [RollbackUpdateResponse](#peloton.api.v0.update.svc.RollbackUpdateResponse)
  
  
  
    - [UpdateService](#peloton.api.v0.update.svc.UpdateService)
  

- [respool_svc.proto](#respool_svc.proto)
    - [CreateResourcePoolRequest](#peloton.api.v0.respool.CreateResourcePoolRequest)
    - [CreateResourcePoolResponse](#peloton.api.v0.respool.CreateResourcePoolResponse)
    - [DeleteResourcePoolRequest](#peloton.api.v0.respool.DeleteResourcePoolRequest)
    - [DeleteResourcePoolResponse](#peloton.api.v0.respool.DeleteResourcePoolResponse)
    - [GetResourcePoolRequest](#peloton.api.v0.respool.GetResourcePoolRequest)
    - [GetResourcePoolResponse](#peloton.api.v0.respool.GetResourcePoolResponse)
    - [LookupResourcePoolIDRequest](#peloton.api.v0.respool.LookupResourcePoolIDRequest)
    - [LookupResourcePoolIDResponse](#peloton.api.v0.respool.LookupResourcePoolIDResponse)
    - [QueryResourcePoolsRequest](#peloton.api.v0.respool.QueryResourcePoolsRequest)
    - [QueryResourcePoolsResponse](#peloton.api.v0.respool.QueryResourcePoolsResponse)
    - [UpdateResourcePoolRequest](#peloton.api.v0.respool.UpdateResourcePoolRequest)
    - [UpdateResourcePoolResponse](#peloton.api.v0.respool.UpdateResourcePoolResponse)
  
  
  
    - [ResourcePoolService](#peloton.api.v0.respool.ResourcePoolService)
  

- [task_svc.proto](#task_svc.proto)
    - [BrowseSandboxRequest](#peloton.api.v0.task.svc.BrowseSandboxRequest)
    - [BrowseSandboxResponse](#peloton.api.v0.task.svc.BrowseSandboxResponse)
    - [GetPodEventsRequest](#peloton.api.v0.task.svc.GetPodEventsRequest)
    - [GetPodEventsResponse](#peloton.api.v0.task.svc.GetPodEventsResponse)
    - [GetPodEventsResponse.Error](#peloton.api.v0.task.svc.GetPodEventsResponse.Error)
    - [GetTaskCacheRequest](#peloton.api.v0.task.svc.GetTaskCacheRequest)
    - [GetTaskCacheResponse](#peloton.api.v0.task.svc.GetTaskCacheResponse)
    - [GetTaskRequest](#peloton.api.v0.task.svc.GetTaskRequest)
    - [GetTaskResponse](#peloton.api.v0.task.svc.GetTaskResponse)
    - [ListTasksRequest](#peloton.api.v0.task.svc.ListTasksRequest)
    - [ListTasksResponse](#peloton.api.v0.task.svc.ListTasksResponse)
    - [ListTasksResponse.TasksEntry](#peloton.api.v0.task.svc.ListTasksResponse.TasksEntry)
    - [PodEvent](#peloton.api.v0.task.svc.PodEvent)
    - [QueryTasksRequest](#peloton.api.v0.task.svc.QueryTasksRequest)
    - [QueryTasksResponse](#peloton.api.v0.task.svc.QueryTasksResponse)
    - [RefreshTasksRequest](#peloton.api.v0.task.svc.RefreshTasksRequest)
    - [RefreshTasksResponse](#peloton.api.v0.task.svc.RefreshTasksResponse)
    - [RestartTasksRequest](#peloton.api.v0.task.svc.RestartTasksRequest)
    - [RestartTasksResponse](#peloton.api.v0.task.svc.RestartTasksResponse)
    - [StartTasksRequest](#peloton.api.v0.task.svc.StartTasksRequest)
    - [StartTasksResponse](#peloton.api.v0.task.svc.StartTasksResponse)
    - [StopTasksRequest](#peloton.api.v0.task.svc.StopTasksRequest)
    - [StopTasksResponse](#peloton.api.v0.task.svc.StopTasksResponse)
    - [TaskEventsError](#peloton.api.v0.task.svc.TaskEventsError)
  
  
  
    - [TaskService](#peloton.api.v0.task.svc.TaskService)
  

- [job_svc.proto](#job_svc.proto)
    - [CreateJobRequest](#peloton.api.v0.job.svc.CreateJobRequest)
    - [CreateJobResponse](#peloton.api.v0.job.svc.CreateJobResponse)
    - [DeleteJobRequest](#peloton.api.v0.job.svc.DeleteJobRequest)
    - [DeleteJobResponse](#peloton.api.v0.job.svc.DeleteJobResponse)
    - [GetJobCacheRequest](#peloton.api.v0.job.svc.GetJobCacheRequest)
    - [GetJobCacheResponse](#peloton.api.v0.job.svc.GetJobCacheResponse)
    - [GetJobRequest](#peloton.api.v0.job.svc.GetJobRequest)
    - [GetJobResponse](#peloton.api.v0.job.svc.GetJobResponse)
    - [QueryJobsRequest](#peloton.api.v0.job.svc.QueryJobsRequest)
    - [QueryJobsResponse](#peloton.api.v0.job.svc.QueryJobsResponse)
    - [RefreshJobRequest](#peloton.api.v0.job.svc.RefreshJobRequest)
    - [RefreshJobResponse](#peloton.api.v0.job.svc.RefreshJobResponse)
    - [RestartConfig](#peloton.api.v0.job.svc.RestartConfig)
    - [RestartJobRequest](#peloton.api.v0.job.svc.RestartJobRequest)
    - [RestartJobResponse](#peloton.api.v0.job.svc.RestartJobResponse)
    - [StartConfig](#peloton.api.v0.job.svc.StartConfig)
    - [StartJobRequest](#peloton.api.v0.job.svc.StartJobRequest)
    - [StartJobResponse](#peloton.api.v0.job.svc.StartJobResponse)
    - [StopConfig](#peloton.api.v0.job.svc.StopConfig)
    - [StopJobRequest](#peloton.api.v0.job.svc.StopJobRequest)
    - [StopJobResponse](#peloton.api.v0.job.svc.StopJobResponse)
    - [UpdateJobRequest](#peloton.api.v0.job.svc.UpdateJobRequest)
    - [UpdateJobResponse](#peloton.api.v0.job.svc.UpdateJobResponse)
  
  
  
    - [JobService](#peloton.api.v0.job.svc.JobService)
  

- [volume_svc.proto](#volume_svc.proto)
    - [DeleteVolumeRequest](#peloton.api.v0.volume.svc.DeleteVolumeRequest)
    - [DeleteVolumeResponse](#peloton.api.v0.volume.svc.DeleteVolumeResponse)
    - [GetVolumeRequest](#peloton.api.v0.volume.svc.GetVolumeRequest)
    - [GetVolumeResponse](#peloton.api.v0.volume.svc.GetVolumeResponse)
    - [ListVolumesRequest](#peloton.api.v0.volume.svc.ListVolumesRequest)
    - [ListVolumesResponse](#peloton.api.v0.volume.svc.ListVolumesResponse)
    - [ListVolumesResponse.VolumesEntry](#peloton.api.v0.volume.svc.ListVolumesResponse.VolumesEntry)
  
  
  
    - [VolumeService](#peloton.api.v0.volume.svc.VolumeService)
  

- [pod_svc.proto](#pod_svc.proto)
    - [BrowsePodSandboxRequest](#peloton.api.v1alpha.pod.svc.BrowsePodSandboxRequest)
    - [BrowsePodSandboxResponse](#peloton.api.v1alpha.pod.svc.BrowsePodSandboxResponse)
    - [DeletePodEventsRequest](#peloton.api.v1alpha.pod.svc.DeletePodEventsRequest)
    - [DeletePodEventsResponse](#peloton.api.v1alpha.pod.svc.DeletePodEventsResponse)
    - [GetPodCacheRequest](#peloton.api.v1alpha.pod.svc.GetPodCacheRequest)
    - [GetPodCacheResponse](#peloton.api.v1alpha.pod.svc.GetPodCacheResponse)
    - [GetPodEventsRequest](#peloton.api.v1alpha.pod.svc.GetPodEventsRequest)
    - [GetPodEventsResponse](#peloton.api.v1alpha.pod.svc.GetPodEventsResponse)
    - [GetPodRequest](#peloton.api.v1alpha.pod.svc.GetPodRequest)
    - [GetPodResponse](#peloton.api.v1alpha.pod.svc.GetPodResponse)
    - [RefreshPodRequest](#peloton.api.v1alpha.pod.svc.RefreshPodRequest)
    - [RefreshPodResponse](#peloton.api.v1alpha.pod.svc.RefreshPodResponse)
    - [RestartPodRequest](#peloton.api.v1alpha.pod.svc.RestartPodRequest)
    - [RestartPodResponse](#peloton.api.v1alpha.pod.svc.RestartPodResponse)
    - [StartPodRequest](#peloton.api.v1alpha.pod.svc.StartPodRequest)
    - [StartPodResponse](#peloton.api.v1alpha.pod.svc.StartPodResponse)
    - [StopPodRequest](#peloton.api.v1alpha.pod.svc.StopPodRequest)
    - [StopPodResponse](#peloton.api.v1alpha.pod.svc.StopPodResponse)
  
  
  
    - [PodService](#peloton.api.v1alpha.pod.svc.PodService)
  

- [host_svc.proto](#host_svc.proto)
    - [CompleteMaintenanceRequest](#peloton.api.v1alpha.host.svc.CompleteMaintenanceRequest)
    - [CompleteMaintenanceResponse](#peloton.api.v1alpha.host.svc.CompleteMaintenanceResponse)
    - [QueryHostsRequest](#peloton.api.v1alpha.host.svc.QueryHostsRequest)
    - [QueryHostsResponse](#peloton.api.v1alpha.host.svc.QueryHostsResponse)
    - [StartMaintenanceRequest](#peloton.api.v1alpha.host.svc.StartMaintenanceRequest)
    - [StartMaintenanceResponse](#peloton.api.v1alpha.host.svc.StartMaintenanceResponse)
  
  
  
    - [HostService](#peloton.api.v1alpha.host.svc.HostService)
  

- [respool_svc.proto](#respool_svc.proto)
    - [CreateResourcePoolRequest](#peloton.api.v1alpha.respool.CreateResourcePoolRequest)
    - [CreateResourcePoolResponse](#peloton.api.v1alpha.respool.CreateResourcePoolResponse)
    - [DeleteResourcePoolRequest](#peloton.api.v1alpha.respool.DeleteResourcePoolRequest)
    - [DeleteResourcePoolResponse](#peloton.api.v1alpha.respool.DeleteResourcePoolResponse)
    - [GetResourcePoolRequest](#peloton.api.v1alpha.respool.GetResourcePoolRequest)
    - [GetResourcePoolResponse](#peloton.api.v1alpha.respool.GetResourcePoolResponse)
    - [LookupResourcePoolIDRequest](#peloton.api.v1alpha.respool.LookupResourcePoolIDRequest)
    - [LookupResourcePoolIDResponse](#peloton.api.v1alpha.respool.LookupResourcePoolIDResponse)
    - [QueryResourcePoolsRequest](#peloton.api.v1alpha.respool.QueryResourcePoolsRequest)
    - [QueryResourcePoolsResponse](#peloton.api.v1alpha.respool.QueryResourcePoolsResponse)
    - [UpdateResourcePoolRequest](#peloton.api.v1alpha.respool.UpdateResourcePoolRequest)
    - [UpdateResourcePoolResponse](#peloton.api.v1alpha.respool.UpdateResourcePoolResponse)
  
  
  
    - [ResourcePoolService](#peloton.api.v1alpha.respool.ResourcePoolService)
  

- [stateless.proto](#stateless.proto)
    - [CreateSpec](#peloton.api.v1alpha.job.stateless.CreateSpec)
    - [JobInfo](#peloton.api.v1alpha.job.stateless.JobInfo)
    - [JobSpec](#peloton.api.v1alpha.job.stateless.JobSpec)
    - [JobSpec.InstanceSpecEntry](#peloton.api.v1alpha.job.stateless.JobSpec.InstanceSpecEntry)
    - [JobStatus](#peloton.api.v1alpha.job.stateless.JobStatus)
    - [JobStatus.PodConfigurationVersionStatsEntry](#peloton.api.v1alpha.job.stateless.JobStatus.PodConfigurationVersionStatsEntry)
    - [JobStatus.PodStatsEntry](#peloton.api.v1alpha.job.stateless.JobStatus.PodStatsEntry)
    - [JobSummary](#peloton.api.v1alpha.job.stateless.JobSummary)
    - [QuerySpec](#peloton.api.v1alpha.job.stateless.QuerySpec)
    - [SlaSpec](#peloton.api.v1alpha.job.stateless.SlaSpec)
    - [UpdateSpec](#peloton.api.v1alpha.job.stateless.UpdateSpec)
    - [WorkflowEvent](#peloton.api.v1alpha.job.stateless.WorkflowEvent)
    - [WorkflowInfo](#peloton.api.v1alpha.job.stateless.WorkflowInfo)
    - [WorkflowStatus](#peloton.api.v1alpha.job.stateless.WorkflowStatus)
  
    - [JobState](#peloton.api.v1alpha.job.stateless.JobState)
    - [WorkflowState](#peloton.api.v1alpha.job.stateless.WorkflowState)
    - [WorkflowType](#peloton.api.v1alpha.job.stateless.WorkflowType)
  
  
  

- [watch_svc.proto](#watch_svc.proto)
    - [CancelRequest](#peloton.api.v1alpha.watch.svc.CancelRequest)
    - [CancelResponse](#peloton.api.v1alpha.watch.svc.CancelResponse)
    - [WatchRequest](#peloton.api.v1alpha.watch.svc.WatchRequest)
    - [WatchResponse](#peloton.api.v1alpha.watch.svc.WatchResponse)
  
  
  
    - [WatchService](#peloton.api.v1alpha.watch.svc.WatchService)
  

- [volume_svc.proto](#volume_svc.proto)
    - [DeleteVolumeRequest](#peloton.api.v1alpha.volume.svc.DeleteVolumeRequest)
    - [DeleteVolumeResponse](#peloton.api.v1alpha.volume.svc.DeleteVolumeResponse)
    - [GetVolumeRequest](#peloton.api.v1alpha.volume.svc.GetVolumeRequest)
    - [GetVolumeResponse](#peloton.api.v1alpha.volume.svc.GetVolumeResponse)
    - [ListVolumesRequest](#peloton.api.v1alpha.volume.svc.ListVolumesRequest)
    - [ListVolumesResponse](#peloton.api.v1alpha.volume.svc.ListVolumesResponse)
    - [ListVolumesResponse.VolumesEntry](#peloton.api.v1alpha.volume.svc.ListVolumesResponse.VolumesEntry)
  
  
  
    - [VolumeService](#peloton.api.v1alpha.volume.svc.VolumeService)
  

- [stateless_svc.proto](#stateless_svc.proto)
    - [AbortJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowRequest)
    - [AbortJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowResponse)
    - [CreateJobRequest](#peloton.api.v1alpha.job.stateless.svc.CreateJobRequest)
    - [CreateJobResponse](#peloton.api.v1alpha.job.stateless.svc.CreateJobResponse)
    - [DeleteJobRequest](#peloton.api.v1alpha.job.stateless.svc.DeleteJobRequest)
    - [DeleteJobResponse](#peloton.api.v1alpha.job.stateless.svc.DeleteJobResponse)
    - [GetJobCacheRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobCacheRequest)
    - [GetJobCacheResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobCacheResponse)
    - [GetJobIDFromJobNameRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameRequest)
    - [GetJobIDFromJobNameResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameResponse)
    - [GetJobRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobRequest)
    - [GetJobResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobResponse)
    - [GetReplaceJobDiffRequest](#peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffRequest)
    - [GetReplaceJobDiffResponse](#peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffResponse)
    - [GetWorkflowEventsRequest](#peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsRequest)
    - [GetWorkflowEventsResponse](#peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsResponse)
    - [ListJobUpdatesRequest](#peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesRequest)
    - [ListJobUpdatesResponse](#peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesResponse)
    - [ListJobsRequest](#peloton.api.v1alpha.job.stateless.svc.ListJobsRequest)
    - [ListJobsResponse](#peloton.api.v1alpha.job.stateless.svc.ListJobsResponse)
    - [ListPodsRequest](#peloton.api.v1alpha.job.stateless.svc.ListPodsRequest)
    - [ListPodsResponse](#peloton.api.v1alpha.job.stateless.svc.ListPodsResponse)
    - [PatchJobRequest](#peloton.api.v1alpha.job.stateless.svc.PatchJobRequest)
    - [PatchJobResponse](#peloton.api.v1alpha.job.stateless.svc.PatchJobResponse)
    - [PauseJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowRequest)
    - [PauseJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowResponse)
    - [QueryJobsRequest](#peloton.api.v1alpha.job.stateless.svc.QueryJobsRequest)
    - [QueryJobsResponse](#peloton.api.v1alpha.job.stateless.svc.QueryJobsResponse)
    - [QueryPodsRequest](#peloton.api.v1alpha.job.stateless.svc.QueryPodsRequest)
    - [QueryPodsResponse](#peloton.api.v1alpha.job.stateless.svc.QueryPodsResponse)
    - [RefreshJobRequest](#peloton.api.v1alpha.job.stateless.svc.RefreshJobRequest)
    - [RefreshJobResponse](#peloton.api.v1alpha.job.stateless.svc.RefreshJobResponse)
    - [ReplaceJobRequest](#peloton.api.v1alpha.job.stateless.svc.ReplaceJobRequest)
    - [ReplaceJobResponse](#peloton.api.v1alpha.job.stateless.svc.ReplaceJobResponse)
    - [RestartJobRequest](#peloton.api.v1alpha.job.stateless.svc.RestartJobRequest)
    - [RestartJobResponse](#peloton.api.v1alpha.job.stateless.svc.RestartJobResponse)
    - [ResumeJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowRequest)
    - [ResumeJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowResponse)
    - [StartJobRequest](#peloton.api.v1alpha.job.stateless.svc.StartJobRequest)
    - [StartJobResponse](#peloton.api.v1alpha.job.stateless.svc.StartJobResponse)
    - [StopJobRequest](#peloton.api.v1alpha.job.stateless.svc.StopJobRequest)
    - [StopJobResponse](#peloton.api.v1alpha.job.stateless.svc.StopJobResponse)
  
  
  
    - [JobService](#peloton.api.v1alpha.job.stateless.svc.JobService)
  

- [Scalar Value Types](#scalar-value-types)



<a name="timestamp.proto"/>
<p align="right"><a href="#top">Top</a></p>

## timestamp.proto



<a name="google.protobuf.Timestamp"/>

### Timestamp
A Timestamp represents a point in time independent of any time zone
or calendar, represented as seconds and fractions of seconds at
nanosecond resolution in UTC Epoch time. It is encoded using the
Proleptic Gregorian Calendar which extends the Gregorian calendar
backwards to year one. It is encoded assuming all minutes are 60
seconds long, i.e. leap seconds are &#34;smeared&#34; so that no leap second
table is needed for interpretation. Range is from
0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z.
By restricting to that range, we ensure that we can convert to
and from  RFC 3339 date strings.
See [https://www.ietf.org/rfc/rfc3339.txt](https://www.ietf.org/rfc/rfc3339.txt).

# Examples

Example 1: Compute Timestamp from POSIX `time()`.

Timestamp timestamp;
timestamp.set_seconds(time(NULL));
timestamp.set_nanos(0);

Example 2: Compute Timestamp from POSIX `gettimeofday()`.

struct timeval tv;
gettimeofday(&amp;tv, NULL);

Timestamp timestamp;
timestamp.set_seconds(tv.tv_sec);
timestamp.set_nanos(tv.tv_usec * 1000);

Example 3: Compute Timestamp from Win32 `GetSystemTimeAsFileTime()`.

FILETIME ft;
GetSystemTimeAsFileTime(&amp;ft);
UINT64 ticks = (((UINT64)ft.dwHighDateTime) &lt;&lt; 32) | ft.dwLowDateTime;

A Windows tick is 100 nanoseconds. Windows epoch 1601-01-01T00:00:00Z
is 11644473600 seconds before Unix epoch 1970-01-01T00:00:00Z.
Timestamp timestamp;
timestamp.set_seconds((INT64) ((ticks / 10000000) - 11644473600LL));
timestamp.set_nanos((INT32) ((ticks % 10000000) * 100));

Example 4: Compute Timestamp from Java `System.currentTimeMillis()`.

long millis = System.currentTimeMillis();

Timestamp timestamp = Timestamp.newBuilder().setSeconds(millis / 1000)
.setNanos((int) ((millis % 1000) * 1000000)).build();


Example 5: Compute Timestamp from current time in Python.

timestamp = Timestamp()
timestamp.GetCurrentTime()

# JSON Mapping

In JSON format, the Timestamp type is encoded as a string in the
[RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format. That is, the
format is &#34;{year}-{month}-{day}T{hour}:{min}:{sec}[.{frac_sec}]Z&#34;
where {year} is always expressed using four digits while {month}, {day},
{hour}, {min}, and {sec} are zero-padded to two digits each. The fractional
seconds, which can go up to 9 digits (i.e. up to 1 nanosecond resolution),
are optional. The &#34;Z&#34; suffix indicates the timezone (&#34;UTC&#34;); the timezone
is required, though only UTC (as indicated by &#34;Z&#34;) is presently supported.

For example, &#34;2017-01-15T01:30:15.01Z&#34; encodes 15.01 seconds past
01:30 UTC on January 15, 2017.

In JavaScript, one can convert a Date object to this format using the
standard [toISOString()](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString]
method. In Python, a standard `datetime.datetime` object can be converted
to this format using [`strftime`](https://docs.python.org/2/library/time.html#time.strftime)
with the time format spec &#39;%Y-%m-%dT%H:%M:%S.%fZ&#39;. Likewise, in Java, one
can use the Joda Time&#39;s [`ISODateTimeFormat.dateTime()`](
http://www.joda.org/joda-time/apidocs/org/joda/time/format/ISODateTimeFormat.html#dateTime--)
to obtain a formatter capable of generating timestamps in this format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| seconds | [int64](#int64) |  | Represents seconds of UTC time since Unix epoch 1970-01-01T00:00:00Z. Must be from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59Z inclusive. |
| nanos | [int32](#int32) |  | Non-negative fractions of a second at nanosecond resolution. Negative second values with fractions must still have non-negative nanos values that count forward in time. Must be from 0 to 999,999,999 inclusive. |





 

 

 

 



<a name="peloton.proto"/>
<p align="right"><a href="#top">Top</a></p>

## peloton.proto



<a name="peloton.api.v0.peloton.ChangeLog"/>

### ChangeLog
Change log of an entity info, such as Job config etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  | Version number of the entity info which is monotonically increasing. Clients can use this to guide against race conditions using MVCC. |
| createdAt | [uint64](#uint64) |  | The timestamp when the entity info is created |
| updatedAt | [uint64](#uint64) |  | The timestamp when the entity info is updated |
| updatedBy | [string](#string) |  | The user or service that updated the entity info |






<a name="peloton.api.v0.peloton.HostOfferID"/>

### HostOfferID
A unique ID assigned to offers from a host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.JobID"/>

### JobID
A unique ID assigned to a Job. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.Label"/>

### Label
Key, value pair used to store free form user-data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.OpaqueData"/>

### OpaqueData
Opaque data passed to Peloton from the client.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.ResourcePoolID"/>

### ResourcePoolID
A unique ID assigned to a Resource Pool. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.Secret"/>

### Secret
Secret is used to store secrets per job and contains
ID, absolute container mount path and base64 encoded secret data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [SecretID](#peloton.api.v0.peloton.SecretID) |  | UUID of the secret |
| path | [string](#string) |  | Path at which the secret file will be mounted in the container |
| value | [Secret.Value](#peloton.api.v0.peloton.Secret.Value) |  | Secret value |






<a name="peloton.api.v0.peloton.Secret.Value"/>

### Secret.Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Secret data as byte array |






<a name="peloton.api.v0.peloton.SecretID"/>

### SecretID
A unique ID assigned to a Secret. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.TaskID"/>

### TaskID
A unique ID assigned to a Task (aka job instance). The task ID is in
the format of JobID-&lt;InstanceID&gt;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.TimeRange"/>

### TimeRange
Time range specified by min and max timestamps.
Time range is left closed and right open: [min, max)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| min | [.google.protobuf.Timestamp](#peloton.api.v0.peloton..google.protobuf.Timestamp) |  |  |
| max | [.google.protobuf.Timestamp](#peloton.api.v0.peloton..google.protobuf.Timestamp) |  |  |






<a name="peloton.api.v0.peloton.UpdateID"/>

### UpdateID
A unique ID assigned to a update.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.peloton.VolumeID"/>

### VolumeID
A unique ID assigned to a Volume. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 

 

 

 



<a name="errors.proto"/>
<p align="right"><a href="#top">Top</a></p>

## errors.proto



<a name="peloton.api.v0.errors.InvalidRespool"/>

### InvalidRespool



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respoolID | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.errors..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.errors.JobGetRuntimeFail"/>

### JobGetRuntimeFail



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.errors..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.errors.JobNotFound"/>

### JobNotFound



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.errors..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.errors.UnknownError"/>

### UnknownError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |





 

 

 

 



<a name="query.proto"/>
<p align="right"><a href="#top">Top</a></p>

## query.proto



<a name="peloton.api.v0.query.OrderBy"/>

### OrderBy
Order by clause of a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| order | [OrderBy.Order](#peloton.api.v0.query.OrderBy.Order) |  |  |
| property | [PropertyPath](#peloton.api.v0.query.PropertyPath) |  |  |






<a name="peloton.api.v0.query.Pagination"/>

### Pagination
Generic pagination for a list of records to be returned by a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the pagination for a query result |
| limit | [uint32](#uint32) |  | Limit of the pagination for a query result |
| total | [uint32](#uint32) |  | Total number of records for a query result |






<a name="peloton.api.v0.query.PaginationSpec"/>

### PaginationSpec
Pagination query spec used as argument to queries that returns a Pagination
result.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the query for pagination |
| limit | [uint32](#uint32) |  | Limit per page of the query for pagination |
| orderBy | [OrderBy](#peloton.api.v0.query.OrderBy) | repeated | List of fields to be order by in sequence |
| maxLimit | [uint32](#uint32) |  | Max limit of the pagination result. |






<a name="peloton.api.v0.query.PropertyPath"/>

### PropertyPath
A dot separated path to a object property such as config.name or
runtime.creationTime for a job object.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 


<a name="peloton.api.v0.query.OrderBy.Order"/>

### OrderBy.Order


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| ASC | 1 |  |
| DESC | 2 |  |


 

 

 



<a name="task.proto"/>
<p align="right"><a href="#top">Top</a></p>

## task.proto



<a name="peloton.api.v0.task.AndConstraint"/>

### AndConstraint
AndConstraint represents a logical &#39;and&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.v0.task.Constraint) | repeated |  |






<a name="peloton.api.v0.task.BrowseSandboxFailure"/>

### BrowseSandboxFailure
Failures for browsing sandbox files requests to mesos call.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.BrowseSandboxRequest"/>

### BrowseSandboxRequest
DEPRECATED by peloton.api.v0.task.svc.BrowseSandboxRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| instanceId | [uint32](#uint32) |  |  |
| taskId | [string](#string) |  | Get the sandbox path of a particular task of an instance. This should be set to the mesos task id in the runtime of the task for which the sandbox is being requested. If not provided, the path of the latest task is returned. |






<a name="peloton.api.v0.task.BrowseSandboxResponse"/>

### BrowseSandboxResponse
DEPRECATED by peloton.api.v0.task.svc.BrowseSandboxResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [BrowseSandboxResponse.Error](#peloton.api.v0.task.BrowseSandboxResponse.Error) |  |  |
| hostname | [string](#string) |  |  |
| port | [string](#string) |  |  |
| paths | [string](#string) | repeated |  |
| mesosMasterHostname | [string](#string) |  | Mesos Master hostname and port. |
| mesosMasterPort | [string](#string) |  |  |






<a name="peloton.api.v0.task.BrowseSandboxResponse.Error"/>

### BrowseSandboxResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange) |  |  |
| notRunning | [TaskNotRunning](#peloton.api.v0.task.TaskNotRunning) |  |  |
| failure | [BrowseSandboxFailure](#peloton.api.v0.task.BrowseSandboxFailure) |  |  |






<a name="peloton.api.v0.task.Constraint"/>

### Constraint
Constraint represents a host label constraint or a related tasks label constraint.
This is used to require that a host have certain label constraints or to require
that the tasks already running on the host have certain label constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Constraint.Type](#peloton.api.v0.task.Constraint.Type) |  |  |
| labelConstraint | [LabelConstraint](#peloton.api.v0.task.LabelConstraint) |  |  |
| andConstraint | [AndConstraint](#peloton.api.v0.task.AndConstraint) |  |  |
| orConstraint | [OrConstraint](#peloton.api.v0.task.OrConstraint) |  |  |






<a name="peloton.api.v0.task.DeletePodEventsRequest"/>

### DeletePodEventsRequest
Request message for TaskService.DeletePodEvents method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  | The job ID of the task |
| instanceId | [uint32](#uint32) |  | The instance ID of the task |
| runId | [uint64](#uint64) |  | deletes the run less than equal to runId. |






<a name="peloton.api.v0.task.DeletePodEventsResponse"/>

### DeletePodEventsResponse
Response message for TaskService.DeletePodEvents method.

Return errors:
INTERNAL:      if failed to delete task events for internal errors.






<a name="peloton.api.v0.task.GetCacheRequest"/>

### GetCacheRequest
DEPRECATED by peloton.api.task.svc.GetTaskCacheRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  | The job ID to look up the task. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to get. |






<a name="peloton.api.v0.task.GetCacheResponse"/>

### GetCacheResponse
DEPRECATED by peloton.api.task.svc.GetTaskCacheResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| runtime | [RuntimeInfo](#peloton.api.v0.task.RuntimeInfo) |  | The task runtime of the task. |






<a name="peloton.api.v0.task.GetPodEventsRequest"/>

### GetPodEventsRequest
Request message for TaskService.GetPodEvents method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  | The job ID of the task |
| instanceId | [uint32](#uint32) |  | The instance ID of the task |
| limit | [uint64](#uint64) |  | Defines the number of unique run ids for which the pod events will be returned. If a specific run id is requested, using runId, then this defaults to 1. Defaults to 10 otherwise. |
| runId | [string](#string) |  | Unique identifier to fetch pod events of an instance for a particular run. This is an optional parameter, if unset limit number of run ids worth of pod events will be returned. |






<a name="peloton.api.v0.task.GetPodEventsResponse"/>

### GetPodEventsResponse
Response message for TaskService.GetPodEvents method.

Return errors:
INTERNAL:      if failed to get task events for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [PodEvent](#peloton.api.v0.task.PodEvent) | repeated |  |
| error | [GetPodEventsResponse.Error](#peloton.api.v0.task.GetPodEventsResponse.Error) |  |  |






<a name="peloton.api.v0.task.GetPodEventsResponse.Error"/>

### GetPodEventsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.v0.task.svc.GetTaskRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| instanceId | [uint32](#uint32) |  |  |






<a name="peloton.api.v0.task.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.v0.task.svc.GetTaskResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [TaskInfo](#peloton.api.v0.task.TaskInfo) |  | DEPRECATED by repeated TaskInfo |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange) |  |  |
| results | [TaskInfo](#peloton.api.v0.task.TaskInfo) | repeated | Returns all active and completed tasks of the given instance. |






<a name="peloton.api.v0.task.HealthCheckConfig"/>

### HealthCheckConfig
Health check configuration for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  | Whether the health check is enabled. |
| initialIntervalSecs | [uint32](#uint32) |  | Start time wait in seconds. Zero or empty value would use default value of 15 from Mesos. |
| intervalSecs | [uint32](#uint32) |  | Interval in seconds between two health checks. Zero or empty value would use default value of 10 from Mesos. |
| maxConsecutiveFailures | [uint32](#uint32) |  | Max number of consecutive failures before failing health check. Zero or empty value would use default value of 3 from Mesos. |
| timeoutSecs | [uint32](#uint32) |  | Health check command timeout in seconds. Zero or empty value would use default value of 20 from Mesos. |
| type | [HealthCheckConfig.Type](#peloton.api.v0.task.HealthCheckConfig.Type) |  |  |
| commandCheck | [HealthCheckConfig.CommandCheck](#peloton.api.v0.task.HealthCheckConfig.CommandCheck) |  | Only applicable when type is `COMMAND`. |






<a name="peloton.api.v0.task.HealthCheckConfig.CommandCheck"/>

### HealthCheckConfig.CommandCheck



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [string](#string) |  | Health check command to be executed. Note that this command by default inherits all environment varibles from the task it&#39;s monitoring, unless `unshare_environments` is set to true. |
| unshareEnvironments | [bool](#bool) |  | If set, this check will not share the environment variables of the task. |






<a name="peloton.api.v0.task.InstanceIdOutOfRange"/>

### InstanceIdOutOfRange
DEPRECATED by google.rpc.OUT_OF_RANGE error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  | Entity ID of the job |
| instanceCount | [uint32](#uint32) |  | Instance count of the job |






<a name="peloton.api.v0.task.InstanceRange"/>

### InstanceRange
Task InstanceID range [from, to)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from | [uint32](#uint32) |  |  |
| to | [uint32](#uint32) |  |  |






<a name="peloton.api.v0.task.LabelConstraint"/>

### LabelConstraint
LabelConstraint represents a constraint on the number of occurrences of a given
label from the set of host labels or task labels present on the host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [LabelConstraint.Kind](#peloton.api.v0.task.LabelConstraint.Kind) |  | Determines which labels the constraint should apply to. |
| condition | [LabelConstraint.Condition](#peloton.api.v0.task.LabelConstraint.Condition) |  | Determines which constraint there should be on the number of occurrences of the label. |
| label | [.peloton.api.v0.peloton.Label](#peloton.api.v0.task..peloton.api.v0.peloton.Label) |  | The label which this defines a constraint on: For Kind == HOST, each attribute on Mesos agent is transformed to a label, with `hostname` as a special label which is always inferred from agent hostname and set. |
| requirement | [uint32](#uint32) |  | A limit on the number of occurrences of the label. |






<a name="peloton.api.v0.task.ListRequest"/>

### ListRequest
DEPRECATED by peloton.api.v0.task.svc.ListTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| range | [InstanceRange](#peloton.api.v0.task.InstanceRange) |  |  |






<a name="peloton.api.v0.task.ListResponse"/>

### ListResponse
DEPRECATED by peloton.api.v0.task.svc.ListTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [ListResponse.Result](#peloton.api.v0.task.ListResponse.Result) |  |  |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |






<a name="peloton.api.v0.task.ListResponse.Result"/>

### ListResponse.Result



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [ListResponse.Result.ValueEntry](#peloton.api.v0.task.ListResponse.Result.ValueEntry) | repeated |  |






<a name="peloton.api.v0.task.ListResponse.Result.ValueEntry"/>

### ListResponse.Result.ValueEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [TaskInfo](#peloton.api.v0.task.TaskInfo) |  |  |






<a name="peloton.api.v0.task.OrConstraint"/>

### OrConstraint
OrConstraint represents a logical &#39;or&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.v0.task.Constraint) | repeated |  |






<a name="peloton.api.v0.task.PersistentVolumeConfig"/>

### PersistentVolumeConfig
Persistent volume configuration for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| containerPath | [string](#string) |  | Volume mount path inside container. |
| sizeMB | [uint32](#uint32) |  | Volume size in MB. |






<a name="peloton.api.v0.task.PodEvent"/>

### PodEvent
Pod event of a Peloton pod instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The task ID of the task event. |
| actualState | [string](#string) |  | Actual state of an instance |
| goalState | [string](#string) |  | Goal State of an instance |
| timestamp | [string](#string) |  | The time when the event was created. The time is represented in RFC3339 form with UTC timezone. |
| configVersion | [uint64](#uint64) |  | The config version currently used by the runtime. |
| desiredConfigVersion | [uint64](#uint64) |  | The desired config version that should be used by the runtime. |
| agentID | [string](#string) |  | The agentID for the task |
| hostname | [string](#string) |  | The host on which the task is running |
| message | [string](#string) |  | Short human friendly message explaining state. |
| reason | [string](#string) |  | The short reason for the task event |
| prevTaskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The previous task ID of the pod event. |
| healthy | [string](#string) |  | The health check result of the task |
| desriedTaskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The desired mesos task ID of the task event. |






<a name="peloton.api.v0.task.PortConfig"/>

### PortConfig
Network port configuration for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the network port, e.g. http, tchannel. Required field. |
| value | [uint32](#uint32) |  | Static port number if any. If unset, will be dynamically allocated by the scheduler |
| envName | [string](#string) |  | Environment variable name to be exported when running a task for this port. Required field for dynamic port. |






<a name="peloton.api.v0.task.PreemptionPolicy"/>

### PreemptionPolicy
Preemption policy for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [PreemptionPolicy.Type](#peloton.api.v0.task.PreemptionPolicy.Type) |  |  |
| killOnPreempt | [bool](#bool) |  | This policy defines if the task should be restarted after it is preempted. If set to true the task will not be rescheduled after it is preempted. If set to false the task will be rescheduled. Defaults to false This only takes effect if the task is preemptible. |






<a name="peloton.api.v0.task.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.v0.task.svc.QueryTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| spec | [QuerySpec](#peloton.api.v0.task.QuerySpec) |  |  |






<a name="peloton.api.v0.task.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.v0.task.svc.QueryTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.v0.task.QueryResponse.Error) |  |  |
| records | [TaskInfo](#peloton.api.v0.task.TaskInfo) | repeated |  |
| pagination | [.peloton.api.v0.query.Pagination](#peloton.api.v0.task..peloton.api.v0.query.Pagination) |  |  |






<a name="peloton.api.v0.task.QueryResponse.Error"/>

### QueryResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |






<a name="peloton.api.v0.task.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for tasks. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.v0.query.PaginationSpec](#peloton.api.v0.task..peloton.api.v0.query.PaginationSpec) |  | DEPRECATED: use QueryJobRequest.pagination instead. The spec of how to do pagination for the query results. |
| taskStates | [TaskState](#peloton.api.v0.task.TaskState) | repeated | List of task states to query the tasks. Will match all tasks if the list is empty. |
| names | [string](#string) | repeated | List of task names to query the tasks. Will match all names if the list is empty. |
| hosts | [string](#string) | repeated | List of hosts to query the tasks. Will match all hosts if the list is empty. |






<a name="peloton.api.v0.task.RefreshRequest"/>

### RefreshRequest
DEPRECATED by peloton.api.v0.task.svc.RefreshTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| range | [InstanceRange](#peloton.api.v0.task.InstanceRange) |  |  |






<a name="peloton.api.v0.task.RefreshResponse"/>

### RefreshResponse
DEPRECATED by peloton.api.v0.task.svc.RefreshTasksResponse.






<a name="peloton.api.v0.task.ResourceConfig"/>

### ResourceConfig
Resource configuration for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cpuLimit | [double](#double) |  | CPU limit in number of CPU cores |
| memLimitMb | [double](#double) |  | Memory limit in MB |
| diskLimitMb | [double](#double) |  | Disk limit in MB |
| fdLimit | [uint32](#uint32) |  | File descriptor limit |
| gpuLimit | [double](#double) |  | GPU limit in number of GPUs |






<a name="peloton.api.v0.task.RestartPolicy"/>

### RestartPolicy
Restart policy for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxFailures | [uint32](#uint32) |  | Max number of task failures can occur before giving up scheduling retry, no backoff for now. Default 0 means no retry on failures. |






<a name="peloton.api.v0.task.RestartRequest"/>

### RestartRequest
DEPRECATED by peloton.api.v0.task.svc.RestartTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.v0.task.InstanceRange) | repeated |  |






<a name="peloton.api.v0.task.RestartResponse"/>

### RestartResponse
DEPRECATED by peloton.api.v0.task.svc.RestartTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange) |  |  |






<a name="peloton.api.v0.task.RuntimeInfo"/>

### RuntimeInfo
Runtime info of an task instance in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [TaskState](#peloton.api.v0.task.TaskState) |  | Runtime status of the task |
| mesosTaskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The mesos task ID for this instance |
| startTime | [string](#string) |  | The time when the instance starts to run. Will be unset if the instance hasn&#39;t started running yet. The time is represented in RFC3339 form with UTC timezone. |
| completionTime | [string](#string) |  | The time when the instance is completed. Will be unset if the instance hasn&#39;t completed yet. The time is represented in RFC3339 form with UTC timezone. |
| host | [string](#string) |  | The name of the host where the instance is running |
| ports | [RuntimeInfo.PortsEntry](#peloton.api.v0.task.RuntimeInfo.PortsEntry) | repeated | Dynamic ports reserved on the host while this instance is running |
| goalState | [TaskState](#peloton.api.v0.task.TaskState) |  | The desired state of the task which should be eventually reached by the system. |
| message | [string](#string) |  | The message that explains the current state of a task such as why the task is failed. Only track the latest one if the task has been retried and failed multiple times. |
| reason | [string](#string) |  | The reason that explains the current state of a task. Only track the latest one if the task has been retried and failed multiple times. See Mesos TaskStatus.Reason for more details. |
| failureCount | [uint32](#uint32) |  | The number of times the task has failed after retries. |
| volumeID | [.peloton.api.v0.peloton.VolumeID](#peloton.api.v0.task..peloton.api.v0.peloton.VolumeID) |  | persistent volume id |
| configVersion | [uint64](#uint64) |  | The config version currently used by the runtime. |
| desiredConfigVersion | [uint64](#uint64) |  | The desired config version that should be used by the runtime. |
| agentID | [.mesos.v1.AgentID](#peloton.api.v0.task..mesos.v1.AgentID) |  | the id of mesos agent on the host to be launched. |
| revision | [.peloton.api.v0.peloton.ChangeLog](#peloton.api.v0.task..peloton.api.v0.peloton.ChangeLog) |  | Revision of the current task info. |
| prevMesosTaskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The mesos task id of the previous task run of the instance. |
| resourceUsage | [RuntimeInfo.ResourceUsageEntry](#peloton.api.v0.task.RuntimeInfo.ResourceUsageEntry) | repeated | The resource usage for this task. The map key is each resource kind in string format and the map value is the number of unit-seconds of that resource used by the job. Example: if a task that uses 1 CPU and finishes in 10 seconds, this map will contain &lt;&#34;cpu&#34;:10&gt; |
| healthy | [HealthState](#peloton.api.v0.task.HealthState) |  | The result of the health check |
| desiredMesosTaskId | [.mesos.v1.TaskID](#peloton.api.v0.task..mesos.v1.TaskID) |  | The desired mesos task ID for this instance |
| terminationStatus | [TerminationStatus](#peloton.api.v0.task.TerminationStatus) |  | Termination status of the task. Set only if the task is in a non-successful terminal state such as KILLED or FAILED. |
| desiredHost | [string](#string) |  | The name of the host where the instance should be running on upon restart. It is used for best effort in-place update/restart. |






<a name="peloton.api.v0.task.RuntimeInfo.PortsEntry"/>

### RuntimeInfo.PortsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v0.task.RuntimeInfo.ResourceUsageEntry"/>

### RuntimeInfo.ResourceUsageEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [double](#double) |  |  |






<a name="peloton.api.v0.task.StartRequest"/>

### StartRequest
DEPRECATED by peloton.api.v0.task.svc.StartTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.v0.task.InstanceRange) | repeated |  |






<a name="peloton.api.v0.task.StartResponse"/>

### StartResponse
DEPRECATED by peloton.api.v0.task.svc.StartTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [StartResponse.Error](#peloton.api.v0.task.StartResponse.Error) |  |  |
| startedInstanceIds | [uint32](#uint32) | repeated |  |
| invalidInstanceIds | [uint32](#uint32) | repeated |  |






<a name="peloton.api.v0.task.StartResponse.Error"/>

### StartResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange) |  |  |
| failure | [TaskStartFailure](#peloton.api.v0.task.TaskStartFailure) |  |  |






<a name="peloton.api.v0.task.StopRequest"/>

### StopRequest
DEPRECATED by peloton.api.v0.task.svc.StopTasksRequest.
If no ranges specified, then stop all the tasks in the job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.v0.task.InstanceRange) | repeated |  |






<a name="peloton.api.v0.task.StopResponse"/>

### StopResponse
DEPRECATED by peloton.api.v0.task.svc.StopTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [StopResponse.Error](#peloton.api.v0.task.StopResponse.Error) |  |  |
| stoppedInstanceIds | [uint32](#uint32) | repeated |  |
| invalidInstanceIds | [uint32](#uint32) | repeated |  |






<a name="peloton.api.v0.task.StopResponse.Error"/>

### StopResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.task..peloton.api.v0.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.v0.task.InstanceIdOutOfRange) |  |  |
| updateError | [TaskUpdateError](#peloton.api.v0.task.TaskUpdateError) |  |  |






<a name="peloton.api.v0.task.TaskConfig"/>

### TaskConfig
Task configuration for a given job instance
Note that only add string/slice/ptr type into TaskConfig directly due to
the limitation of go reflection inside our task specific config logic.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the task |
| labels | [.peloton.api.v0.peloton.Label](#peloton.api.v0.task..peloton.api.v0.peloton.Label) | repeated | List of user-defined labels for the task |
| resource | [ResourceConfig](#peloton.api.v0.task.ResourceConfig) |  | Resource config of the task |
| container | [.mesos.v1.ContainerInfo](#peloton.api.v0.task..mesos.v1.ContainerInfo) |  | Container config of the task. |
| command | [.mesos.v1.CommandInfo](#peloton.api.v0.task..mesos.v1.CommandInfo) |  | Command line config of the task |
| executor | [.mesos.v1.ExecutorInfo](#peloton.api.v0.task..mesos.v1.ExecutorInfo) |  | Custom executor config of the task. |
| healthCheck | [HealthCheckConfig](#peloton.api.v0.task.HealthCheckConfig) |  | Health check config of the task |
| ports | [PortConfig](#peloton.api.v0.task.PortConfig) | repeated | List of network ports to be allocated for the task |
| constraint | [Constraint](#peloton.api.v0.task.Constraint) |  | Constraint on the attributes of the host or labels on tasks on the host that this task should run on. Use `AndConstraint`/`OrConstraint` to compose multiple constraints if necessary. |
| restartPolicy | [RestartPolicy](#peloton.api.v0.task.RestartPolicy) |  | Task restart policy on failures |
| volume | [PersistentVolumeConfig](#peloton.api.v0.task.PersistentVolumeConfig) |  | Persistent volume config of the task. |
| preemptionPolicy | [PreemptionPolicy](#peloton.api.v0.task.PreemptionPolicy) |  | Preemption policy of the task |
| controller | [bool](#bool) |  | Whether this is a controller task. A controller is a special batch task which controls other tasks inside a job. E.g. spark driver tasks in a spark job will be a controller task. |
| killGracePeriodSeconds | [uint32](#uint32) |  | This is used to set the amount of time between when the executor sends the SIGTERM message to gracefully terminate a task and when it kills it by sending SIGKILL. If you do not set the grace period duration the default is 30 seconds. |
| revocable | [bool](#bool) |  | Whether the instance is revocable. If so, it might be scheduled using revocable resources and subject to preemption when there is resource contention on the host. This can override the revocable configuration at the job level. |






<a name="peloton.api.v0.task.TaskEvent"/>

### TaskEvent
Task event of a Peloton task instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskId | [.peloton.api.v0.peloton.TaskID](#peloton.api.v0.task..peloton.api.v0.peloton.TaskID) |  | The task ID of the task event. |
| state | [TaskState](#peloton.api.v0.task.TaskState) |  | The task state of the task event. |
| message | [string](#string) |  | Short human friendly message explaining state. |
| timestamp | [string](#string) |  | The time when the event was created. The time is represented in RFC3339 form with UTC timezone. |
| source | [TaskEvent.Source](#peloton.api.v0.task.TaskEvent.Source) |  | The source that generated the task event. |
| hostname | [string](#string) |  | The host on which the task is running |
| reason | [string](#string) |  | The short reason for the task event |
| agentId | [string](#string) |  | The agentID for the task |
| prevTaskId | [.peloton.api.v0.peloton.TaskID](#peloton.api.v0.task..peloton.api.v0.peloton.TaskID) |  | The previous mesos task ID of the task event. |
| healthy | [HealthState](#peloton.api.v0.task.HealthState) |  | The health check result of the task |
| desiredTaskId | [.peloton.api.v0.peloton.TaskID](#peloton.api.v0.task..peloton.api.v0.peloton.TaskID) |  | The desired mesos task ID of the task event. |






<a name="peloton.api.v0.task.TaskEventsError"/>

### TaskEventsError
DEPRECATED by google.rpc.INTERNAL error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.TaskInfo"/>

### TaskInfo
Info of a task instance in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| instanceId | [uint32](#uint32) |  | The numerical ID assigned to this instance. Instance IDs must be unique and contiguous within a job. The ID is in the range of [0, N-1] for a job with instance count of N. |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task..peloton.api.v0.peloton.JobID) |  | Job ID of the task |
| config | [TaskConfig](#peloton.api.v0.task.TaskConfig) |  | Configuration of the task |
| runtime | [RuntimeInfo](#peloton.api.v0.task.RuntimeInfo) |  | Runtime info of the instance |






<a name="peloton.api.v0.task.TaskNotRunning"/>

### TaskNotRunning
Task not running error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.TaskStartFailure"/>

### TaskStartFailure
Error when TaskStart failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.TaskUpdateError"/>

### TaskUpdateError
DEPRECATED by google.rpc.INTERNAL error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.TerminationStatus"/>

### TerminationStatus
TerminationStatus contains details about termination of a task. It mainly
contains Peloton-specific reasons for termination.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reason | [TerminationStatus.Reason](#peloton.api.v0.task.TerminationStatus.Reason) |  | Reason for termination. |
| exit_code | [uint32](#uint32) |  | If non-zero, exit status when the container terminated. |
| signal | [string](#string) |  | Name of signal received by the container when it terminated. |





 


<a name="peloton.api.v0.task.Constraint.Type"/>

### Constraint.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN_CONSTRAINT | 0 | Reserved for compatibility. |
| LABEL_CONSTRAINT | 1 |  |
| AND_CONSTRAINT | 2 |  |
| OR_CONSTRAINT | 3 |  |



<a name="peloton.api.v0.task.HealthCheckConfig.Type"/>

### HealthCheckConfig.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new types. |
| COMMAND | 1 | Command line based health check |
| HTTP | 2 | HTTP endpoint based health check |
| GRPC | 3 | GRPC endpoint based health check |



<a name="peloton.api.v0.task.HealthState"/>

### HealthState
HealthState is the health check state of a task

| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 | Default value. |
| DISABLED | 1 | If the health check config is not enabled in the task config, the initial value of the health state is DISABLED. |
| HEALTH_UNKNOWN | 2 | If the health check config is enabled in the task config, the initial value of the health state is HEALTH_UNKNOWN. |
| HEALTHY | 3 | In a Mesos event, If the healthy field is true and the reason field is REASON_TASK_HEALTH_CHECK_STATUS_UPDATED the health state of the task is HEALTHY |
| UNHEALTHY | 4 | In a Mesos event, If the healthy field is false and the reason field is REASON_TASK_HEALTH_CHECK_STATUS_UPDATED the health state of the task is UNHEALTHY |



<a name="peloton.api.v0.task.LabelConstraint.Condition"/>

### LabelConstraint.Condition
Condition represents a constraint on the number of occurrences of the label.

| Name | Number | Description |
| ---- | ------ | ----------- |
| CONDITION_UNKNOWN | 0 | Reserved for compatibility. |
| CONDITION_LESS_THAN | 1 |  |
| CONDITION_EQUAL | 2 |  |
| CONDITION_GREATER_THAN | 3 |  |



<a name="peloton.api.v0.task.LabelConstraint.Kind"/>

### LabelConstraint.Kind
Kind represents whatever the constraint applies to the labels on the host
or to the labels of the tasks that are located on the host.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for compatibility. |
| TASK | 1 |  |
| HOST | 2 |  |



<a name="peloton.api.v0.task.PreemptionPolicy.Type"/>

### PreemptionPolicy.Type
Type represents whether the instance is preemptible. If so, it might
be scheduled using elastic resources from other resource pools and
subject to preemption when the demands of other resource pools increase.
If it is non-preemptible it will be scheduled using the reserved
resources only.
This can override the preemptible sla configuration at the job level.

| Name | Number | Description |
| ---- | ------ | ----------- |
| TYPE_INVALID | 0 |  |
| TYPE_PREEMPTIBLE | 1 |  |
| TYPE_NON_PREEMPTIBLE | 2 |  |



<a name="peloton.api.v0.task.TaskEvent.Source"/>

### TaskEvent.Source
Describes the source of the task event

| Name | Number | Description |
| ---- | ------ | ----------- |
| SOURCE_UNKNOWN | 0 |  |
| SOURCE_JOBMGR | 1 |  |
| SOURCE_RESMGR | 2 |  |
| SOURCE_HOSTMGR | 3 |  |



<a name="peloton.api.v0.task.TaskState"/>

### TaskState
Runtime states of a task instance

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new states. |
| INITIALIZED | 1 | The task is being initialized |
| PENDING | 2 | The task is pending and waiting for resources |
| READY | 3 | The task has been allocated with resources and ready for placement |
| PLACING | 4 | The task is being placed to a host based on its resource requirements and constraints |
| PLACED | 5 | The task has been assigned to a host matching the resource requirements and constraints |
| LAUNCHING | 6 | The task is taken from resmgr to be launched |
| LAUNCHED | 15 | The task is being launched in Job manager TODO: We need to correct the numbering |
| STARTING | 7 | The task is being started by Mesos agent |
| RUNNING | 8 | The task is running on a Mesos host |
| SUCCEEDED | 9 | The task terminated with an exit code of zero |
| FAILED | 10 | The task terminated with a non-zero exit code |
| LOST | 11 | The task is lost |
| PREEMPTING | 12 | The task is being preempted by another one on the node |
| KILLING | 13 | The task is being killed |
| KILLED | 14 | Execution of the task was terminated by the system |
| DELETED | 16 | The task is to be deleted after termination |



<a name="peloton.api.v0.task.TerminationStatus.Reason"/>

### TerminationStatus.Reason
Reason lists various causes for a task termination

| Name | Number | Description |
| ---- | ------ | ----------- |
| TERMINATION_STATUS_REASON_INVALID | 0 | Default value. |
| TERMINATION_STATUS_REASON_KILLED_ON_REQUEST | 1 | Task was killed because a stop request was received from a client. |
| TERMINATION_STATUS_REASON_FAILED | 2 | Task failed. See also TerminationStatus.exit_code, TerminationStatus.signal and ContainerStatus.message. |
| TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE | 3 | Task was killed to put the host in to maintenance. |
| TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES | 4 | Tasked was killed to reclaim resources allocated to it. |


 

 


<a name="peloton.api.v0.task.TaskManager"/>

### TaskManager
Task manager interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Get | [GetRequest](#peloton.api.v0.task.GetRequest) | [GetResponse](#peloton.api.v0.task.GetRequest) | Get the info of a task in job. |
| List | [ListRequest](#peloton.api.v0.task.ListRequest) | [ListResponse](#peloton.api.v0.task.ListRequest) | List all task info in a job. |
| Start | [StartRequest](#peloton.api.v0.task.StartRequest) | [StartResponse](#peloton.api.v0.task.StartRequest) | Start a set of tasks for a job. Will be no-op for tasks that are currently running. |
| Stop | [StopRequest](#peloton.api.v0.task.StopRequest) | [StopResponse](#peloton.api.v0.task.StopRequest) | Stop a set of tasks for a job. Will be no-op for tasks that are currently stopped. |
| Restart | [RestartRequest](#peloton.api.v0.task.RestartRequest) | [RestartResponse](#peloton.api.v0.task.RestartRequest) | Restart a set of tasks for a job. Will start tasks that are currently stopped. |
| Query | [QueryRequest](#peloton.api.v0.task.QueryRequest) | [QueryResponse](#peloton.api.v0.task.QueryRequest) | Query task info in a job, using a set of filters. |
| BrowseSandbox | [BrowseSandboxRequest](#peloton.api.v0.task.BrowseSandboxRequest) | [BrowseSandboxResponse](#peloton.api.v0.task.BrowseSandboxRequest) | BrowseSandbox returns list of file paths inside sandbox. |
| Refresh | [RefreshRequest](#peloton.api.v0.task.RefreshRequest) | [RefreshResponse](#peloton.api.v0.task.RefreshRequest) | Debug only method. Allows user to load task runtime state from DB and re-execute the action associated with current state. |
| GetCache | [GetCacheRequest](#peloton.api.v0.task.GetCacheRequest) | [GetCacheResponse](#peloton.api.v0.task.GetCacheRequest) | Debug only method. Get the cache of a task stored in Peloton. |
| GetPodEvents | [GetPodEventsRequest](#peloton.api.v0.task.GetPodEventsRequest) | [GetPodEventsResponse](#peloton.api.v0.task.GetPodEventsRequest) | GetPodEvents returns pod events (state transition for a pod), in reverse chronological order. pod is singular instance of a Peloton job. |
| DeletePodEvents | [DeletePodEventsRequest](#peloton.api.v0.task.DeletePodEventsRequest) | [DeletePodEventsResponse](#peloton.api.v0.task.DeletePodEventsRequest) | DeletePodEvents, deletes the pod events for provided request, which is for a jobID &#43; instanceID &#43; less than equal to runID. Response will be successful or error on unable to delete events for input. |

 



<a name="update.proto"/>
<p align="right"><a href="#top">Top</a></p>

## update.proto



<a name="peloton.api.v0.update.UpdateConfig"/>

### UpdateConfig
Update options for a job update


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | Update batch size of the deployment |
| batchPercentage | [double](#double) |  | Update batch percentage of the deployment. If present, will take precedence over batchSize |
| stopBeforeUpdate | [bool](#bool) |  | Whether or not to stop all instance before update |
| startPaused | [bool](#bool) |  | startPaused indicates if the update should start in the paused state, requiring an explicit resume to initiate. |
| rollbackOnFailure | [bool](#bool) |  | rollbackOnFailure indicates if the update should be rolled back automatically if failure is detected |
| maxInstanceAttempts | [uint32](#uint32) |  | maxInstanceAttempts is the maximum attempt times for one task. If the value is 0, the instance can be retried for infinite times. |
| maxFailureInstances | [uint32](#uint32) |  | maxFailureInstances is the number of failed instances in one update that allowed. If the value is 0, there is no limit for max failure instances and the update is marked successful even if all of the instances fail. |
| inPlace | [bool](#bool) |  | If set to true, peloton would try to place the task restarted/updated on the host it previously run on. It is best effort, and has no guarantee of success. |






<a name="peloton.api.v0.update.UpdateInfo"/>

### UpdateInfo
Information of an update, such as update config and runtime status


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update..peloton.api.v0.peloton.UpdateID) |  | Update ID of the job update |
| config | [UpdateConfig](#peloton.api.v0.update.UpdateConfig) |  | Update configuration |
| status | [UpdateStatus](#peloton.api.v0.update.UpdateStatus) |  | Update runtime status |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.update..peloton.api.v0.peloton.JobID) |  | Job ID of the job update |
| configVersion | [uint64](#uint64) |  | Configuration version of the job after this update |
| prevConfigVersion | [uint64](#uint64) |  | Job configuration version before the update |
| opaque_data | [.peloton.api.v0.peloton.OpaqueData](#peloton.api.v0.update..peloton.api.v0.peloton.OpaqueData) |  | Opaque metadata provided by the user |






<a name="peloton.api.v0.update.UpdateStatus"/>

### UpdateStatus
UpdateStatus provides current runtime status of an update


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| numTasksDone | [uint32](#uint32) |  | Number of tasks that have been updated |
| numTasksRemaining | [uint32](#uint32) |  | Number of tasks to be updated |
| state | [State](#peloton.api.v0.update.State) |  | Runtime state of the update |
| numTasksFailed | [uint32](#uint32) |  | Number of tasks that failed during the update |





 


<a name="peloton.api.v0.update.State"/>

### State
Runtime state of a job update

| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 | Invalid protobuf value |
| INITIALIZED | 1 | The update has been created but not started yet |
| ROLLING_FORWARD | 2 | The update is rolling forward |
| PAUSED | 3 | The update has been paused |
| SUCCEEDED | 4 | The update has completed successfully |
| ABORTED | 5 | The update was aborted/cancelled |
| FAILED | 6 | The update is failed |
| ROLLING_BACKWARD | 7 | The update is rolling barckward |
| ROLLED_BACK | 8 | The update was rolled back due to failure |


 

 

 



<a name="peloton.proto"/>
<p align="right"><a href="#top">Top</a></p>

## peloton.proto



<a name="peloton.api.v1alpha.peloton.EntityVersion"/>

### EntityVersion
An opaque token associated with an entity object used to implement
optimistic concurrency control.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.HostOfferID"/>

### HostOfferID
A unique ID assigned to offers from a host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.JobID"/>

### JobID
A unique ID assigned to a Job. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.Label"/>

### Label
Key, value pair used to store free form user-data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.OpaqueData"/>

### OpaqueData
Opaque data passed to Peloton from the client.
Passing an empty string in the structure will unset the existing data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.PodID"/>

### PodID
A unique ID assigned to a pod. It should be treated as an opaque token.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.PodName"/>

### PodName
A unique name assigned to a pod. By default, the pod name is in the format of JobID-&lt;InstanceID&gt;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.ResourcePoolID"/>

### ResourcePoolID
A unique ID assigned to a Resource Pool. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.Revision"/>

### Revision
Revision of an entity info, such as JobSpec etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  | Version number of the entity info which is monotonically increasing. Clients can use this to guide against race conditions using MVCC. |
| created_at | [uint64](#uint64) |  | The timestamp when the entity info is created |
| updated_at | [uint64](#uint64) |  | The timestamp when the entity info is updated |
| updated_by | [string](#string) |  | The user or service that updated the entity info |






<a name="peloton.api.v1alpha.peloton.Secret"/>

### Secret
Secret is used to store secrets per job and contains
ID, absolute container mount path and base64 encoded secret data


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| secret_id | [SecretID](#peloton.api.v1alpha.peloton.SecretID) |  | UUID of the secret |
| path | [string](#string) |  | Path at which the secret file will be mounted in the container |
| value | [Secret.Value](#peloton.api.v1alpha.peloton.Secret.Value) |  | Secret value |






<a name="peloton.api.v1alpha.peloton.Secret.Value"/>

### Secret.Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Secret data as byte array |






<a name="peloton.api.v1alpha.peloton.SecretID"/>

### SecretID
A unique ID assigned to a Secret. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.peloton.TimeRange"/>

### TimeRange
Time range specified by min and max timestamps.
Time range is left closed and right open: [min, max)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| min | [.google.protobuf.Timestamp](#peloton.api.v1alpha.peloton..google.protobuf.Timestamp) |  |  |
| max | [.google.protobuf.Timestamp](#peloton.api.v1alpha.peloton..google.protobuf.Timestamp) |  |  |






<a name="peloton.api.v1alpha.peloton.VolumeID"/>

### VolumeID
A unique ID assigned to a Volume. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 

 

 

 



<a name="host.proto"/>
<p align="right"><a href="#top">Top</a></p>

## host.proto



<a name="peloton.api.v0.host.HostInfo"/>

### HostInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  | The hostname of the host |
| ip | [string](#string) |  | The IP address of the host |
| state | [HostState](#peloton.api.v0.host.HostState) |  | The current state of the host |





 


<a name="peloton.api.v0.host.HostState"/>

### HostState


| Name | Number | Description |
| ---- | ------ | ----------- |
| HOST_STATE_INVALID | 0 |  |
| HOST_STATE_UNKNOWN | 1 | Reserved for future compatibility of new states. |
| HOST_STATE_UP | 2 | The host is healthy |
| HOST_STATE_DRAINING | 3 | The tasks running on the host are being rescheduled. There will be no further placement of tasks on the host |
| HOST_STATE_DRAINED | 4 | There are no tasks running on the host and is ready to be put into maintenance. |
| HOST_STATE_DOWN | 5 | The host is in maintenance. |


 

 

 



<a name="changelog.proto"/>
<p align="right"><a href="#top">Top</a></p>

## changelog.proto



<a name="peloton.api.v0.changelog.ChangeLog"/>

### ChangeLog
Change log of the entity info


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [int64](#int64) |  | Version number of the entity info which is monotonically increasing. Clients can use this to guide against race conditions using MVCC. |
| createdAt | [int64](#int64) |  | The timestamp when the entity info is created |
| updatedAt | [int64](#int64) |  | The timestamp when the entity info is updated |
| updatedBy | [string](#string) |  | The entity of the user that updated the entity info |





 

 

 

 



<a name="respool.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool.proto



<a name="peloton.api.v0.respool.ControllerLimit"/>

### ControllerLimit
The max limit of resources `CONTROLLER`(see TaskType) tasks can use in
this resource pool. This is defined as a percentage of the resource pool&#39;s
reservation. If undefined there is no maximum limit for controller tasks
i.e. controller tasks will not be treated differently. For eg if the
resource pool&#39;s reservation is defined as:

cpu:100
mem:1000
disk:1000
gpu:10

And the ControllerLimit = 10 ,Then the maximum resources the controller
tasks can use is 10% of the reservation, i.e.

cpu:10
mem:100
disk:100
gpu:1


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxPercent | [double](#double) |  |  |






<a name="peloton.api.v0.respool.CreateRequest"/>

### CreateRequest
DEPRECATED by peloton.api.v0.respool.svc.CreateResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig) |  |  |






<a name="peloton.api.v0.respool.CreateResponse"/>

### CreateResponse
DEPRECATED by peloton.api.v0.respool.svc.CreateResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [CreateResponse.Error](#peloton.api.v0.respool.CreateResponse.Error) |  |  |
| result | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |






<a name="peloton.api.v0.respool.CreateResponse.Error"/>

### CreateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| alreadyExists | [ResourcePoolAlreadyExists](#peloton.api.v0.respool.ResourcePoolAlreadyExists) |  |  |
| invalidResourcePoolConfig | [InvalidResourcePoolConfig](#peloton.api.v0.respool.InvalidResourcePoolConfig) |  |  |






<a name="peloton.api.v0.respool.DeleteRequest"/>

### DeleteRequest
DEPRECATED by peloton.api.v0.respool.svc.DeleteResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  |  |






<a name="peloton.api.v0.respool.DeleteResponse"/>

### DeleteResponse
DEPRECATED by peloton.api.v0.respool.svc.DeleteResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [DeleteResponse.Error](#peloton.api.v0.respool.DeleteResponse.Error) |  |  |






<a name="peloton.api.v0.respool.DeleteResponse.Error"/>

### DeleteResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolPathNotFound](#peloton.api.v0.respool.ResourcePoolPathNotFound) |  |  |
| isBusy | [ResourcePoolIsBusy](#peloton.api.v0.respool.ResourcePoolIsBusy) |  |  |
| isNotLeaf | [ResourcePoolIsNotLeaf](#peloton.api.v0.respool.ResourcePoolIsNotLeaf) |  |  |
| notDeleted | [ResourcePoolNotDeleted](#peloton.api.v0.respool.ResourcePoolNotDeleted) |  |  |






<a name="peloton.api.v0.respool.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.v0.respool.svc.GetResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The ID of the resource pool to get |
| includeChildPools | [bool](#bool) |  | Whether or not to include the resource pool info of the direct children |






<a name="peloton.api.v0.respool.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.v0.respool.svc.GetResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetResponse.Error](#peloton.api.v0.respool.GetResponse.Error) |  |  |
| poolinfo | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) |  |  |
| childPools | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.v0.respool.GetResponse.Error"/>

### GetResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolNotFound](#peloton.api.v0.respool.ResourcePoolNotFound) |  |  |






<a name="peloton.api.v0.respool.InvalidResourcePoolConfig"/>

### InvalidResourcePoolConfig
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.InvalidResourcePoolPath"/>

### InvalidResourcePoolPath
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.LookupRequest"/>

### LookupRequest
DEPRECATED by peloton.api.v0.respool.svc.LookupResourcePoolIDRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  |  |






<a name="peloton.api.v0.respool.LookupResponse"/>

### LookupResponse
DEPRECATED by peloton.api.v0.respool.svc.LookupResourcePoolIDResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [LookupResponse.Error](#peloton.api.v0.respool.LookupResponse.Error) |  |  |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |






<a name="peloton.api.v0.respool.LookupResponse.Error"/>

### LookupResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolPathNotFound](#peloton.api.v0.respool.ResourcePoolPathNotFound) |  |  |
| invalidPath | [InvalidResourcePoolPath](#peloton.api.v0.respool.InvalidResourcePoolPath) |  |  |






<a name="peloton.api.v0.respool.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.v0.respool.svc.QueryResourcePoolRequest


TODO Filters






<a name="peloton.api.v0.respool.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.v0.respool.svc.QueryResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.v0.respool.QueryResponse.Error) |  |  |
| resourcePools | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.v0.respool.QueryResponse.Error"/>

### QueryResponse.Error
TODO add error types






<a name="peloton.api.v0.respool.ResourceConfig"/>

### ResourceConfig
Resource configuration for a resource


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| reservation | [double](#double) |  | Reservation/min of the resource |
| limit | [double](#double) |  | Limit of the resource |
| share | [double](#double) |  | Share on the resource pool |
| type | [ReservationType](#peloton.api.v0.respool.ReservationType) |  | ReservationType indicates the the type of reservation There are two kind of reservation 1. ELASTIC 2. STATIC |






<a name="peloton.api.v0.respool.ResourcePoolAlreadyExists"/>

### ResourcePoolAlreadyExists
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolConfig"/>

### ResourcePoolConfig
Resource Pool configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| changeLog | [.peloton.api.v0.changelog.ChangeLog](#peloton.api.v0.respool..peloton.api.v0.changelog.ChangeLog) |  | Change log entry of the Resource Pool config TODO use peloton.Changelog |
| name | [string](#string) |  | Name of the resource pool |
| owningTeam | [string](#string) |  | Owning team of the pool |
| ldapGroups | [string](#string) | repeated | LDAP groups of the pool |
| description | [string](#string) |  | Description of the resource pool |
| resources | [ResourceConfig](#peloton.api.v0.respool.ResourceConfig) | repeated | Resource config of the Resource Pool |
| parent | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent |
| policy | [SchedulingPolicy](#peloton.api.v0.respool.SchedulingPolicy) |  | Task Scheduling policy |
| controllerLimit | [ControllerLimit](#peloton.api.v0.respool.ControllerLimit) |  | The controller limit for this resource pool |
| slackLimit | [SlackLimit](#peloton.api.v0.respool.SlackLimit) |  | Cap on max non-slack resources[mem,disk] in percentage that can be used by revocable task. |






<a name="peloton.api.v0.respool.ResourcePoolInfo"/>

### ResourcePoolInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | Resource Pool Id |
| config | [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig) |  | ResourcePool config |
| parent | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent TODO: parent duplicated from ResourcePoolConfig |
| children | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) | repeated | Resource Pool&#39;s children |
| usage | [ResourceUsage](#peloton.api.v0.respool.ResourceUsage) | repeated | Resource usage for each resource kind |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  | Resource Pool Path |






<a name="peloton.api.v0.respool.ResourcePoolIsBusy"/>

### ResourcePoolIsBusy
DEPRECATED by google.rpc.FAILED_PRECONDITION error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolIsNotLeaf"/>

### ResourcePoolIsNotLeaf
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolNotDeleted"/>

### ResourcePoolNotDeleted
DEPRECATED by google.rpc.INTERNAL error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolNotFound"/>

### ResourcePoolNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolPath"/>

### ResourcePoolPath
A fully qualified path to a resource pool in a resource pool hierrarchy.
The path to a resource pool can be defined as an absolute path,
starting from the root node and separated by a slash.

The resource hierarchy is anchored at a node called the root,
designated by a slash &#34;/&#34;.

For the below resource hierarchy ; the &#34;compute&#34; resource pool would be
desgignated by path: /infrastructure/compute
root
├─ infrastructure
│  └─ compute
└─ marketplace


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourcePoolPathNotFound"/>

### ResourcePoolPathNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.respool.ResourceUsage"/>

### ResourceUsage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| allocation | [double](#double) |  | Allocation of the resource |
| slack | [double](#double) |  | slack is the resource which is allocated but not used and mesos will give those resources as revocable offers |






<a name="peloton.api.v0.respool.SlackLimit"/>

### SlackLimit
The max limit of resources `REVOCABLE`(see TaskType) tasks can use in
this resource pool. This is defined as a percentage of the resource pool&#39;s
reservation. If undefined there is no maximum limit for revocable tasks
i.e. revocable tasks will not be treated differently. For eg if the
resource pool&#39;s reservation is defined as:

cpu:100
mem:1000
disk:1000

And the SlackLimit = 10 ,Then the maximum resources the revocable
tasks can use is 10% of the reservation, i.e.

mem:100
disk:100

For cpu, it will use revocable resources.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxPercent | [double](#double) |  |  |






<a name="peloton.api.v0.respool.UpdateRequest"/>

### UpdateRequest
DEPRECATED by peloton.api.v0.respool.svc.UpdateResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| config | [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig) |  |  |






<a name="peloton.api.v0.respool.UpdateResponse"/>

### UpdateResponse
DEPRECATED by peloton.api.v0.respool.svc.UpdateResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [UpdateResponse.Error](#peloton.api.v0.respool.UpdateResponse.Error) |  |  |






<a name="peloton.api.v0.respool.UpdateResponse.Error"/>

### UpdateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolNotFound](#peloton.api.v0.respool.ResourcePoolNotFound) |  |  |
| invalidResourcePoolConfig | [InvalidResourcePoolConfig](#peloton.api.v0.respool.InvalidResourcePoolConfig) |  |  |





 


<a name="peloton.api.v0.respool.ReservationType"/>

### ReservationType
ReservationType indicates reservation type for the resourcepool

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN_TYPE | 0 |  |
| ELASTIC | 1 | ELASTIC reservation enables resource pool to be elastic in reservation , which means other resource pool can take resources from this resource pool as well as this resource pool also can take resources from any other resource pool. This is the by default behavior for the resource pool |
| STATIC | 2 | STATIC reservation enables resource pool to be static in reservation , which means irrespective of the demand this resource pool will have atleast reservation as entitlement value. No other resource pool can take resources from this resource pool. If demand for this resource pool is high it can take resources from other resource pools. By default value for reservation type ELASTIC. |



<a name="peloton.api.v0.respool.SchedulingPolicy"/>

### SchedulingPolicy
Scheduling policy for Resource Pool.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| PriorityFIFO | 1 | This scheduling policy will return item for highest priority in FIFO order |


 

 


<a name="peloton.api.v0.respool.ResourceManager"/>

### ResourceManager
DEPRECATED by peloton.api.v0.respool.svc.ResourcePoolService
Resource Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateResourcePool | [CreateRequest](#peloton.api.v0.respool.CreateRequest) | [CreateResponse](#peloton.api.v0.respool.CreateRequest) | Create a resource pool entity for a given config |
| GetResourcePool | [GetRequest](#peloton.api.v0.respool.GetRequest) | [GetResponse](#peloton.api.v0.respool.GetRequest) | Get the resource pool entity |
| DeleteResourcePool | [DeleteRequest](#peloton.api.v0.respool.DeleteRequest) | [DeleteResponse](#peloton.api.v0.respool.DeleteRequest) | Delete a resource pool entity |
| UpdateResourcePool | [UpdateRequest](#peloton.api.v0.respool.UpdateRequest) | [UpdateResponse](#peloton.api.v0.respool.UpdateRequest) | modify a resource pool entity |
| LookupResourcePoolID | [LookupRequest](#peloton.api.v0.respool.LookupRequest) | [LookupResponse](#peloton.api.v0.respool.LookupRequest) | Lookup the resource pool ID for a given resource pool path |
| Query | [QueryRequest](#peloton.api.v0.respool.QueryRequest) | [QueryResponse](#peloton.api.v0.respool.QueryRequest) | Query the resource pool. |

 



<a name="job.proto"/>
<p align="right"><a href="#top">Top</a></p>

## job.proto



<a name="peloton.api.v0.job.CreateRequest"/>

### CreateRequest
DEPRECATED by peloton.api.v0.job.svc.CreateJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| config | [JobConfig](#peloton.api.v0.job.JobConfig) |  |  |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job |






<a name="peloton.api.v0.job.CreateResponse"/>

### CreateResponse
DEPRECATED by peloton.api.v0.job.svc.CreateJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [CreateResponse.Error](#peloton.api.v0.job.CreateResponse.Error) |  |  |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |






<a name="peloton.api.v0.job.CreateResponse.Error"/>

### CreateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| alreadyExists | [JobAlreadyExists](#peloton.api.v0.job.JobAlreadyExists) |  |  |
| invalidConfig | [InvalidJobConfig](#peloton.api.v0.job.InvalidJobConfig) |  |  |
| invalidJobId | [InvalidJobId](#peloton.api.v0.job.InvalidJobId) |  |  |






<a name="peloton.api.v0.job.DeleteRequest"/>

### DeleteRequest
DEPRECATED by peloton.api.v0.job.svc.DeleteRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |






<a name="peloton.api.v0.job.DeleteResponse"/>

### DeleteResponse
DEPRECATED by peloton.api.v0.job.svc.DeleteResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [DeleteResponse.Error](#peloton.api.v0.job.DeleteResponse.Error) |  |  |






<a name="peloton.api.v0.job.DeleteResponse.Error"/>

### DeleteResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.job..peloton.api.v0.errors.JobNotFound) |  |  |






<a name="peloton.api.v0.job.GetActiveJobsRequest"/>

### GetActiveJobsRequest
DEPRECATED by peloton.api.job.svc.GetActiveJobsRequest






<a name="peloton.api.v0.job.GetActiveJobsResponse"/>

### GetActiveJobsResponse
DEPRECATED by peloton.api.job.svc.GetActiveJobsResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ids | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) | repeated | List of Active Job IDs |






<a name="peloton.api.v0.job.GetCacheRequest"/>

### GetCacheRequest
DEPRECATED by peloton.api.job.svc.GetJobCacheRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v0.job.GetCacheResponse"/>

### GetCacheResponse
DEPRECATED by peloton.api.job.svc.GetJobCacheResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [JobConfig](#peloton.api.v0.job.JobConfig) |  | The job configuration in cache of the matching job. |
| runtime | [RuntimeInfo](#peloton.api.v0.job.RuntimeInfo) |  | The job runtime in cache of the matching job. |






<a name="peloton.api.v0.job.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.v0.job.svc.GetJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |






<a name="peloton.api.v0.job.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.v0.job.svc.GetJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetResponse.Error](#peloton.api.v0.job.GetResponse.Error) |  |  |
| jobInfo | [JobInfo](#peloton.api.v0.job.JobInfo) |  |  |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job, secret.Value will be empty. SecretID and path will be populated, so that caller can identify which secret is associated with this job. |






<a name="peloton.api.v0.job.GetResponse.Error"/>

### GetResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.v0.errors.JobNotFound](#peloton.api.v0.job..peloton.api.v0.errors.JobNotFound) |  |  |
| getRuntimeFail | [.peloton.api.v0.errors.JobGetRuntimeFail](#peloton.api.v0.job..peloton.api.v0.errors.JobGetRuntimeFail) |  |  |






<a name="peloton.api.v0.job.InvalidJobConfig"/>

### InvalidJobConfig
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.job.InvalidJobId"/>

### InvalidJobId
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.job.JobAlreadyExists"/>

### JobAlreadyExists
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.job.JobConfig"/>

### JobConfig
Job configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| changeLog | [.peloton.api.v0.peloton.ChangeLog](#peloton.api.v0.job..peloton.api.v0.peloton.ChangeLog) |  | Change log entry of the job config |
| name | [string](#string) |  | Name of the job |
| type | [JobType](#peloton.api.v0.job.JobType) |  | Type of the job |
| owningTeam | [string](#string) |  | Owning team of the job |
| ldapGroups | [string](#string) | repeated | LDAP groups of the job |
| description | [string](#string) |  | Description of the job |
| labels | [.peloton.api.v0.peloton.Label](#peloton.api.v0.job..peloton.api.v0.peloton.Label) | repeated | List of user-defined labels for the job |
| instanceCount | [uint32](#uint32) |  | Number of instances of the job |
| sla | [SlaConfig](#peloton.api.v0.job.SlaConfig) |  | SLA config of the job |
| defaultConfig | [.peloton.api.v0.task.TaskConfig](#peloton.api.v0.job..peloton.api.v0.task.TaskConfig) |  | Default task configuration of the job |
| instanceConfig | [JobConfig.InstanceConfigEntry](#peloton.api.v0.job.JobConfig.InstanceConfigEntry) | repeated | Instance specific task config which overwrites the default one |
| respoolID | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.job..peloton.api.v0.peloton.ResourcePoolID) |  | Resource Pool ID where this job belongs to |
| owner | [string](#string) |  | Owner of the job |






<a name="peloton.api.v0.job.JobConfig.InstanceConfigEntry"/>

### JobConfig.InstanceConfigEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [.peloton.api.v0.task.TaskConfig](#peloton.api.v0.job..peloton.api.v0.task.TaskConfig) |  |  |






<a name="peloton.api.v0.job.JobInfo"/>

### JobInfo
Information of a job, such as job config and runtime


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | Job ID |
| config | [JobConfig](#peloton.api.v0.job.JobConfig) |  | Job configuration |
| runtime | [RuntimeInfo](#peloton.api.v0.job.RuntimeInfo) |  | Job runtime information |






<a name="peloton.api.v0.job.JobNotFound"/>

### JobNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.job.JobSummary"/>

### JobSummary
Summary of a job configuration and runtime.
Will be returned as part of Job Query API response.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | Job ID |
| name | [string](#string) |  | Name of the job |
| type | [JobType](#peloton.api.v0.job.JobType) |  | Type of the job |
| owner | [string](#string) |  | Owner of the job |
| owningTeam | [string](#string) |  | Owning team of the job |
| labels | [.peloton.api.v0.peloton.Label](#peloton.api.v0.job..peloton.api.v0.peloton.Label) | repeated | List of user-defined labels for the job |
| instanceCount | [uint32](#uint32) |  | Number of instances of the job |
| respoolID | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.job..peloton.api.v0.peloton.ResourcePoolID) |  | Resource Pool ID where this job belongs to |
| runtime | [RuntimeInfo](#peloton.api.v0.job.RuntimeInfo) |  | Job runtime information |






<a name="peloton.api.v0.job.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.v0.job.svc.QueryJobsRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respoolID | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.job..peloton.api.v0.peloton.ResourcePoolID) |  |  |
| spec | [QuerySpec](#peloton.api.v0.job.QuerySpec) |  | Per instance configuration in the job configuration cannot be queried. |
| summaryOnly | [bool](#bool) |  | Only return the job summary in the query response If set, JobInfo in QueryResponse would be set to nil and only JobSummary in QueryResponse would be populated. |






<a name="peloton.api.v0.job.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.v0.job.svc.QueryJobsResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.v0.job.QueryResponse.Error) |  |  |
| records | [JobInfo](#peloton.api.v0.job.JobInfo) | repeated | DEPRECATED by peloton.api.v0.job.JobSummary JobInfo is huge and there is no need to return it in QueryResponse. NOTE: instanceConfig is not returned as part of jobConfig inside jobInfo as it can be so huge that exceeds grpc limit size. Use Job.Get() API to get instanceConfig for job. |
| pagination | [.peloton.api.v0.query.Pagination](#peloton.api.v0.job..peloton.api.v0.query.Pagination) |  |  |
| spec | [QuerySpec](#peloton.api.v0.job.QuerySpec) |  | The query spec from the request |
| results | [JobSummary](#peloton.api.v0.job.JobSummary) | repeated | Job summary |






<a name="peloton.api.v0.job.QueryResponse.Error"/>

### QueryResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| err | [.peloton.api.v0.errors.UnknownError](#peloton.api.v0.job..peloton.api.v0.errors.UnknownError) |  |  |
| invalidRespool | [.peloton.api.v0.errors.InvalidRespool](#peloton.api.v0.job..peloton.api.v0.errors.InvalidRespool) |  |  |






<a name="peloton.api.v0.job.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for jobs. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.v0.query.PaginationSpec](#peloton.api.v0.job..peloton.api.v0.query.PaginationSpec) |  | DEPRECATED: use QueryJobRequest.pagination instead. The spec of how to do pagination for the query results. |
| labels | [.peloton.api.v0.peloton.Label](#peloton.api.v0.job..peloton.api.v0.peloton.Label) | repeated | List of labels to query the jobs. Will match all jobs if the list is empty. |
| keywords | [string](#string) | repeated | List of keywords to query the jobs. Will match all jobs if the list is empty. When set, will do a wildcard match on owner, name, labels, description. |
| jobStates | [JobState](#peloton.api.v0.job.JobState) | repeated | List of job states to query the jobs. Will match all jobs if the list is empty. |
| respool | [.peloton.api.v0.respool.ResourcePoolPath](#peloton.api.v0.job..peloton.api.v0.respool.ResourcePoolPath) |  | The resource pool to query the jobs. Will match jobs from all resource pools if unset. |
| owner | [string](#string) |  | Query jobs by owner. This is case sensitive and will look for jobs with owner matching the exact owner string. Will match all jobs if owner is unset. |
| name | [string](#string) |  | Query jobs by name. This is case sensitive and will look for jobs with name matching the name string. Will support partial name match. Will match all jobs if name is unset. |
| creationTimeRange | [.peloton.api.v0.peloton.TimeRange](#peloton.api.v0.job..peloton.api.v0.peloton.TimeRange) |  | Query jobs by creation time range. This will look for all jobs that were created within a specified time range. This search will operate based on job creation time. |
| completionTimeRange | [.peloton.api.v0.peloton.TimeRange](#peloton.api.v0.job..peloton.api.v0.peloton.TimeRange) |  | Query jobs by completion time range. This will look for all jobs that were completed within a specified time range. This search will operate based on job completion time. |






<a name="peloton.api.v0.job.RefreshRequest"/>

### RefreshRequest
DEPRECATED by peloton.api.v0.job.svc.RefreshJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |






<a name="peloton.api.v0.job.RefreshResponse"/>

### RefreshResponse
DEPRECATED by peloton.api.v0.job.svc.RefreshJobResponse






<a name="peloton.api.v0.job.RestartConfig"/>

### RestartConfig
DEPRECATED by peloton.api.job.svc.RestartConfig
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling restart, if unset all tasks specified will be restarted at the same time. |






<a name="peloton.api.v0.job.RestartRequest"/>

### RestartRequest
DEPRECATED by peloton.api.job.svc.RestartJobRequest
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | The job to restart |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job..peloton.api.v0.task.InstanceRange) | repeated | The instances to restart, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| restartConfig | [RestartConfig](#peloton.api.v0.job.RestartConfig) |  | The config for restarting a job |






<a name="peloton.api.v0.job.RestartResponse"/>

### RestartResponse
DEPRECATED by peloton.api.job.svc.RestartJobResponse
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the restart |






<a name="peloton.api.v0.job.RuntimeInfo"/>

### RuntimeInfo
Job RuntimeInfo provides the current runtime status of a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [JobState](#peloton.api.v0.job.JobState) |  | State of the job |
| creationTime | [string](#string) |  | The time when the job was created. The time is represented in RFC3339 form with UTC timezone. |
| startTime | [string](#string) |  | The time when the first task of the job starts to run. The time is represented in RFC3339 form with UTC timezone. |
| completionTime | [string](#string) |  | The time when the last task of the job is completed. The time is represented in RFC3339 form with UTC timezone. |
| taskStats | [RuntimeInfo.TaskStatsEntry](#peloton.api.v0.job.RuntimeInfo.TaskStatsEntry) | repeated | The number of tasks grouped by each task state. The map key is the task.TaskState in string format and the map value is the number of tasks in the particular state. |
| configVersion | [int64](#int64) |  | This field is being deprecated and should not be used. |
| goalState | [JobState](#peloton.api.v0.job.JobState) |  | Goal state of the job. |
| configurationVersion | [uint64](#uint64) |  | The version reflects the version of the JobConfig, that is currently used by the job. |
| revision | [.peloton.api.v0.peloton.ChangeLog](#peloton.api.v0.job..peloton.api.v0.peloton.ChangeLog) |  | Changelog entry to version the job runtime. |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job..peloton.api.v0.peloton.UpdateID) |  | The identifier of the current job update. If the field is set to nil, it implies that the job has no current running job update. |
| resourceUsage | [RuntimeInfo.ResourceUsageEntry](#peloton.api.v0.job.RuntimeInfo.ResourceUsageEntry) | repeated | The resource usage for this job. The map key is each resource kind in string format and the map value is the number of unit-seconds of that resource used by the job. Example: if a job has one task that uses 1 CPU and finishes in 10 seconds, this map will contain &lt;&#34;cpu&#34;:10&gt; |
| workflowVersion | [uint64](#uint64) |  | The version reflects the version of the workflow, it is an opaque value which is used internally only |
| stateVersion | [uint64](#uint64) |  | The state version currently used by the runtime. |
| desiredStateVersion | [uint64](#uint64) |  | The desired state version that should be used by the runtime. |
| taskConfigVersionStats | [RuntimeInfo.TaskConfigVersionStatsEntry](#peloton.api.v0.job.RuntimeInfo.TaskConfigVersionStatsEntry) | repeated | The number of tasks grouped by which configuration version they are on. The map key is the job configuration version and the map value is the number of tasks using that particular job configuration version. |






<a name="peloton.api.v0.job.RuntimeInfo.ResourceUsageEntry"/>

### RuntimeInfo.ResourceUsageEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [double](#double) |  |  |






<a name="peloton.api.v0.job.RuntimeInfo.TaskConfigVersionStatsEntry"/>

### RuntimeInfo.TaskConfigVersionStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint64](#uint64) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v0.job.RuntimeInfo.TaskStatsEntry"/>

### RuntimeInfo.TaskStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v0.job.SlaConfig"/>

### SlaConfig
SLA configuration for a job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| priority | [uint32](#uint32) |  | Priority of a job. Higher value takes priority over lower value when making scheduling decisions as well as preemption decisions |
| preemptible | [bool](#bool) |  | Whether the job instances are preemptible. If so, it might be scheduled using revocable offers |
| revocable | [bool](#bool) |  | Whether the job instances are revocable. If so, it might be scheduled using revocable resources and subject to preemption when other jobs reclaims those resources. |
| maximumRunningInstances | [uint32](#uint32) |  | Maximum number of instances to admit and run at any point in time. If specified, should be &lt;= instanceCount and &gt;= minimumRunningInstances; default value is instanceCount. |
| minimumRunningInstances | [uint32](#uint32) |  | Minimum number of instances to admit and run at any point in time. If specified, should be &lt;= maximumRunningInstances &lt;= instanceCount; default value is 1. Admission requires the corresponding resource pool has enough reserved resources for the full set of minimum number of instances. |
| maxRunningTime | [uint32](#uint32) |  | maxRunningTime represents the max time which all tasks of this job can run. This timer starts when the task enters into running state. if the tasks of this job exceeds this run time then they will be killed |
| maximumUnavailableInstances | [uint32](#uint32) |  | Maximum number of job instances which can be unavailable at a given time. |






<a name="peloton.api.v0.job.StartConfig"/>

### StartConfig
DEPRECATED by peloton.api.job.svc.StartConfig
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling start, if unset all tasks specified will be started at the same time. |






<a name="peloton.api.v0.job.StartRequest"/>

### StartRequest
DEPRECATED by peloton.api.job.svc.StartJobRequest
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | The job to start |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job..peloton.api.v0.task.InstanceRange) | repeated | The instances to start, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| startConfig | [StartConfig](#peloton.api.v0.job.StartConfig) |  | The config for starting a job |






<a name="peloton.api.v0.job.StartResponse"/>

### StartResponse
DEPRECATED by peloton.api.job.svc.StartJobResponse
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the start |






<a name="peloton.api.v0.job.StopConfig"/>

### StopConfig
DEPRECATED by peloton.api.job.svc.StopConfig
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling stop, if unset all tasks specified will be restarted at the same time. |






<a name="peloton.api.v0.job.StopRequest"/>

### StopRequest
DEPRECATED by peloton.api.job.svc.StopJobRequest
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  | The job to restart |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job..peloton.api.v0.task.InstanceRange) | repeated | The instances to restart, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| stopConfig | [StopConfig](#peloton.api.v0.job.StopConfig) |  | The config for stopping a job |






<a name="peloton.api.v0.job.StopResponse"/>

### StopResponse
DEPRECATED by peloton.api.job.svc.StopJobResponse
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the stop |






<a name="peloton.api.v0.job.UpdateRequest"/>

### UpdateRequest
DEPRECATED by peloton.api.v0.job.svc.UpdateJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| config | [JobConfig](#peloton.api.v0.job.JobConfig) |  |  |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job. This list should contain existing secret IDs/paths with same or new data. It may also contain additional secrets. |






<a name="peloton.api.v0.job.UpdateResponse"/>

### UpdateResponse
DEPRECATED by peloton.api.v0.job.svc.UpdateJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [UpdateResponse.Error](#peloton.api.v0.job.UpdateResponse.Error) |  |  |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job..peloton.api.v0.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.job.UpdateResponse.Error"/>

### UpdateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobNotFound | [JobNotFound](#peloton.api.v0.job.JobNotFound) |  |  |
| invalidConfig | [InvalidJobConfig](#peloton.api.v0.job.InvalidJobConfig) |  |  |
| invalidJobId | [InvalidJobId](#peloton.api.v0.job.InvalidJobId) |  |  |





 


<a name="peloton.api.v0.job.JobState"/>

### JobState
Runtime states of a Job

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new states. |
| INITIALIZED | 1 | The job has been initialized and persisted in DB. |
| PENDING | 2 | All tasks have been created and persisted in DB, but no task is RUNNING yet. |
| RUNNING | 3 | Any of the tasks in the job is in RUNNING state. |
| SUCCEEDED | 4 | All tasks in the job are in SUCCEEDED state. |
| FAILED | 5 | All tasks in the job are in terminated state and one or more tasks is in FAILED state. |
| KILLED | 6 | All tasks in the job are in terminated state and one or more tasks in the job is killed by the user. |
| KILLING | 7 | All tasks in the job have been requested to be killed by the user. |
| UNINITIALIZED | 8 | The job is partially created and is not ready to be scheduled |
| DELETED | 9 | The job has been deleted. |



<a name="peloton.api.v0.job.JobType"/>

### JobType
Job type definition such as batch, service and infra agent.

| Name | Number | Description |
| ---- | ------ | ----------- |
| BATCH | 0 | Normal batch job which will run to completion after all instances finishes. |
| SERVICE | 1 | Service job which is long running and will be restarted upon failures. |
| DAEMON | 2 | Daemon job which has one instance running on each host for infra agents like muttley, m3collector etc. |


 

 


<a name="peloton.api.v0.job.JobManager"/>

### JobManager
DEPRECATED by peloton.api.v0.job.svc.JobService
Job Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#peloton.api.v0.job.CreateRequest) | [CreateResponse](#peloton.api.v0.job.CreateRequest) | Create a Job entity for a given config |
| Get | [GetRequest](#peloton.api.v0.job.GetRequest) | [GetResponse](#peloton.api.v0.job.GetRequest) | Get the config of a job entity |
| Query | [QueryRequest](#peloton.api.v0.job.QueryRequest) | [QueryResponse](#peloton.api.v0.job.QueryRequest) | Query the jobs that match a list of labels. |
| Delete | [DeleteRequest](#peloton.api.v0.job.DeleteRequest) | [DeleteResponse](#peloton.api.v0.job.DeleteRequest) | Delete a job entity and stop all related tasks |
| Update | [UpdateRequest](#peloton.api.v0.job.UpdateRequest) | [UpdateResponse](#peloton.api.v0.job.UpdateRequest) | Update a Job entity with a new config |
| Restart | [RestartRequest](#peloton.api.v0.job.RestartRequest) | [RestartResponse](#peloton.api.v0.job.RestartRequest) | Restart the tasks specified in restart request Experimental only |
| Start | [StartRequest](#peloton.api.v0.job.StartRequest) | [StartResponse](#peloton.api.v0.job.StartRequest) | Start the tasks specified in restart request Experimental only |
| Stop | [StopRequest](#peloton.api.v0.job.StopRequest) | [StopResponse](#peloton.api.v0.job.StopRequest) | Stop the tasks specified in stop request Experimental only |
| Refresh | [RefreshRequest](#peloton.api.v0.job.RefreshRequest) | [RefreshResponse](#peloton.api.v0.job.RefreshRequest) | Debug only method. Allows user to load job runtime state from DB and re-execute the action associated with current state. |
| GetCache | [GetCacheRequest](#peloton.api.v0.job.GetCacheRequest) | [GetCacheResponse](#peloton.api.v0.job.GetCacheRequest) | Debug only method. Get the cache of a job stored in Peloton. |
| GetActiveJobs | [GetActiveJobsRequest](#peloton.api.v0.job.GetActiveJobsRequest) | [GetActiveJobsResponse](#peloton.api.v0.job.GetActiveJobsRequest) | Debug only method. Get the list of active job IDs stored in Peloton. This method is experimental and will be deprecated It will be temporarily used for testing the consistency between active_jobs table and mv_job_by_state materialzied view |

 



<a name="volume.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume.proto



<a name="peloton.api.v0.volume.PersistentVolumeInfo"/>

### PersistentVolumeInfo
Persistent volume information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.VolumeID](#peloton.api.v0.volume..peloton.api.v0.peloton.VolumeID) |  | ID of the persistent volume. |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.volume..peloton.api.v0.peloton.JobID) |  | ID of the job that owns the volume. |
| instanceId | [uint32](#uint32) |  | ID of the instance that owns the volume. |
| hostname | [string](#string) |  | Hostname of the persisted volume. |
| state | [VolumeState](#peloton.api.v0.volume.VolumeState) |  | Current state of the volume. |
| goalState | [VolumeState](#peloton.api.v0.volume.VolumeState) |  | Goal state of the volume. |
| sizeMB | [uint32](#uint32) |  | Volume size in MB. |
| containerPath | [string](#string) |  | Volume mount path inside container. |
| createTime | [string](#string) |  | Volume creation time. |
| updateTime | [string](#string) |  | Volume info last update time. |





 


<a name="peloton.api.v0.volume.VolumeState"/>

### VolumeState
States of a persistent volume

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new states. |
| INITIALIZED | 1 | The persistent volume is being initialized. |
| CREATED | 2 | The persistent volume is created successfully. |
| DELETED | 3 | The persistent volume is deleted. |


 

 

 



<a name="query.proto"/>
<p align="right"><a href="#top">Top</a></p>

## query.proto



<a name="peloton.api.v1alpha.query.OrderBy"/>

### OrderBy
Order by clause of a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| order | [OrderBy.Order](#peloton.api.v1alpha.query.OrderBy.Order) |  |  |
| property | [PropertyPath](#peloton.api.v1alpha.query.PropertyPath) |  |  |






<a name="peloton.api.v1alpha.query.Pagination"/>

### Pagination
Generic pagination for a list of records to be returned by a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the pagination for a query result |
| limit | [uint32](#uint32) |  | Limit of the pagination for a query result |
| total | [uint32](#uint32) |  | Total number of records for a query result |






<a name="peloton.api.v1alpha.query.PaginationSpec"/>

### PaginationSpec
Pagination query spec used as argument to queries that returns a Pagination result.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the query for pagination |
| limit | [uint32](#uint32) |  | Limit per page of the query for pagination |
| order_by | [OrderBy](#peloton.api.v1alpha.query.OrderBy) | repeated | List of fields to be order by in sequence |
| max_limit | [uint32](#uint32) |  | Max limit of the pagination result. |






<a name="peloton.api.v1alpha.query.PropertyPath"/>

### PropertyPath
A dot separated path to a object property such as config.name or
runtime.creationTime for a job object.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 


<a name="peloton.api.v1alpha.query.OrderBy.Order"/>

### OrderBy.Order


| Name | Number | Description |
| ---- | ------ | ----------- |
| ORDER_BY_INVALID | 0 |  |
| ORDER_BY_ASC | 1 |  |
| ORDER_BY_DESC | 2 |  |


 

 

 



<a name="pod.proto"/>
<p align="right"><a href="#top">Top</a></p>

## pod.proto



<a name="peloton.api.v1alpha.pod.AndConstraint"/>

### AndConstraint
AndConstraint represents a logical &#39;and&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.v1alpha.pod.Constraint) | repeated |  |






<a name="peloton.api.v1alpha.pod.Constraint"/>

### Constraint
Constraint represents a host label constraint or a related pods label constraint.
This is used to require that a host have certain label constraints or to require
that the pods already running on the host have certain label constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Constraint.Type](#peloton.api.v1alpha.pod.Constraint.Type) |  |  |
| label_constraint | [LabelConstraint](#peloton.api.v1alpha.pod.LabelConstraint) |  |  |
| and_constraint | [AndConstraint](#peloton.api.v1alpha.pod.AndConstraint) |  |  |
| or_constraint | [OrConstraint](#peloton.api.v1alpha.pod.OrConstraint) |  |  |






<a name="peloton.api.v1alpha.pod.ContainerSpec"/>

### ContainerSpec
A single application container running inside a pod


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the container. Each container in a pod must have a unique name. Cannot be updated. |
| resource | [ResourceSpec](#peloton.api.v1alpha.pod.ResourceSpec) |  | Resource config of the container |
| container | [.mesos.v1.ContainerInfo](#peloton.api.v1alpha.pod..mesos.v1.ContainerInfo) |  | Container config of the container |
| command | [.mesos.v1.CommandInfo](#peloton.api.v1alpha.pod..mesos.v1.CommandInfo) |  | Command line config of the container |
| executor | [.mesos.v1.ExecutorInfo](#peloton.api.v1alpha.pod..mesos.v1.ExecutorInfo) |  | Custom executor config of the task. |
| liveness_check | [HealthCheckSpec](#peloton.api.v1alpha.pod.HealthCheckSpec) |  | Liveness health check config of the container |
| readiness_check | [HealthCheckSpec](#peloton.api.v1alpha.pod.HealthCheckSpec) |  | Readiness health check config of the container This is currently not supported. |
| ports | [PortSpec](#peloton.api.v1alpha.pod.PortSpec) | repeated | List of network ports to be allocated for the pod |






<a name="peloton.api.v1alpha.pod.ContainerStatus"/>

### ContainerStatus
Runtime status of a container in a pod


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the container |
| state | [ContainerState](#peloton.api.v1alpha.pod.ContainerState) |  | Runtime state of the container |
| ports | [ContainerStatus.PortsEntry](#peloton.api.v1alpha.pod.ContainerStatus.PortsEntry) | repeated | Dynamic ports reserved on the host while this container is running |
| message | [string](#string) |  | The message that explains the current state of a container such as why the container is failed. Only track the latest one if the container has been retried and failed multiple times. |
| reason | [string](#string) |  | The reason that explains the current state of a container. Only track the latest one if the container has been retried and failed multiple times. |
| failure_count | [uint32](#uint32) |  | The number of times the container has failed after retries. |
| healthy | [HealthStatus](#peloton.api.v1alpha.pod.HealthStatus) |  | The result of the health check |
| image | [string](#string) |  | The image the container is running |
| start_time | [string](#string) |  | The time when the container starts to run. Will be unset if the pod hasn&#39;t started running yet. The time is represented in RFC3339 form with UTC timezone. |
| completion_time | [string](#string) |  | The time when the container terminated. Will be unset if the pod hasn&#39;t completed yet. The time is represented in RFC3339 form with UTC timezone. |
| terminationStatus | [TerminationStatus](#peloton.api.v1alpha.pod.TerminationStatus) |  | Termination status of the task. Set only if the task is in a non-successful terminal state such as CONTAINER_STATE_FAILED or CONTAINER_STATE_KILLED. |






<a name="peloton.api.v1alpha.pod.ContainerStatus.PortsEntry"/>

### ContainerStatus.PortsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v1alpha.pod.HealthCheckSpec"/>

### HealthCheckSpec
Health check configuration for a container


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  | Whether the health check is enabled. |
| initial_interval_secs | [uint32](#uint32) |  | Start time wait in seconds. Zero or empty value would use default value of 15 from Mesos. |
| interval_secs | [uint32](#uint32) |  | Interval in seconds between two health checks. Zero or empty value would use default value of 10 from Mesos. |
| max_consecutive_failures | [uint32](#uint32) |  | Max number of consecutive failures before failing health check. Zero or empty value would use default value of 3 from Mesos. |
| timeout_secs | [uint32](#uint32) |  | Health check command timeout in seconds. Zero or empty value would use default value of 20 from Mesos. |
| type | [HealthCheckSpec.HealthCheckType](#peloton.api.v1alpha.pod.HealthCheckSpec.HealthCheckType) |  |  |
| command_check | [HealthCheckSpec.CommandCheck](#peloton.api.v1alpha.pod.HealthCheckSpec.CommandCheck) |  | Only applicable when type is `COMMAND`. |






<a name="peloton.api.v1alpha.pod.HealthCheckSpec.CommandCheck"/>

### HealthCheckSpec.CommandCheck



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [string](#string) |  | Health check command to be executed. Note that this command by default inherits all environment varibles from the container it&#39;s monitoring, unless `unshare_environments` is set to true. |
| unshare_environments | [bool](#bool) |  | If set, this check will not share the environment variables of the container. |






<a name="peloton.api.v1alpha.pod.HealthStatus"/>

### HealthStatus
The result of the health check


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [HealthState](#peloton.api.v1alpha.pod.HealthState) |  | The health check state |
| output | [string](#string) |  | The output of the health check run |






<a name="peloton.api.v1alpha.pod.InstanceIDRange"/>

### InstanceIDRange
Pod InstanceID range [from, to)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from | [uint32](#uint32) |  |  |
| to | [uint32](#uint32) |  |  |






<a name="peloton.api.v1alpha.pod.LabelConstraint"/>

### LabelConstraint
LabelConstraint represents a constraint on the number of occurrences of a given
label from the set of host labels or pod labels present on the host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [LabelConstraint.Kind](#peloton.api.v1alpha.pod.LabelConstraint.Kind) |  | Determines which labels the constraint should apply to. |
| condition | [LabelConstraint.Condition](#peloton.api.v1alpha.pod.LabelConstraint.Condition) |  | Determines which constraint there should be on the number of occurrences of the label. |
| label | [.peloton.api.v1alpha.peloton.Label](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.Label) |  | The label which this defines a constraint on: For Kind == HOST, each attribute on Mesos agent is transformed to a label, with `hostname` as a special label which is always inferred from agent hostname and set. |
| requirement | [uint32](#uint32) |  | A limit on the number of occurrences of the label. |






<a name="peloton.api.v1alpha.pod.OrConstraint"/>

### OrConstraint
OrConstraint represents a logical &#39;or&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.v1alpha.pod.Constraint) | repeated |  |






<a name="peloton.api.v1alpha.pod.PersistentVolumeSpec"/>

### PersistentVolumeSpec
Persistent volume configuration for a pod.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_path | [string](#string) |  | Volume mount path inside container. |
| size_mb | [uint32](#uint32) |  | Volume size in MB. |






<a name="peloton.api.v1alpha.pod.PodEvent"/>

### PodEvent
Pod events of a particular run of a job instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The current pod ID |
| actual_state | [string](#string) |  | Actual state of a pod |
| desired_state | [string](#string) |  | Goal State of a pod |
| timestamp | [string](#string) |  | The time when the event was created. The time is represented in RFC3339 form with UTC timezone. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.EntityVersion) |  | The entity version currently used by the pod. |
| desired_version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.EntityVersion) |  | The desired entity version that should be used by the pod. |
| agent_id | [string](#string) |  | The agentID for the pod |
| hostname | [string](#string) |  | The host on which the pod is running |
| message | [string](#string) |  | Short human friendly message explaining state. |
| reason | [string](#string) |  | The short reason for the pod event |
| prev_pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The previous pod ID |
| healthy | [string](#string) |  | The health check result of the pod |
| desired_pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The desired pod ID |






<a name="peloton.api.v1alpha.pod.PodInfo"/>

### PodInfo
Info of a pod in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [PodSpec](#peloton.api.v1alpha.pod.PodSpec) |  | Configuration of the pod |
| status | [PodStatus](#peloton.api.v1alpha.pod.PodStatus) |  | Runtime status of the pod |






<a name="peloton.api.v1alpha.pod.PodSpec"/>

### PodSpec
Pod configuration for a given job instance
Note that only add string/slice/ptr type into PodConfig directly due to
the limitation of go reflection inside our pod specific config logic.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodName) |  | Name of the pod |
| labels | [.peloton.api.v1alpha.peloton.Label](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.Label) | repeated | List of user-defined labels for the pod |
| init_containers | [ContainerSpec](#peloton.api.v1alpha.pod.ContainerSpec) | repeated | List of initialization containers belonging to the pod. These containers are assumed to run to completion and are executed in order prior to containers being started. If any init container fails, the pod is considered to have failed. Init containers cannot be configured to have readiness or liveness health checks. |
| containers | [ContainerSpec](#peloton.api.v1alpha.pod.ContainerSpec) | repeated | List of containers belonging to the pod. These will be started in parallel after init containers terminate. There must be at least one container in a pod. |
| constraint | [Constraint](#peloton.api.v1alpha.pod.Constraint) |  | Constraint on the attributes of the host or labels on pods on the host that this pod should run on. Use `AndConstraint`/`OrConstraint` to compose multiple constraints if necessary. |
| restart_policy | [RestartPolicy](#peloton.api.v1alpha.pod.RestartPolicy) |  | Pod restart policy on failures |
| volume | [PersistentVolumeSpec](#peloton.api.v1alpha.pod.PersistentVolumeSpec) |  | Persistent volume config of the pod. |
| preemption_policy | [PreemptionPolicy](#peloton.api.v1alpha.pod.PreemptionPolicy) |  | Preemption policy of the pod |
| controller | [bool](#bool) |  | Whether this is a controller pod. A controller is a special batch pod which controls other pods inside a job. E.g. spark driver pods in a spark job will be a controller pod. |
| kill_grace_period_seconds | [uint32](#uint32) |  | This is used to set the amount of time between when the executor sends the SIGTERM message to gracefully terminate a pod and when it kills it by sending SIGKILL. If you do not set the grace period duration the default is 30 seconds. |
| revocable | [bool](#bool) |  | revocable represents pod to use physical or slack resources. |






<a name="peloton.api.v1alpha.pod.PodStatus"/>

### PodStatus
Runtime status of a pod instance in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [PodState](#peloton.api.v1alpha.pod.PodState) |  | Runtime state of the pod |
| pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The current pod ID for this pod |
| start_time | [string](#string) |  | The time when the pod starts to run. Will be unset if the pod hasn&#39;t started running yet. The time is represented in RFC3339 form with UTC timezone. |
| completion_time | [string](#string) |  | The time when the pod is completed. Will be unset if the pod hasn&#39;t completed yet. The time is represented in RFC3339 form with UTC timezone. |
| host | [string](#string) |  | The name of the host where the pod is running |
| init_containers_status | [ContainerStatus](#peloton.api.v1alpha.pod.ContainerStatus) | repeated | Status of the init containers. |
| containers_status | [ContainerStatus](#peloton.api.v1alpha.pod.ContainerStatus) | repeated | Status of the containers. |
| desired_state | [PodState](#peloton.api.v1alpha.pod.PodState) |  | The desired state of the pod which should be eventually reached by the system. |
| message | [string](#string) |  | The message that explains the current state of a pod. |
| reason | [string](#string) |  | The reason that explains the current state of a pod. See Mesos TaskStatus.Reason for more details. |
| failure_count | [uint32](#uint32) |  | The number of times the pod has failed after retries. |
| volume_id | [.peloton.api.v1alpha.peloton.VolumeID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.VolumeID) |  | persistent volume id |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.EntityVersion) |  | The entity version currently used by the pod. TODO Avoid leaking job abstractions into public pod APIs. Remove after internal protobuf structures are defined. |
| desired_version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.EntityVersion) |  | The desired entity version that should be used by the pod. TODO Avoid leaking job abstractions into public pod APIs. Remove after internal protobuf structures are defined. |
| agent_id | [.mesos.v1.AgentID](#peloton.api.v1alpha.pod..mesos.v1.AgentID) |  | the id of mesos agent on the host to be launched. |
| revision | [.peloton.api.v1alpha.peloton.Revision](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.Revision) |  | Revision of the current pod status. |
| prev_pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The pod id of the previous pod. |
| resource_usage | [PodStatus.ResourceUsageEntry](#peloton.api.v1alpha.pod.PodStatus.ResourceUsageEntry) | repeated | The resource usage for this pod. The map key is each resource kind in string format and the map value is the number of unit-seconds of that resource used by the job. Example: if a pod that uses 1 CPU and finishes in 10 seconds, this map will contain &lt;&#34;cpu&#34;:10&gt; |
| desired_pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodID) |  | The desired pod ID for this pod |
| desiredHost | [string](#string) |  | The name of the host where the pod should be running on upon restart. It is used for best effort in-place update/restart. |






<a name="peloton.api.v1alpha.pod.PodStatus.ResourceUsageEntry"/>

### PodStatus.ResourceUsageEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [double](#double) |  |  |






<a name="peloton.api.v1alpha.pod.PodSummary"/>

### PodSummary
Summary information about a pod


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodName) |  | Name of the pod |
| status | [PodStatus](#peloton.api.v1alpha.pod.PodStatus) |  | Runtime status of the pod |






<a name="peloton.api.v1alpha.pod.PortSpec"/>

### PortSpec
Network port configuration for a container


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the network port, e.g. http, tchannel. Required field. |
| value | [uint32](#uint32) |  | Static port number if any. If unset, will be dynamically allocated by the scheduler |
| env_name | [string](#string) |  | Environment variable name to be exported when running a container for this port. Required field for dynamic port. |






<a name="peloton.api.v1alpha.pod.PreemptionPolicy"/>

### PreemptionPolicy
Preemption policy for a pod


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kill_on_preempt | [bool](#bool) |  | This policy defines if the pod should be restarted after it is preempted. If set to true the pod will not be rescheduled after it is preempted. If set to false the pod will be rescheduled. Defaults to false |






<a name="peloton.api.v1alpha.pod.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for pods. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.v1alpha.query.PaginationSpec](#peloton.api.v1alpha.pod..peloton.api.v1alpha.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |
| pod_states | [PodState](#peloton.api.v1alpha.pod.PodState) | repeated | List of pod states to query the pods. Will match all pods if the list is empty. |
| names | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod..peloton.api.v1alpha.peloton.PodName) | repeated | List of pod names to query the pods. Will match all names if the list is empty. |
| hosts | [string](#string) | repeated | List of hosts to query the pods. Will match all hosts if the list is empty. |






<a name="peloton.api.v1alpha.pod.ResourceSpec"/>

### ResourceSpec
Resource configuration for a container.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cpu_limit | [double](#double) |  | CPU limit in number of CPU cores |
| mem_limit_mb | [double](#double) |  | Memory limit in MB |
| disk_limit_mb | [double](#double) |  | Disk limit in MB |
| fd_limit | [uint32](#uint32) |  | File descriptor limit |
| gpu_limit | [double](#double) |  | GPU limit in number of GPUs |






<a name="peloton.api.v1alpha.pod.RestartPolicy"/>

### RestartPolicy
Restart policy for a pod.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| max_failures | [uint32](#uint32) |  | Max number of pod failures can occur before giving up scheduling retry, no backoff for now. Default 0 means no retry on failures. |






<a name="peloton.api.v1alpha.pod.TerminationStatus"/>

### TerminationStatus
TerminationStatus contains details about termination of a task. It mainly
contains Peloton-specific reasons for termination.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| reason | [TerminationStatus.Reason](#peloton.api.v1alpha.pod.TerminationStatus.Reason) |  | Reason for termination. |
| exit_code | [uint32](#uint32) |  | If non-zero, exit status when the container terminated. |
| signal | [string](#string) |  | Name of signal received by the container when it terminated. |





 


<a name="peloton.api.v1alpha.pod.Constraint.Type"/>

### Constraint.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| CONSTRAINT_TYPE_INVALID | 0 | Reserved for compatibility. |
| CONSTRAINT_TYPE_LABEL | 1 |  |
| CONSTRAINT_TYPE_AND | 2 |  |
| CONSTRAINT_TYPE_OR | 3 |  |



<a name="peloton.api.v1alpha.pod.ContainerState"/>

### ContainerState
Runtime states of a container in a pod

| Name | Number | Description |
| ---- | ------ | ----------- |
| CONTAINER_STATE_INVALID | 0 | Invalid state. |
| CONTAINER_STATE_PENDING | 1 | The container has not been created yet |
| CONTAINER_STATE_LAUNCHED | 2 | The container has been launched |
| CONTAINER_STATE_STARTING | 3 | The container is being started on a host |
| CONTAINER_STATE_RUNNING | 4 | The container is running on a host |
| CONTAINER_STATE_SUCCEEDED | 5 | The container terminated with an exit code of zero |
| CONTAINER_STATE_FAILED | 6 | The container terminated with a non-zero exit code |
| CONTAINER_STATE_KILLING | 7 | The container is being killed |
| CONTAINER_STATE_KILLED | 8 | Execution of the container was terminated by the system |



<a name="peloton.api.v1alpha.pod.HealthCheckSpec.HealthCheckType"/>

### HealthCheckSpec.HealthCheckType


| Name | Number | Description |
| ---- | ------ | ----------- |
| HEALTH_CHECK_TYPE_UNKNOWN | 0 | Reserved for future compatibility of new types. |
| HEALTH_CHECK_TYPE_COMMAND | 1 | Command line based health check |
| HEALTH_CHECK_TYPE_HTTP | 2 | HTTP endpoint based health check |
| HEALTH_CHECK_TYPE_GRPC | 3 | gRPC based health check |



<a name="peloton.api.v1alpha.pod.HealthState"/>

### HealthState
HealthState is the health check state of a container

| Name | Number | Description |
| ---- | ------ | ----------- |
| HEALTH_STATE_INVALID | 0 | Default value. |
| HEALTH_STATE_DISABLED | 1 | If the health check config is not enabled in the container config, then the health state is DISABLED. |
| HEALTH_STATE_UNKNOWN | 2 | If the health check config is enabled in the container config, but the container has not reported the output of the health check yet, then the health state is UNKNOWN. |
| HEALTH_STATE_HEALTHY | 3 | In a Mesos event, If the healthy field is true and the reason field is REASON_TASK_HEALTH_CHECK_STATUS_UPDATED the health state of the container is HEALTHY |
| HEALTH_STATE_UNHEALTHY | 4 | In a Mesos event, If the healthy field is false and the reason field is REASON_TASK_HEALTH_CHECK_STATUS_UPDATED the health state of the container is UNHEALTHY |



<a name="peloton.api.v1alpha.pod.LabelConstraint.Condition"/>

### LabelConstraint.Condition
Condition represents a constraint on the number of occurrences of the label.

| Name | Number | Description |
| ---- | ------ | ----------- |
| LABEL_CONSTRAINT_CONDITION_INVALID | 0 |  |
| LABEL_CONSTRAINT_CONDITION_LESS_THAN | 1 |  |
| LABEL_CONSTRAINT_CONDITION_EQUAL | 2 |  |
| LABEL_CONSTRAINT_CONDITION_GREATER_THAN | 3 |  |



<a name="peloton.api.v1alpha.pod.LabelConstraint.Kind"/>

### LabelConstraint.Kind
Kind represents whatever the constraint applies to the labels on the host
or to the labels of the pods that are located on the host.

| Name | Number | Description |
| ---- | ------ | ----------- |
| LABEL_CONSTRAINT_KIND_INVALID | 0 |  |
| LABEL_CONSTRAINT_KIND_POD | 1 |  |
| LABEL_CONSTRAINT_KIND_HOST | 2 |  |



<a name="peloton.api.v1alpha.pod.PodState"/>

### PodState
Runtime states of a pod instance

| Name | Number | Description |
| ---- | ------ | ----------- |
| POD_STATE_INVALID | 0 | Invalid state. |
| POD_STATE_INITIALIZED | 1 | The pod is being initialized |
| POD_STATE_PENDING | 2 | The pod is pending and waiting for resources |
| POD_STATE_READY | 3 | The pod has been allocated with resources and ready for placement |
| POD_STATE_PLACING | 4 | The pod is being placed to a host based on its resource requirements and constraints |
| POD_STATE_PLACED | 5 | The pod has been assigned to a host matching the resource requirements and constraints |
| POD_STATE_LAUNCHING | 6 | The pod is taken from resmgr to be launched |
| POD_STATE_LAUNCHED | 7 | The pod is being launched in Job manager |
| POD_STATE_STARTING | 8 | Either init containers are starting/running or the main containers in the pod are being started by Mesos agent |
| POD_STATE_RUNNING | 9 | All containers in the pod are running |
| POD_STATE_SUCCEEDED | 10 | All containers in the pod terminated with an exit code of zero |
| POD_STATE_FAILED | 11 | At least on container in the pod terminated with a non-zero exit code |
| POD_STATE_LOST | 12 | The pod is lost |
| POD_STATE_KILLING | 13 | The pod is being killed |
| POD_STATE_KILLED | 14 | At least one of the containers in the pod was terminated by the system |
| POD_STATE_PREEMPTING | 15 | The pod is being preempted by another one on the node |
| POD_STATE_DELETED | 16 | The pod is to be deleted after termination |



<a name="peloton.api.v1alpha.pod.TerminationStatus.Reason"/>

### TerminationStatus.Reason
Reason lists various causes for a task termination

| Name | Number | Description |
| ---- | ------ | ----------- |
| TERMINATION_STATUS_REASON_INVALID | 0 | Default value. |
| TERMINATION_STATUS_REASON_KILLED_ON_REQUEST | 1 | Task was killed because a stop request was received from a client. |
| TERMINATION_STATUS_REASON_FAILED | 2 | Task failed. See also TerminationStatus.exit_code, TerminationStatus.signal and ContainerStatus.message. |
| TERMINATION_STATUS_REASON_KILLED_HOST_MAINTENANCE | 3 | Task was killed to put the host in to maintenance. |
| TERMINATION_STATUS_REASON_PREEMPTED_RESOURCES | 4 | Tasked was killed to reclaim resources allocated to it. |


 

 

 



<a name="host.proto"/>
<p align="right"><a href="#top">Top</a></p>

## host.proto



<a name="peloton.api.v1alpha.host.HostInfo"/>

### HostInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  | The hostname of the host |
| ip | [string](#string) |  | The IP address of the host |
| state | [HostState](#peloton.api.v1alpha.host.HostState) |  | The current state of the host |





 


<a name="peloton.api.v1alpha.host.HostState"/>

### HostState


| Name | Number | Description |
| ---- | ------ | ----------- |
| HOST_STATE_INVALID | 0 |  |
| HOST_STATE_UNKNOWN | 1 | Reserved for future compatibility of new states. |
| HOST_STATE_UP | 2 | The host is healthy |
| HOST_STATE_DRAINING | 3 | The tasks running on the host are being rescheduled. There will be no further placement of tasks on the host |
| HOST_STATE_DRAINED | 4 | There are no tasks running on the host and is ready to be put into maintenance. |
| HOST_STATE_DOWN | 5 | The host is in maintenance. |


 

 

 



<a name="respool.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool.proto



<a name="peloton.api.v1alpha.respool.ControllerLimit"/>

### ControllerLimit
The max limit of resources `CONTROLLER`(see TaskType) tasks can use in
this resource pool. This is defined as a percentage of the resource pool&#39;s
reservation. If undefined there is no maximum limit for controller tasks
i.e. controller tasks will not be treated differently. For eg if the
resource pool&#39;s reservation is defined as:

cpu:100
mem:1000
disk:1000
gpu:10

And the ControllerLimit = 10 ,Then the maximum resources the controller
tasks can use is 10% of the reservation, i.e.

cpu:10
mem:100
disk:100
gpu:1


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| max_percent | [double](#double) |  |  |






<a name="peloton.api.v1alpha.respool.ResourcePoolInfo"/>

### ResourcePoolInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | Resource Pool Id |
| spec | [ResourcePoolSpec](#peloton.api.v1alpha.respool.ResourcePoolSpec) |  | ResourcePool spec |
| parent | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent TODO: parent duplicated from ResourcePoolConfig |
| children | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) | repeated | Resource Pool&#39;s children |
| usages | [ResourceUsage](#peloton.api.v1alpha.respool.ResourceUsage) | repeated | Resource usage for each resource kind |
| path | [ResourcePoolPath](#peloton.api.v1alpha.respool.ResourcePoolPath) |  | Resource Pool Path |






<a name="peloton.api.v1alpha.respool.ResourcePoolPath"/>

### ResourcePoolPath
A fully qualified path to a resource pool in a resource pool hierrarchy.
The path to a resource pool can be defined as an absolute path,
starting from the root node and separated by a slash.

The resource hierarchy is anchored at a node called the root,
designated by a slash &#34;/&#34;.

For the below resource hierarchy ; the &#34;compute&#34; resource pool would be
desgignated by path: /infrastructure/compute
root
├─ infrastructure
│  └─ compute
└─ marketplace


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.v1alpha.respool.ResourcePoolSpec"/>

### ResourcePoolSpec
Resource Pool configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revision | [.peloton.api.v1alpha.peloton.Revision](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.Revision) |  | Revision of the Resource Pool config |
| name | [string](#string) |  | Name of the resource pool |
| owning_team | [string](#string) |  | Owning team of the pool |
| ldap_groups | [string](#string) | repeated | LDAP groups of the pool |
| description | [string](#string) |  | Description of the resource pool |
| resources | [ResourceSpec](#peloton.api.v1alpha.respool.ResourceSpec) | repeated | Resource config of the Resource Pool |
| parent | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent |
| policy | [SchedulingPolicy](#peloton.api.v1alpha.respool.SchedulingPolicy) |  | Task Scheduling policy |
| controller_limit | [ControllerLimit](#peloton.api.v1alpha.respool.ControllerLimit) |  | The controller limit for this resource pool |
| slack_limit | [SlackLimit](#peloton.api.v1alpha.respool.SlackLimit) |  | Cap on max non-slack resources[mem,disk] in percentage that can be used by revocable task. |






<a name="peloton.api.v1alpha.respool.ResourceSpec"/>

### ResourceSpec
Resource configuration for a resource


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| reservation | [double](#double) |  | Reservation/min of the resource |
| limit | [double](#double) |  | Limit of the resource |
| share | [double](#double) |  | Share on the resource pool |
| type | [ReservationType](#peloton.api.v1alpha.respool.ReservationType) |  | ReservationType indicates the the type of reservation There are two kind of reservation 1. ELASTIC 2. STATIC |






<a name="peloton.api.v1alpha.respool.ResourceUsage"/>

### ResourceUsage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| allocation | [double](#double) |  | Allocation of the resource |
| slack | [double](#double) |  | slack is the resource which is allocated but not used and mesos will give those resources as revocable offers |






<a name="peloton.api.v1alpha.respool.SlackLimit"/>

### SlackLimit
The max limit of resources `REVOCABLE`(see TaskType) tasks can use in
this resource pool. This is defined as a percentage of the resource pool&#39;s
reservation. If undefined there is no maximum limit for revocable tasks
i.e. revocable tasks will not be treated differently. For eg if the
resource pool&#39;s reservation is defined as:

cpu:100
mem:1000
disk:1000

And the SlackLimit = 10 ,Then the maximum resources the revocable
tasks can use is 10% of the reservation, i.e.

mem:100
disk:100

For cpu, it will use revocable resources.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxPercent | [double](#double) |  |  |





 


<a name="peloton.api.v1alpha.respool.ReservationType"/>

### ReservationType
ReservationType indicates reservation type for the resourcepool

| Name | Number | Description |
| ---- | ------ | ----------- |
| RESERVATION_TYPE_INVALID | 0 |  |
| RESERVATION_TYPE_ELASTIC | 1 | ELASTIC reservation enables resource pool to be elastic in reservation , which means other resource pool can take resources from this resource pool as well as this resource pool also can take resources from any other resource pool. This is the by default behavior for the resource pool |
| RESERVATION_TYPE_STATIC | 2 | STATIC reservation enables resource pool to be static in reservation , which means irrespective of the demand this resource pool will have atleast reservation as entitlement value. No other resource pool can take resources from this resource pool. If demand for this resource pool is high it can take resources from other resource pools. By default value for reservation type ELASTIC. |



<a name="peloton.api.v1alpha.respool.SchedulingPolicy"/>

### SchedulingPolicy
Scheduling policy for Resource Pool.

| Name | Number | Description |
| ---- | ------ | ----------- |
| SCHEDULING_POLICY_INVALID | 0 |  |
| SCHEDULING_POLICY_PRIORITY_FIFO | 1 | This scheduling policy will return item for highest priority in FIFO order |


 

 

 



<a name="watch.proto"/>
<p align="right"><a href="#top">Top</a></p>

## watch.proto



<a name="peloton.api.v1alpha.watch.PodFilter"/>

### PodFilter
PodFilter specifies the pod(s) to watch. Watch on pods is restricted
to a single job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.watch..peloton.api.v1alpha.peloton.JobID) |  | The JobID of the pods that will be monitored. Mandatory. |
| pod_names | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.watch..peloton.api.v1alpha.peloton.PodName) | repeated | Names of the pods to watch. If empty, all pods in the job will be monitored. |






<a name="peloton.api.v1alpha.watch.StatelessJobFilter"/>

### StatelessJobFilter
StatelessJobFilter specifies the job(s) to watch.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_ids | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.watch..peloton.api.v1alpha.peloton.JobID) | repeated | The IDs of the jobs to watch. If unset, all jobs will be monitored. |





 

 

 

 



<a name="volume.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume.proto



<a name="peloton.api.v1alpha.volume.PersistentVolumeInfo"/>

### PersistentVolumeInfo
Persistent volume information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volume_id | [.peloton.api.v1alpha.peloton.VolumeID](#peloton.api.v1alpha.volume..peloton.api.v1alpha.peloton.VolumeID) |  | ID of the persistent volume. |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.volume..peloton.api.v1alpha.peloton.PodName) |  | ID of the pod that owns the volume. |
| hostname | [string](#string) |  | Hostname of the persisted volume. |
| state | [VolumeState](#peloton.api.v1alpha.volume.VolumeState) |  | Current state of the volume. |
| desired_state | [VolumeState](#peloton.api.v1alpha.volume.VolumeState) |  | Goal state of the volume. |
| size_mb | [uint32](#uint32) |  | Volume size in MB. |
| container_path | [string](#string) |  | Volume mount path inside container. |
| create_time | [string](#string) |  | Volume creation time. |
| update_time | [string](#string) |  | Volume info last update time. |





 


<a name="peloton.api.v1alpha.volume.VolumeState"/>

### VolumeState
States of a persistent volume

| Name | Number | Description |
| ---- | ------ | ----------- |
| VOLUME_STATE_INVALID | 0 | Reserved for future compatibility of new states. |
| VOLUME_STATE_INITIALIZED | 1 | The persistent volume is being initialized. |
| VOLUME_STATE_CREATED | 2 | The persistent volume is created successfully. |
| VOLUME_STATE_DELETED | 3 | The persistent volume is deleted. |


 

 

 



<a name="host_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## host_svc.proto



<a name="peloton.api.v0.host.svc.CompleteMaintenanceRequest"/>

### CompleteMaintenanceRequest
Request message for HostService.CompleteMaintenance method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostnames | [string](#string) | repeated | List of hosts put be brought back up |






<a name="peloton.api.v0.host.svc.CompleteMaintenanceResponse"/>

### CompleteMaintenanceResponse
Response message for HostService.CompleteMaintenance method.






<a name="peloton.api.v0.host.svc.QueryHostsRequest"/>

### QueryHostsRequest
Request message for HostService.QueryHosts method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_states | [.peloton.api.v0.host.HostState](#peloton.api.v0.host.svc..peloton.api.v0.host.HostState) | repeated | List of host states to query the hosts. Will return all hosts if the list is empty. |






<a name="peloton.api.v0.host.svc.QueryHostsResponse"/>

### QueryHostsResponse
Response message for HostService.QueryHosts method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_infos | [.peloton.api.v0.host.HostInfo](#peloton.api.v0.host.svc..peloton.api.v0.host.HostInfo) | repeated | List of hosts that match the host query criteria. |






<a name="peloton.api.v0.host.svc.StartMaintenanceRequest"/>

### StartMaintenanceRequest
Request message for HostService.StartMaintenance method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostnames | [string](#string) | repeated | List of hosts to be put into maintenance |






<a name="peloton.api.v0.host.svc.StartMaintenanceResponse"/>

### StartMaintenanceResponse
Response message for HostService.StartMaintenance method.





 

 

 


<a name="peloton.api.v0.host.svc.HostService"/>

### HostService
HostService defines the host related methods such as query hosts, start maintenance,
complete maintenance etc.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| QueryHosts | [QueryHostsRequest](#peloton.api.v0.host.svc.QueryHostsRequest) | [QueryHostsResponse](#peloton.api.v0.host.svc.QueryHostsRequest) | Get hosts which are in one of the specified states |
| StartMaintenance | [StartMaintenanceRequest](#peloton.api.v0.host.svc.StartMaintenanceRequest) | [StartMaintenanceResponse](#peloton.api.v0.host.svc.StartMaintenanceRequest) | Start maintenance on the specified hosts |
| CompleteMaintenance | [CompleteMaintenanceRequest](#peloton.api.v0.host.svc.CompleteMaintenanceRequest) | [CompleteMaintenanceResponse](#peloton.api.v0.host.svc.CompleteMaintenanceRequest) | Complete maintenance on the specified hosts |

 



<a name="update_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## update_svc.proto



<a name="peloton.api.v0.update.svc.AbortUpdateRequest"/>

### AbortUpdateRequest
Request message for UpdateService.AbortUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  | Identifier of the update to be aborted. |
| softAbort | [bool](#bool) |  |  |
| opaque_data | [.peloton.api.v0.peloton.OpaqueData](#peloton.api.v0.update.svc..peloton.api.v0.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v0.update.svc.AbortUpdateResponse"/>

### AbortUpdateResponse
Response message for UpdateService.AbortUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.






<a name="peloton.api.v0.update.svc.CreateUpdateRequest"/>

### CreateUpdateRequest
Request message for UpdateService.CreateUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.JobID) |  | Entity id of the job to be updated. |
| jobConfig | [.peloton.api.v0.job.JobConfig](#peloton.api.v0.update.svc..peloton.api.v0.job.JobConfig) |  | New configuration of the job to be updated. The new job config will be applied to all instances without violating the job SLA. |
| updateConfig | [.peloton.api.v0.update.UpdateConfig](#peloton.api.v0.update.svc..peloton.api.v0.update.UpdateConfig) |  | The options of the update. |
| opaque_data | [.peloton.api.v0.peloton.OpaqueData](#peloton.api.v0.update.svc..peloton.api.v0.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v0.update.svc.CreateUpdateResponse"/>

### CreateUpdateResponse
Response message for UpdateService.CreateUpdate method.
Returns errors:
NOT_FOUND:      if the job with the provided identifier is not found.
INVALID_ARGUMENT: if the provided job config or update config is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  | Identifier for the newly created update. |






<a name="peloton.api.v0.update.svc.GetUpdateCacheRequest"/>

### GetUpdateCacheRequest
Request message for UpdateService.GetUpdateCache method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  |  |






<a name="peloton.api.v0.update.svc.GetUpdateCacheResponse"/>

### GetUpdateCacheResponse
Response message for UpdateService.GetUpdateCache method.
Returns errors:
INVALID_ARGUMENT: if the update ID is not provided.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.JobID) |  | Job ID of the job update |
| state | [.peloton.api.v0.update.State](#peloton.api.v0.update.svc..peloton.api.v0.update.State) |  | The state of the job update |
| instancesTotal | [uint32](#uint32) | repeated | List of instances which will be updated with this update |
| instancesDone | [uint32](#uint32) | repeated | List of instances which have already been updated |
| instancesCurrent | [uint32](#uint32) | repeated | List of instances which are currently being updated |
| instancesAdded | [uint32](#uint32) | repeated | List of instances which have been added with this update |
| instancesUpdated | [uint32](#uint32) | repeated | List of existing instances which need to be updated with this update |
| instancesFailed | [uint32](#uint32) | repeated | List of existing instances which fail to be updated with this update |






<a name="peloton.api.v0.update.svc.GetUpdateRequest"/>

### GetUpdateRequest
Request message for UpdateService.GetUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  |  |
| statusOnly | [bool](#bool) |  | If set, only return the update status in the response. |






<a name="peloton.api.v0.update.svc.GetUpdateResponse"/>

### GetUpdateResponse
Response message for UpdateService.GetUpdate method.
Returns errors:
INVALID_ARGUMENT: if the update ID is not provided.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateInfo | [.peloton.api.v0.update.UpdateInfo](#peloton.api.v0.update.svc..peloton.api.v0.update.UpdateInfo) |  | Update information. |






<a name="peloton.api.v0.update.svc.ListUpdatesRequest"/>

### ListUpdatesRequest
Request message for UpdateService.ListUpdates method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [int32](#int32) |  | Number of updates to return. Not supported. |
| jobID | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.JobID) |  | Updates will be returned for the given job identifier. |






<a name="peloton.api.v0.update.svc.ListUpdatesResponse"/>

### ListUpdatesResponse
Response message for UpdateService.ListUpdates method.
Returns errors:
INVALID_ARGUMENT: if the job ID is not provided.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateInfo | [.peloton.api.v0.update.UpdateInfo](#peloton.api.v0.update.svc..peloton.api.v0.update.UpdateInfo) | repeated |  |






<a name="peloton.api.v0.update.svc.PauseUpdateRequest"/>

### PauseUpdateRequest
Request message for UpdateService.PauseUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  | Identifier of the update to be paused. |
| opaque_data | [.peloton.api.v0.peloton.OpaqueData](#peloton.api.v0.update.svc..peloton.api.v0.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v0.update.svc.PauseUpdateResponse"/>

### PauseUpdateResponse
Response message for UpdateService.PauseUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be paused.






<a name="peloton.api.v0.update.svc.ResumeUpdateRequest"/>

### ResumeUpdateRequest
Request message for UpdateService.ResumeUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  | Identifier of the update to be resumed. |
| opaque_data | [.peloton.api.v0.peloton.OpaqueData](#peloton.api.v0.update.svc..peloton.api.v0.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v0.update.svc.ResumeUpdateResponse"/>

### ResumeUpdateResponse
Response message for UpdateService.ResumeUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.






<a name="peloton.api.v0.update.svc.RollbackUpdateRequest"/>

### RollbackUpdateRequest
Request message for UpdateService.RollbackUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.update.svc..peloton.api.v0.peloton.UpdateID) |  | Identifier of the update to be rolled back. |






<a name="peloton.api.v0.update.svc.RollbackUpdateResponse"/>

### RollbackUpdateResponse
Response message for UpdateService.RollbackUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.





 

 

 


<a name="peloton.api.v0.update.svc.UpdateService"/>

### UpdateService
Update service interface
EXPERIMENTAL: This API is not yet stable.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateUpdate | [CreateUpdateRequest](#peloton.api.v0.update.svc.CreateUpdateRequest) | [CreateUpdateResponse](#peloton.api.v0.update.svc.CreateUpdateRequest) | Create a new update for a job. Only one update can exist for a job at a given time. |
| GetUpdate | [GetUpdateRequest](#peloton.api.v0.update.svc.GetUpdateRequest) | [GetUpdateResponse](#peloton.api.v0.update.svc.GetUpdateRequest) | Get the status of an update. |
| ListUpdates | [ListUpdatesRequest](#peloton.api.v0.update.svc.ListUpdatesRequest) | [ListUpdatesResponse](#peloton.api.v0.update.svc.ListUpdatesRequest) | List all updates (including current and previously completed) for a given job. |
| PauseUpdate | [PauseUpdateRequest](#peloton.api.v0.update.svc.PauseUpdateRequest) | [PauseUpdateResponse](#peloton.api.v0.update.svc.PauseUpdateRequest) | Pause an update. |
| ResumeUpdate | [ResumeUpdateRequest](#peloton.api.v0.update.svc.ResumeUpdateRequest) | [ResumeUpdateResponse](#peloton.api.v0.update.svc.ResumeUpdateRequest) | Resume a paused update. |
| RollbackUpdate | [RollbackUpdateRequest](#peloton.api.v0.update.svc.RollbackUpdateRequest) | [RollbackUpdateResponse](#peloton.api.v0.update.svc.RollbackUpdateRequest) | Rollback an update. |
| AbortUpdate | [AbortUpdateRequest](#peloton.api.v0.update.svc.AbortUpdateRequest) | [AbortUpdateResponse](#peloton.api.v0.update.svc.AbortUpdateRequest) | Abort an update. |
| GetUpdateCache | [GetUpdateCacheRequest](#peloton.api.v0.update.svc.GetUpdateCacheRequest) | [GetUpdateCacheResponse](#peloton.api.v0.update.svc.GetUpdateCacheRequest) | Debug only method. Get the cache of a job update. |

 



<a name="respool_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool_svc.proto



<a name="peloton.api.v0.respool.CreateResourcePoolRequest"/>

### CreateResourcePoolRequest
Request message for ResourcePoolService.CreateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The unique resource pool UUID specified by the client. This can be used by the client to re-create a failed resource pool without the side-effect of creating duplicated resource pool. If unset, the server will create a new UUID for the resource pool. |
| config | [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig) |  | The detailed configuration of the resource pool be to created. |






<a name="peloton.api.v0.respool.CreateResourcePoolResponse"/>

### CreateResourcePoolResponse
Response message for ResourcePoolService.CreateResourcePool method.

Return errors:
ALREADY_EXISTS:   if the resource pool already exists.
INVALID_ARGUMENT: if the resource pool config is invalid.o


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The ID of the newly created resource pool. |






<a name="peloton.api.v0.respool.DeleteResourcePoolRequest"/>

### DeleteResourcePoolRequest
Request message for ResourcePoolService.DeleteResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The ID of the resource pool to be deleted. |






<a name="peloton.api.v0.respool.DeleteResourcePoolResponse"/>

### DeleteResourcePoolResponse
Response message for ResourcePoolService.DeleteResourcePool method.

Return errors:
NOT_FOUND:        if the resource pool is not found.
INVALID_ARGUMENT: if the resource pool is not leaf node.
FAILED_PRECONDITION:  if the resource pool is busy.
INTERNAL:         if the resource pool fail to delete for internal errors.






<a name="peloton.api.v0.respool.GetResourcePoolRequest"/>

### GetResourcePoolRequest
Request message for ResourcePoolService.GetResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The ID of the resource pool to get the detailed information. |
| includeChildPools | [bool](#bool) |  | Whether or not to include the resource pool info of the direct children |






<a name="peloton.api.v0.respool.GetResourcePoolResponse"/>

### GetResourcePoolResponse
Response message for ResourcePoolService.GetResourcePool method.

Return errors:
NOT_FOUND:   if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resPool | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) |  | The detailed information of the resource pool. |
| childResPools | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) | repeated | The list of child resource pools. |






<a name="peloton.api.v0.respool.LookupResourcePoolIDRequest"/>

### LookupResourcePoolIDRequest
Request message for ResourcePoolService.LookupResourcePoolID method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v0.respool.ResourcePoolPath) |  | The resource pool path to look up the resource pool ID. |






<a name="peloton.api.v0.respool.LookupResourcePoolIDResponse"/>

### LookupResourcePoolIDResponse
Response message for ResourcePoolService.LookupResourcePoolID method.

Return errors:
NOT_FOUND:        if the resource pool is not found.
INVALID_ARGUMENT: if the resource pool path is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The resource pool ID for the given resource pool path. |






<a name="peloton.api.v0.respool.QueryResourcePoolsRequest"/>

### QueryResourcePoolsRequest
Request message for ResourcePoolService.QueryResourcePools method.


TODO Filters






<a name="peloton.api.v0.respool.QueryResourcePoolsResponse"/>

### QueryResourcePoolsResponse
Response message for ResourcePoolService.QueryResourcePools method.

Return errors:


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resPools | [ResourcePoolInfo](#peloton.api.v0.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.v0.respool.UpdateResourcePoolRequest"/>

### UpdateResourcePoolRequest
Request message for ResourcePoolService.UpdateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.ResourcePoolID](#peloton.api.v0.respool..peloton.api.v0.peloton.ResourcePoolID) |  | The ID of the resource pool to update the configuration. |
| config | [ResourcePoolConfig](#peloton.api.v0.respool.ResourcePoolConfig) |  | The configuration of the resource pool to be updated. |






<a name="peloton.api.v0.respool.UpdateResourcePoolResponse"/>

### UpdateResourcePoolResponse
Response message for ResourcePoolService.UpdateResourcePool method.

Return errors:
NOT_FOUND:   if the resource pool is not found.





 

 

 


<a name="peloton.api.v0.respool.ResourcePoolService"/>

### ResourcePoolService
ResourcePoolService defines the resource pool related methods
such as create, get, delete and upgrade resource pools.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateResourcePool | [CreateResourcePoolRequest](#peloton.api.v0.respool.CreateResourcePoolRequest) | [CreateResourcePoolResponse](#peloton.api.v0.respool.CreateResourcePoolRequest) | Create a resource pool entity for a given config |
| GetResourcePool | [GetResourcePoolRequest](#peloton.api.v0.respool.GetResourcePoolRequest) | [GetResourcePoolResponse](#peloton.api.v0.respool.GetResourcePoolRequest) | Get the resource pool entity |
| DeleteResourcePool | [DeleteResourcePoolRequest](#peloton.api.v0.respool.DeleteResourcePoolRequest) | [DeleteResourcePoolResponse](#peloton.api.v0.respool.DeleteResourcePoolRequest) | Delete a resource pool entity |
| UpdateResourcePool | [UpdateResourcePoolRequest](#peloton.api.v0.respool.UpdateResourcePoolRequest) | [UpdateResourcePoolResponse](#peloton.api.v0.respool.UpdateResourcePoolRequest) | Modify a resource pool entity |
| LookupResourcePoolID | [LookupResourcePoolIDRequest](#peloton.api.v0.respool.LookupResourcePoolIDRequest) | [LookupResourcePoolIDResponse](#peloton.api.v0.respool.LookupResourcePoolIDRequest) | Lookup the resource pool ID for a given resource pool path |
| QueryResourcePools | [QueryResourcePoolsRequest](#peloton.api.v0.respool.QueryResourcePoolsRequest) | [QueryResourcePoolsResponse](#peloton.api.v0.respool.QueryResourcePoolsRequest) | Query the resource pools. |

 



<a name="task_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## task_svc.proto



<a name="peloton.api.v0.task.svc.BrowseSandboxRequest"/>

### BrowseSandboxRequest
Request message for TaskService.BrowseSandbox method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the task to browse the sandbox. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to browse the sandbox. |
| taskId | [string](#string) |  | Get the sandbox path of a particular task of an instance. This should be set to the mesos task id in the runtime of the task for which the sandbox is being requested. If not provided, the path of the latest task is returned. |






<a name="peloton.api.v0.task.svc.BrowseSandboxResponse"/>

### BrowseSandboxResponse
Response message for TaskService.BrowseSandbox method.

Return errors:
NOT_FOUND:     if the job ID is not found.
OUT_OF_RANGE:  if the instance IDs are out of range.
INTERNAL:      if fail to browse the sandbox for internal errors.
FAILED_PRECONDITION:  if the task has not been run yet.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  | The hostname of the sandbox. |
| port | [string](#string) |  | The port of the sandbox. |
| paths | [string](#string) | repeated | The list of sandbox file paths. TODO: distinguish files and directories in the sandbox |
| mesosMasterHostname | [string](#string) |  | Mesos Master hostname and port. |
| mesosMasterPort | [string](#string) |  |  |






<a name="peloton.api.v0.task.svc.GetPodEventsRequest"/>

### GetPodEventsRequest
Request message for TaskService.GetPodEvents method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the task |
| instanceId | [uint32](#uint32) |  | The instance ID of the task |
| limit | [uint64](#uint64) |  | Limit of events |






<a name="peloton.api.v0.task.svc.GetPodEventsResponse"/>

### GetPodEventsResponse
Response message for TaskService.GetPodEvents method.

Return errors:
INTERNAL:      if failed to get pod events for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [PodEvent](#peloton.api.v0.task.svc.PodEvent) | repeated |  |
| error | [GetPodEventsResponse.Error](#peloton.api.v0.task.svc.GetPodEventsResponse.Error) |  |  |






<a name="peloton.api.v0.task.svc.GetPodEventsResponse.Error"/>

### GetPodEventsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.v0.task.svc.GetTaskCacheRequest"/>

### GetTaskCacheRequest
Request message for TaskService.GetTaskCache method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID to look up the task. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to get. |






<a name="peloton.api.v0.task.svc.GetTaskCacheResponse"/>

### GetTaskCacheResponse
Response message for TaskService.GetTaskCache method.

Return errors:
NOT_FOUND:      if the job or task not found in cache.
INTERNAL_ERROR: if fail to read cache due to internal error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| runtime | [.peloton.api.v0.task.RuntimeInfo](#peloton.api.v0.task.svc..peloton.api.v0.task.RuntimeInfo) |  | The task runtime of the task. |






<a name="peloton.api.v0.task.svc.GetTaskRequest"/>

### GetTaskRequest
Request message for TaskService.GetTask method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the task to get. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to get. |






<a name="peloton.api.v0.task.svc.GetTaskResponse"/>

### GetTaskResponse
Response message for TaskService.GetTask method.

Return errors:
NOT_FOUND:     if the job or task not found.
OUT_OF_RANGE:  if the instance ID is out of range.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.v0.task.TaskInfo](#peloton.api.v0.task.svc..peloton.api.v0.task.TaskInfo) |  | The task info of the task. DEPRECATED |
| results | [.peloton.api.v0.task.TaskInfo](#peloton.api.v0.task.svc..peloton.api.v0.task.TaskInfo) | repeated | Returns all active and completed tasks of the given instance. |






<a name="peloton.api.v0.task.svc.ListTasksRequest"/>

### ListTasksRequest
Request message for TaskService.ListTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to list. |
| range | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.task.svc..peloton.api.v0.task.InstanceRange) |  | The instance ID range of the tasks to list. |






<a name="peloton.api.v0.task.svc.ListTasksResponse"/>

### ListTasksResponse
Response message for TaskService.GetTask method.

Return errors:
NOT_FOUND:  if the job ID is not found.
OUT_OF_RANGE:  if the instance IDs are out of range.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [ListTasksResponse.TasksEntry](#peloton.api.v0.task.svc.ListTasksResponse.TasksEntry) | repeated | The map of instance ID to task info for all matching tasks. |






<a name="peloton.api.v0.task.svc.ListTasksResponse.TasksEntry"/>

### ListTasksResponse.TasksEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [.peloton.api.v0.task.TaskInfo](#peloton.api.v0.task.svc..peloton.api.v0.task.TaskInfo) |  |  |






<a name="peloton.api.v0.task.svc.PodEvent"/>

### PodEvent
Pod event of a Peloton pod instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskId | [.peloton.api.v0.peloton.TaskID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.TaskID) |  | The task ID of the task event. |
| actualState | [string](#string) |  | Actual state of an instance |
| goalState | [string](#string) |  | Goal State of an instance |
| timestamp | [string](#string) |  | The time when the event was created. The time is represented in RFC3339 form with UTC timezone. |
| configVersion | [uint64](#uint64) |  | The config version currently used by the runtime. |
| desiredConfigVersion | [uint64](#uint64) |  | The desired config version that should be used by the runtime. |
| agentID | [string](#string) |  | The agentID for the task |
| hostname | [string](#string) |  | The host on which the task is running |
| message | [string](#string) |  | Short human friendly message explaining state. |
| reason | [string](#string) |  | The short reason for the task event |
| prevTaskId | [.peloton.api.v0.peloton.TaskID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.TaskID) |  | The previous task ID of the pod event. |
| healthy | [string](#string) |  | The health check state of the task |






<a name="peloton.api.v0.task.svc.QueryTasksRequest"/>

### QueryTasksRequest
Request message for TaskService.QueryTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to query. |
| spec | [.peloton.api.v0.task.QuerySpec](#peloton.api.v0.task.svc..peloton.api.v0.task.QuerySpec) |  | The spec of query criteria for the tasks. |
| pagination | [.peloton.api.v0.query.PaginationSpec](#peloton.api.v0.task.svc..peloton.api.v0.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |






<a name="peloton.api.v0.task.svc.QueryTasksResponse"/>

### QueryTasksResponse
Response message for TaskService.QueryTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found.
INTERNAL:      if fail to query the tasks for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [.peloton.api.v0.task.TaskInfo](#peloton.api.v0.task.svc..peloton.api.v0.task.TaskInfo) | repeated | List of tasks that match the task query criteria. |
| pagination | [.peloton.api.v0.query.Pagination](#peloton.api.v0.task.svc..peloton.api.v0.query.Pagination) |  | Pagination result of the task query. |






<a name="peloton.api.v0.task.svc.RefreshTasksRequest"/>

### RefreshTasksRequest
Request message for TaskService.RefreshTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to list. |
| range | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.task.svc..peloton.api.v0.task.InstanceRange) |  | The instance ID range of the tasks to refresh. |






<a name="peloton.api.v0.task.svc.RefreshTasksResponse"/>

### RefreshTasksResponse
Response message for TaskService.RefreshTasks method.

Return errors:
NOT_FOUND:  if the job or tasks are not found.






<a name="peloton.api.v0.task.svc.RestartTasksRequest"/>

### RestartTasksRequest
Request message for TaskService.RestartTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to restart. |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.task.svc..peloton.api.v0.task.InstanceRange) | repeated | The instance ID ranges of the tasks to restart. |






<a name="peloton.api.v0.task.svc.RestartTasksResponse"/>

### RestartTasksResponse
Response message for TaskService.RestartTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found.
OUT_OF_RANGE:  if the instance IDs are out of range.
INTERNAL:      if the tasks fail to restart for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stopped | [uint32](#uint32) | repeated | The set of instance IDs that have been stopped. |
| failed | [uint32](#uint32) | repeated | The set of instance IDs that are failed to stop. |






<a name="peloton.api.v0.task.svc.StartTasksRequest"/>

### StartTasksRequest
Request message for TaskService.StartTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to start. |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.task.svc..peloton.api.v0.task.InstanceRange) | repeated | The instance ID ranges of the tasks to start. |






<a name="peloton.api.v0.task.svc.StartTasksResponse"/>

### StartTasksResponse
Response message for TaskService.StartTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found.
OUT_OF_RANGE:  if the instance IDs are out of range.
INTERNAL:      if the tasks fail to start for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| started | [uint32](#uint32) | repeated | The set of instance IDs that have been started. |
| failed | [uint32](#uint32) | repeated | The set of instance IDs that are failed to start. |






<a name="peloton.api.v0.task.svc.StopTasksRequest"/>

### StopTasksRequest
Request message for TaskService.StopTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.task.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the tasks to stop. |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.task.svc..peloton.api.v0.task.InstanceRange) | repeated | The instance ID ranges of the tasks to stop. If you dont specify any range it will signal all instance IDs to stop. |






<a name="peloton.api.v0.task.svc.StopTasksResponse"/>

### StopTasksResponse
Response message for TaskService.StopTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found in Peloton.
OUT_OF_RANGE:  if the instance IDs are out of range.
INTERNAL:      if the tasks fail to stop for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stopped | [uint32](#uint32) | repeated | The set of instance IDs that have been signalled to be stopped. These instanced IDs will be asynchronously stopped. |
| failed | [uint32](#uint32) | repeated | The set of instance IDs that have failed to be signalled successfuly. |






<a name="peloton.api.v0.task.svc.TaskEventsError"/>

### TaskEventsError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |





 

 

 


<a name="peloton.api.v0.task.svc.TaskService"/>

### TaskService
Task service defines the task related methods such as get, list,
start, stop and restart tasks.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetTask | [GetTaskRequest](#peloton.api.v0.task.svc.GetTaskRequest) | [GetTaskResponse](#peloton.api.v0.task.svc.GetTaskRequest) | Get the info of a task in job. |
| ListTasks | [ListTasksRequest](#peloton.api.v0.task.svc.ListTasksRequest) | [ListTasksResponse](#peloton.api.v0.task.svc.ListTasksRequest) | List a set of tasks in a job for a given range of instance IDs. |
| StartTasks | [StartTasksRequest](#peloton.api.v0.task.svc.StartTasksRequest) | [StartTasksResponse](#peloton.api.v0.task.svc.StartTasksRequest) | Start a set of tasks for a job. Will be no-op for tasks that are currently running. The tasks are started asynchronously after the API call returns. |
| StopTasks | [StopTasksRequest](#peloton.api.v0.task.svc.StopTasksRequest) | [StopTasksResponse](#peloton.api.v0.task.svc.StopTasksRequest) | Stop a set of tasks for a job. Will be no-op for tasks that are currently stopped. The tasks are stopped asynchronously after the API call returns. |
| RestartTasks | [RestartTasksRequest](#peloton.api.v0.task.svc.RestartTasksRequest) | [RestartTasksResponse](#peloton.api.v0.task.svc.RestartTasksRequest) | Restart a set of tasks for a job. Will start tasks that are currently stopped. This is an asynchronous call. |
| QueryTasks | [QueryTasksRequest](#peloton.api.v0.task.svc.QueryTasksRequest) | [QueryTasksResponse](#peloton.api.v0.task.svc.QueryTasksRequest) | Query task info in a job, using a set of filters. |
| BrowseSandbox | [BrowseSandboxRequest](#peloton.api.v0.task.svc.BrowseSandboxRequest) | [BrowseSandboxResponse](#peloton.api.v0.task.svc.BrowseSandboxRequest) | BrowseSandbox returns list of file paths inside sandbox. The client can use the Mesos Agent HTTP endpoints to read and download the files. http://mesos.apache.org/documentation/latest/endpoints |
| RefreshTasks | [RefreshTasksRequest](#peloton.api.v0.task.svc.RefreshTasksRequest) | [RefreshTasksResponse](#peloton.api.v0.task.svc.RefreshTasksRequest) | Debug only method. Allows user to load task runtime state from DB and re-execute the action associated with current state. |
| GetTaskCache | [GetTaskCacheRequest](#peloton.api.v0.task.svc.GetTaskCacheRequest) | [GetTaskCacheResponse](#peloton.api.v0.task.svc.GetTaskCacheRequest) | Debug only method. Get the cache of a task stored in Peloton. |
| GetPodEvents | [GetPodEventsRequest](#peloton.api.v0.task.svc.GetPodEventsRequest) | [GetPodEventsResponse](#peloton.api.v0.task.svc.GetPodEventsRequest) | GetPodEvents returns pod events (state transition for a pod), in reverse chronological order. pod is singular instance of a Peloton job. |

 



<a name="job_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## job_svc.proto



<a name="peloton.api.v0.job.svc.CreateJobRequest"/>

### CreateJobRequest
Request message for JobService.CreateJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The unique job UUID specified by the client. This can be used by the client to re-create a failed job without the side-effect of creating duplicated jobs. If unset, the server will create a new UUID for the job for each invocation. |
| pool | [.peloton.api.v0.respool.ResourcePoolPath](#peloton.api.v0.job.svc..peloton.api.v0.respool.ResourcePoolPath) |  | The resource pool under which the job should be created. The scheduling of all tasks in the job will be subject to the resource availablity of the resource pool. |
| config | [.peloton.api.v0.job.JobConfig](#peloton.api.v0.job.svc..peloton.api.v0.job.JobConfig) |  | The detailed configuration of the job to be created. |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job.svc..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job |






<a name="peloton.api.v0.job.svc.CreateJobResponse"/>

### CreateJobResponse
Response message for JobService.CreateJob method.

Return errors:
ALREADY_EXISTS:    if the job ID already exists.o
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID of the newly created job. Will be the same as the one in CreateJobRequest if provided. Otherwise, a new job ID will be generated by the server. |






<a name="peloton.api.v0.job.svc.DeleteJobRequest"/>

### DeleteJobRequest
Request message for JobService.DeleteJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID to be deleted. |






<a name="peloton.api.v0.job.svc.DeleteJobResponse"/>

### DeleteJobResponse
Response message for JobService.DeleteJob method.

Return errors:
NOT_FOUND:  if the job is not found in Peloton.






<a name="peloton.api.v0.job.svc.GetJobCacheRequest"/>

### GetJobCacheRequest
Request message for JobService.GetJobCache method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v0.job.svc.GetJobCacheResponse"/>

### GetJobCacheResponse
Response message for JobService.GetJobCache method.

Return errors:
NOT_FOUND:      if the job is not found in Peloton cache.
INTERNAL_ERROR: if fail to read cache due to internal error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [.peloton.api.v0.job.JobConfig](#peloton.api.v0.job.svc..peloton.api.v0.job.JobConfig) |  | The job configuration in cache of the matching job. |
| runtime | [.peloton.api.v0.job.RuntimeInfo](#peloton.api.v0.job.svc..peloton.api.v0.job.RuntimeInfo) |  | The job runtime in cache of the matching job. |






<a name="peloton.api.v0.job.svc.GetJobRequest"/>

### GetJobRequest
Request message for JobService.GetJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v0.job.svc.GetJobResponse"/>

### GetJobResponse
Response message for JobService.GetJob method.

Return errors:
NOT_FOUND:  if the job is not found in Peloton.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.v0.job.JobConfig](#peloton.api.v0.job.svc..peloton.api.v0.job.JobConfig) |  | The job configuration of the matching job. |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job.svc..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job, secret.Value will be empty. SecretID and path will be populated, so that caller can identify which secret is associated with this job. |






<a name="peloton.api.v0.job.svc.QueryJobsRequest"/>

### QueryJobsRequest
Request message for JobService.QueryJobs method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [.peloton.api.v0.job.QuerySpec](#peloton.api.v0.job.svc..peloton.api.v0.job.QuerySpec) |  | The spec of query criteria for the jobs. |
| pagination | [.peloton.api.v0.query.PaginationSpec](#peloton.api.v0.job.svc..peloton.api.v0.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |






<a name="peloton.api.v0.job.svc.QueryJobsResponse"/>

### QueryJobsResponse
Response message for JobService.QueryJobs method.

Return errors:
INVALID_ARGUMENT:  if the resource pool path or job states are invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [.peloton.api.v0.job.JobInfo](#peloton.api.v0.job.svc..peloton.api.v0.job.JobInfo) | repeated | List of jobs that match the job query criteria. |
| pagination | [.peloton.api.v0.query.Pagination](#peloton.api.v0.job.svc..peloton.api.v0.query.Pagination) |  | Pagination result of the job query. |
| spec | [.peloton.api.v0.job.QuerySpec](#peloton.api.v0.job.svc..peloton.api.v0.job.QuerySpec) |  | Return the spec of query criteria from the request. |






<a name="peloton.api.v0.job.svc.RefreshJobRequest"/>

### RefreshJobRequest
Request message for JobService.RefreshJob method.
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v0.job.svc.RefreshJobResponse"/>

### RefreshJobResponse
Response message for JobService.RefreshJob method.

Return errors:
NOT_FOUND:  if the job is not found in Peloton.






<a name="peloton.api.v0.job.svc.RestartConfig"/>

### RestartConfig
RestartConfig is the optional config for RestartJobRequest
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling restart, if unset or 0 all tasks specified will be restarted at the same time. |






<a name="peloton.api.v0.job.svc.RestartJobRequest"/>

### RestartJobRequest
Request message for JobService.RestartJob method.
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job to restart |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job.svc..peloton.api.v0.task.InstanceRange) | repeated | The instances to restart, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| restartConfig | [RestartConfig](#peloton.api.v0.job.svc.RestartConfig) |  | The config for restart a job |






<a name="peloton.api.v0.job.svc.RestartJobResponse"/>

### RestartJobResponse
Response message for JobService.RestartJob method.

Return errors:
INVALID_ARGUMENT:  if the job ID or job resourceVersion is invalid.
NOT_FOUND:         if the job ID is not found.

Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the restart |






<a name="peloton.api.v0.job.svc.StartConfig"/>

### StartConfig
StartConfig is the optional config for StartJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling start, if unset or 0 all tasks specified will be started at the same time. |






<a name="peloton.api.v0.job.svc.StartJobRequest"/>

### StartJobRequest
Request message for JobService.StartJob method.
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job to start |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job.svc..peloton.api.v0.task.InstanceRange) | repeated | The instances to start, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| startConfig | [StartConfig](#peloton.api.v0.job.svc.StartConfig) |  | The config for starting a job |






<a name="peloton.api.v0.job.svc.StartJobResponse"/>

### StartJobResponse
Response message for JobService.StartJob method.

Return errors:
INVALID_ARGUMENT:  if the job ID or job resourceVersion is invalid.
NOT_FOUND:         if the job ID is not found.

Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the start |






<a name="peloton.api.v0.job.svc.StopConfig"/>

### StopConfig
StopConfig is the optional config for StopJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [uint32](#uint32) |  | batch size of rolling stop, if unset or 0 all tasks specified will be stop at the same time. |






<a name="peloton.api.v0.job.svc.StopJobRequest"/>

### StopJobRequest
Request message for JobService.StopJobRequest method.
Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job to stop |
| ranges | [.peloton.api.v0.task.InstanceRange](#peloton.api.v0.job.svc..peloton.api.v0.task.InstanceRange) | repeated | The instances to stop, default to all |
| resourceVersion | [uint64](#uint64) |  | The resourceVersion received from last job operation call for concurrency control |
| stopConfig | [StopConfig](#peloton.api.v0.job.svc.StopConfig) |  | The config for stopping a job |






<a name="peloton.api.v0.job.svc.StopJobResponse"/>

### StopJobResponse
Response message for JobService.StopJob method.

Return errors:
INVALID_ARGUMENT:  if the job ID or job resourceVersion is invalid.
NOT_FOUND:         if the job ID is not found.

Experimental only


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceVersion | [uint64](#uint64) |  | The new resourceVersion after the operation |
| updateID | [.peloton.api.v0.peloton.UpdateID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.UpdateID) |  | updateID associated with the stop |






<a name="peloton.api.v0.job.svc.UpdateJobRequest"/>

### UpdateJobRequest
Request message for JobService.UpdateJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.job.svc..peloton.api.v0.peloton.JobID) |  | The job ID to be updated. |
| config | [.peloton.api.v0.job.JobConfig](#peloton.api.v0.job.svc..peloton.api.v0.job.JobConfig) |  | The new job config to be applied to the job. |
| secrets | [.peloton.api.v0.peloton.Secret](#peloton.api.v0.job.svc..peloton.api.v0.peloton.Secret) | repeated | The list of secrets for this job |






<a name="peloton.api.v0.job.svc.UpdateJobResponse"/>

### UpdateJobResponse
Response message for JobService.UpdateJob method.

Return errors:
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the job ID is not found.





 

 

 


<a name="peloton.api.v0.job.svc.JobService"/>

### JobService
Job service defines the job related methods such as create, get,
query and kill jobs.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateJob | [CreateJobRequest](#peloton.api.v0.job.svc.CreateJobRequest) | [CreateJobResponse](#peloton.api.v0.job.svc.CreateJobRequest) | Create a job entity for a given config. |
| GetJob | [GetJobRequest](#peloton.api.v0.job.svc.GetJobRequest) | [GetJobResponse](#peloton.api.v0.job.svc.GetJobRequest) | Get the config of a job entity. |
| QueryJobs | [QueryJobsRequest](#peloton.api.v0.job.svc.QueryJobsRequest) | [QueryJobsResponse](#peloton.api.v0.job.svc.QueryJobsRequest) | Query the jobs that match a list of labels. |
| DeleteJob | [DeleteJobRequest](#peloton.api.v0.job.svc.DeleteJobRequest) | [DeleteJobResponse](#peloton.api.v0.job.svc.DeleteJobRequest) | Delete a job and stop all related tasks. |
| UpdateJob | [UpdateJobRequest](#peloton.api.v0.job.svc.UpdateJobRequest) | [UpdateJobResponse](#peloton.api.v0.job.svc.UpdateJobRequest) | Update a job entity with a new config. This is a temporary API for updating batch jobs. It only supports adding new instances to an existing job. It will be deprecated when the UpgradeService API is implemented. |
| RestartJob | [RestartJobRequest](#peloton.api.v0.job.svc.RestartJobRequest) | [RestartJobResponse](#peloton.api.v0.job.svc.RestartJobRequest) | Restart the tasks specified in restart request Experimental only |
| StartJob | [StartJobRequest](#peloton.api.v0.job.svc.StartJobRequest) | [StartJobResponse](#peloton.api.v0.job.svc.StartJobRequest) | StartJob the tasks specified in start request Experimental only |
| StopJob | [StopJobRequest](#peloton.api.v0.job.svc.StopJobRequest) | [StopJobResponse](#peloton.api.v0.job.svc.StopJobRequest) | Stop the tasks specified in stop request Experimental only |
| RefreshJob | [RefreshJobRequest](#peloton.api.v0.job.svc.RefreshJobRequest) | [RefreshJobResponse](#peloton.api.v0.job.svc.RefreshJobRequest) | Debug only method. Allows user to load job runtime state from DB and re-execute the action associated with current state. |
| GetJobCache | [GetJobCacheRequest](#peloton.api.v0.job.svc.GetJobCacheRequest) | [GetJobCacheResponse](#peloton.api.v0.job.svc.GetJobCacheRequest) | Debug only method. Get the cache of a job stored in Peloton. |

 



<a name="volume_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume_svc.proto



<a name="peloton.api.v0.volume.svc.DeleteVolumeRequest"/>

### DeleteVolumeRequest
Request message for VolumeService.Delete method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.VolumeID](#peloton.api.v0.volume.svc..peloton.api.v0.peloton.VolumeID) |  | volume id for the delete request. |






<a name="peloton.api.v0.volume.svc.DeleteVolumeResponse"/>

### DeleteVolumeResponse
Response message for VolumeService.Delete method.

Return errors:
NOT_FOUND:         if the volume is not found.






<a name="peloton.api.v0.volume.svc.GetVolumeRequest"/>

### GetVolumeRequest
Request message for VolumeService.Get method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.v0.peloton.VolumeID](#peloton.api.v0.volume.svc..peloton.api.v0.peloton.VolumeID) |  | the volume id. |






<a name="peloton.api.v0.volume.svc.GetVolumeResponse"/>

### GetVolumeResponse
Response message for VolumeService.Get method.

Return errors:
NOT_FOUND:         if the volume is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.v0.volume.PersistentVolumeInfo](#peloton.api.v0.volume.svc..peloton.api.v0.volume.PersistentVolumeInfo) |  | volume info result. |






<a name="peloton.api.v0.volume.svc.ListVolumesRequest"/>

### ListVolumesRequest
Request message for VolumeService.List method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.v0.peloton.JobID](#peloton.api.v0.volume.svc..peloton.api.v0.peloton.JobID) |  | job ID for the volumes. |






<a name="peloton.api.v0.volume.svc.ListVolumesResponse"/>

### ListVolumesResponse
Response message for VolumeService.List method.

Return errors:
NOT_FOUND:         if the volume is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [ListVolumesResponse.VolumesEntry](#peloton.api.v0.volume.svc.ListVolumesResponse.VolumesEntry) | repeated | volumes result map from volume uuid to volume info. |






<a name="peloton.api.v0.volume.svc.ListVolumesResponse.VolumesEntry"/>

### ListVolumesResponse.VolumesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [.peloton.api.v0.volume.PersistentVolumeInfo](#peloton.api.v0.volume.svc..peloton.api.v0.volume.PersistentVolumeInfo) |  |  |





 

 

 


<a name="peloton.api.v0.volume.svc.VolumeService"/>

### VolumeService
Volume Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListVolumes | [ListVolumesRequest](#peloton.api.v0.volume.svc.ListVolumesRequest) | [ListVolumesResponse](#peloton.api.v0.volume.svc.ListVolumesRequest) | List associated volumes for given job. |
| GetVolume | [GetVolumeRequest](#peloton.api.v0.volume.svc.GetVolumeRequest) | [GetVolumeResponse](#peloton.api.v0.volume.svc.GetVolumeRequest) | Get volume data. |
| DeleteVolume | [DeleteVolumeRequest](#peloton.api.v0.volume.svc.DeleteVolumeRequest) | [DeleteVolumeResponse](#peloton.api.v0.volume.svc.DeleteVolumeRequest) | Delete a persistent volume. |

 



<a name="pod_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## pod_svc.proto



<a name="peloton.api.v1alpha.pod.svc.BrowsePodSandboxRequest"/>

### BrowsePodSandboxRequest
Request message for PodService.BrowsePodSandbox method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |
| pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodID) |  | Get the sandbox path of a particular pod identified using the pod identifier. If not provided, the sandbox path for the latest pod is returned. |






<a name="peloton.api.v1alpha.pod.svc.BrowsePodSandboxResponse"/>

### BrowsePodSandboxResponse
Response message for PodService.BrowsePodSandbox method
Return errors:
NOT_FOUND:   if the pod is not found.
ABORT:       if the pod has not been run.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  | The hostname of the sandbox. |
| port | [string](#string) |  | The port of the sandbox. |
| paths | [string](#string) | repeated | The list of sandbox file paths. TODO: distinguish files and directories in the sandbox |
| mesos_master_hostname | [string](#string) |  | Mesos Master hostname and port. |
| mesos_master_port | [string](#string) |  |  |






<a name="peloton.api.v1alpha.pod.svc.DeletePodEventsRequest"/>

### DeletePodEventsRequest
Request message for PodService.DeletePodEvents method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |
| pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodID) |  | Delete the events of a particular pod identified using the pod identifier. |






<a name="peloton.api.v1alpha.pod.svc.DeletePodEventsResponse"/>

### DeletePodEventsResponse
Response message for PodService.DeletePodEvents method
Return errors:
NOT_FOUND:   if the pod is not found.






<a name="peloton.api.v1alpha.pod.svc.GetPodCacheRequest"/>

### GetPodCacheRequest
Request message for PodService.GetPodCache method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |






<a name="peloton.api.v1alpha.pod.svc.GetPodCacheResponse"/>

### GetPodCacheResponse
Response message for PodService.GetPodCache method
Return errors:
NOT_FOUND:   if the pod is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [.peloton.api.v1alpha.pod.PodStatus](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.pod.PodStatus) |  | The runtime status of the pod. |






<a name="peloton.api.v1alpha.pod.svc.GetPodEventsRequest"/>

### GetPodEventsRequest
Request message for PodService.GetPodEvents method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |
| pod_id | [.peloton.api.v1alpha.peloton.PodID](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodID) |  | Get the events of a particular pod identified using the pod identifier. If not provided, events for the latest pod are returned. |






<a name="peloton.api.v1alpha.pod.svc.GetPodEventsResponse"/>

### GetPodEventsResponse
Response message for PodService.GetPodEvents method
Return errors:
NOT_FOUND:   if the pod is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [.peloton.api.v1alpha.pod.PodEvent](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.pod.PodEvent) | repeated |  |






<a name="peloton.api.v1alpha.pod.svc.GetPodRequest"/>

### GetPodRequest
Request message for PodService.GetPod method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |
| status_only | [bool](#bool) |  | If set to true, only return the pod status and not the configuration. |






<a name="peloton.api.v1alpha.pod.svc.GetPodResponse"/>

### GetPodResponse
Response message for PodService.GetPod method
Return errors:
NOT_FOUND:   if the pod is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| current | [.peloton.api.v1alpha.pod.PodInfo](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.pod.PodInfo) |  | Returns the status and configuration (if requested) for the current run of the pod. |
| previous | [.peloton.api.v1alpha.pod.PodInfo](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.pod.PodInfo) | repeated | Returns the status and configuration (if requested) for previous runs of the pod. |






<a name="peloton.api.v1alpha.pod.svc.RefreshPodRequest"/>

### RefreshPodRequest
Request message for PodService.RefreshPod method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |






<a name="peloton.api.v1alpha.pod.svc.RefreshPodResponse"/>

### RefreshPodResponse
Response message for PodService.RefreshPod method
Return errors:
NOT_FOUND:   if the pod is not found.






<a name="peloton.api.v1alpha.pod.svc.RestartPodRequest"/>

### RestartPodRequest
Request message for PodService.RestartPod method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |






<a name="peloton.api.v1alpha.pod.svc.RestartPodResponse"/>

### RestartPodResponse
Response message for PodService.RestartPod method
Return errors:
NOT_FOUND:   if the pod is not found.






<a name="peloton.api.v1alpha.pod.svc.StartPodRequest"/>

### StartPodRequest
Request message for PodService.StartPod method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |






<a name="peloton.api.v1alpha.pod.svc.StartPodResponse"/>

### StartPodResponse
Response message for PodService.StartPod method
Return errors:
NOT_FOUND:   if the pod is not found.






<a name="peloton.api.v1alpha.pod.svc.StopPodRequest"/>

### StopPodRequest
Request message for PodService.StopPod method


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pod_name | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.pod.svc..peloton.api.v1alpha.peloton.PodName) |  | The pod name. |






<a name="peloton.api.v1alpha.pod.svc.StopPodResponse"/>

### StopPodResponse
Response message for PodService.StopPod method
Return errors:
NOT_FOUND:   if the pod is not found.





 

 

 


<a name="peloton.api.v1alpha.pod.svc.PodService"/>

### PodService
Pod service defines the pod related methods.


Methods which mutate the state of the pod.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| StartPod | [StartPodRequest](#peloton.api.v1alpha.pod.svc.StartPodRequest) | [StartPodResponse](#peloton.api.v1alpha.pod.svc.StartPodRequest) | Start the pod. Will be a no-op for pod that is currently running. The pod is started asynchronously after the API call returns. |
| StopPod | [StopPodRequest](#peloton.api.v1alpha.pod.svc.StopPodRequest) | [StopPodResponse](#peloton.api.v1alpha.pod.svc.StopPodRequest) | Stop the pod. Will be no-op for a pod that is currently stopped. The pod is stopped asynchronously after the API call returns. |
| RestartPod | [RestartPodRequest](#peloton.api.v1alpha.pod.svc.RestartPodRequest) | [RestartPodResponse](#peloton.api.v1alpha.pod.svc.RestartPodRequest) | Restart a the pod. Will start a pod that is currently stopped. Will first stop the pod that is currently running and then start it again. This is an asynchronous call. |
| GetPod | [GetPodRequest](#peloton.api.v1alpha.pod.svc.GetPodRequest) | [GetPodResponse](#peloton.api.v1alpha.pod.svc.GetPodRequest) | Get the info of a pod in a job. Return the current run as well as the terminal state of previous runs. |
| GetPodEvents | [GetPodEventsRequest](#peloton.api.v1alpha.pod.svc.GetPodEventsRequest) | [GetPodEventsResponse](#peloton.api.v1alpha.pod.svc.GetPodEventsRequest) | Get the state transitions for a pod (pod events) for a given run of the pod. |
| BrowsePodSandbox | [BrowsePodSandboxRequest](#peloton.api.v1alpha.pod.svc.BrowsePodSandboxRequest) | [BrowsePodSandboxResponse](#peloton.api.v1alpha.pod.svc.BrowsePodSandboxRequest) | Return the list of file paths inside the sandbox for a given run of a pod. The client can use the Mesos Agent HTTP endpoints to read and download the files. http://mesos.apache.org/documentation/latest/endpoints |
| RefreshPod | [RefreshPodRequest](#peloton.api.v1alpha.pod.svc.RefreshPodRequest) | [RefreshPodResponse](#peloton.api.v1alpha.pod.svc.RefreshPodRequest) | Allows user to load pod runtime state from DB and re-execute the action associated with current state. |
| GetPodCache | [GetPodCacheRequest](#peloton.api.v1alpha.pod.svc.GetPodCacheRequest) | [GetPodCacheResponse](#peloton.api.v1alpha.pod.svc.GetPodCacheRequest) | Get the cache of a pod stored in Peloton. |
| DeletePodEvents | [DeletePodEventsRequest](#peloton.api.v1alpha.pod.svc.DeletePodEventsRequest) | [DeletePodEventsResponse](#peloton.api.v1alpha.pod.svc.DeletePodEventsRequest) | Delete the events of a given run of a pod. This is used to prevent the events for a given pod from growing without bounds. |

 



<a name="host_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## host_svc.proto



<a name="peloton.api.v1alpha.host.svc.CompleteMaintenanceRequest"/>

### CompleteMaintenanceRequest
Request message for HostService.CompleteMaintenance method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostnames | [string](#string) | repeated | List of hosts put be brought back up |






<a name="peloton.api.v1alpha.host.svc.CompleteMaintenanceResponse"/>

### CompleteMaintenanceResponse
Response message for HostService.CompleteMaintenance method.
Return errors:
NOT_FOUND:   if the hosts are not found.






<a name="peloton.api.v1alpha.host.svc.QueryHostsRequest"/>

### QueryHostsRequest
Request message for HostService.QueryHosts method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_states | [.peloton.api.v1alpha.host.HostState](#peloton.api.v1alpha.host.svc..peloton.api.v1alpha.host.HostState) | repeated | List of host states to query the hosts. Will return all hosts if the list is empty. |






<a name="peloton.api.v1alpha.host.svc.QueryHostsResponse"/>

### QueryHostsResponse
Response message for HostService.QueryHosts method.
Return errors:


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_infos | [.peloton.api.v1alpha.host.HostInfo](#peloton.api.v1alpha.host.svc..peloton.api.v1alpha.host.HostInfo) | repeated | List of hosts that match the host query criteria. |






<a name="peloton.api.v1alpha.host.svc.StartMaintenanceRequest"/>

### StartMaintenanceRequest
Request message for HostService.StartMaintenance method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostnames | [string](#string) | repeated | List of hosts to be put into maintenance |






<a name="peloton.api.v1alpha.host.svc.StartMaintenanceResponse"/>

### StartMaintenanceResponse
Response message for HostService.StartMaintenance method.
Return errors:
NOT_FOUND:   if the hosts are not found.





 

 

 


<a name="peloton.api.v1alpha.host.svc.HostService"/>

### HostService
HostService defines the host related methods such as query hosts, start maintenance,
complete maintenance etc.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| QueryHosts | [QueryHostsRequest](#peloton.api.v1alpha.host.svc.QueryHostsRequest) | [QueryHostsResponse](#peloton.api.v1alpha.host.svc.QueryHostsRequest) | Get hosts which are in one of the specified states |
| StartMaintenance | [StartMaintenanceRequest](#peloton.api.v1alpha.host.svc.StartMaintenanceRequest) | [StartMaintenanceResponse](#peloton.api.v1alpha.host.svc.StartMaintenanceRequest) | Start maintenance on the specified hosts |
| CompleteMaintenance | [CompleteMaintenanceRequest](#peloton.api.v1alpha.host.svc.CompleteMaintenanceRequest) | [CompleteMaintenanceResponse](#peloton.api.v1alpha.host.svc.CompleteMaintenanceRequest) | Complete maintenance on the specified hosts |

 



<a name="respool_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool_svc.proto



<a name="peloton.api.v1alpha.respool.CreateResourcePoolRequest"/>

### CreateResourcePoolRequest
Request message for ResourcePoolService.CreateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The unique resource pool UUID specified by the client. This can be used by the client to re-create a failed resource pool without the side-effect of creating duplicated resource pool. If unset, the server will create a new UUID for the resource pool. |
| spec | [ResourcePoolSpec](#peloton.api.v1alpha.respool.ResourcePoolSpec) |  | The detailed configuration of the resource pool be to created. |






<a name="peloton.api.v1alpha.respool.CreateResourcePoolResponse"/>

### CreateResourcePoolResponse
Response message for ResourcePoolService.CreateResourcePool method.
Return errors:
ALREADY_EXISTS:   if the resource pool already exists.
INVALID_ARGUMENT: if the resource pool config is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The ID of the newly created resource pool. |






<a name="peloton.api.v1alpha.respool.DeleteResourcePoolRequest"/>

### DeleteResourcePoolRequest
Request message for ResourcePoolService.DeleteResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The ID of the resource pool to be deleted. |






<a name="peloton.api.v1alpha.respool.DeleteResourcePoolResponse"/>

### DeleteResourcePoolResponse
Response message for ResourcePoolService.DeleteResourcePool method.
Return errors:
NOT_FOUND:           if the resource pool is not found.
INVALID_ARGUMENT:    if the resource pool is not leaf node.
FAILED_PRECONDITION: if the resource pool is busy.
INTERNAL:            if the resource pool fail to delete for internal errors.






<a name="peloton.api.v1alpha.respool.GetResourcePoolRequest"/>

### GetResourcePoolRequest
Request message for ResourcePoolService.GetResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The ID of the resource pool to get the detailed information. |
| include_child_pools | [bool](#bool) |  | Whether or not to include the resource pool info of the direct children |






<a name="peloton.api.v1alpha.respool.GetResourcePoolResponse"/>

### GetResourcePoolResponse
Response message for ResourcePoolService.GetResourcePool method.
Return errors:
NOT_FOUND:   if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool | [ResourcePoolInfo](#peloton.api.v1alpha.respool.ResourcePoolInfo) |  | The detailed information of the resource pool. |
| child_respools | [ResourcePoolInfo](#peloton.api.v1alpha.respool.ResourcePoolInfo) | repeated | The list of child resource pools. |






<a name="peloton.api.v1alpha.respool.LookupResourcePoolIDRequest"/>

### LookupResourcePoolIDRequest
Request message for ResourcePoolService.LookupResourcePoolID method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.v1alpha.respool.ResourcePoolPath) |  | The resource pool path to look up the resource pool ID. |






<a name="peloton.api.v1alpha.respool.LookupResourcePoolIDResponse"/>

### LookupResourcePoolIDResponse
Response message for ResourcePoolService.LookupResourcePoolID method.
Response message for ResourcePoolService.UpdateResourcePool method.
Return errors:
NOT_FOUND:         if the resource pool is not found.
INVALID_ARGUMENT:  if the resource pool path is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The resource pool ID for the given resource pool path. |






<a name="peloton.api.v1alpha.respool.QueryResourcePoolsRequest"/>

### QueryResourcePoolsRequest
Request message for ResourcePoolService.QueryResourcePools method.


TODO Filters






<a name="peloton.api.v1alpha.respool.QueryResourcePoolsResponse"/>

### QueryResourcePoolsResponse
Response message for ResourcePoolService.QueryResourcePools method.
Return errors:


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respools | [ResourcePoolInfo](#peloton.api.v1alpha.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.v1alpha.respool.UpdateResourcePoolRequest"/>

### UpdateResourcePoolRequest
Request message for ResourcePoolService.UpdateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.respool..peloton.api.v1alpha.peloton.ResourcePoolID) |  | The ID of the resource pool to update the configuration. |
| spec | [ResourcePoolSpec](#peloton.api.v1alpha.respool.ResourcePoolSpec) |  | The configuration of the resource pool to be updated. |






<a name="peloton.api.v1alpha.respool.UpdateResourcePoolResponse"/>

### UpdateResourcePoolResponse
Response message for ResourcePoolService.UpdateResourcePool method.
Return errors:
NOT_FOUND:   if the resource pool is not found.





 

 

 


<a name="peloton.api.v1alpha.respool.ResourcePoolService"/>

### ResourcePoolService
ResourcePoolService defines the resource pool related methods
such as create, get, delete and upgrade resource pools.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateResourcePool | [CreateResourcePoolRequest](#peloton.api.v1alpha.respool.CreateResourcePoolRequest) | [CreateResourcePoolResponse](#peloton.api.v1alpha.respool.CreateResourcePoolRequest) | Create a resource pool entity for a given config |
| GetResourcePool | [GetResourcePoolRequest](#peloton.api.v1alpha.respool.GetResourcePoolRequest) | [GetResourcePoolResponse](#peloton.api.v1alpha.respool.GetResourcePoolRequest) | Get the resource pool entity |
| DeleteResourcePool | [DeleteResourcePoolRequest](#peloton.api.v1alpha.respool.DeleteResourcePoolRequest) | [DeleteResourcePoolResponse](#peloton.api.v1alpha.respool.DeleteResourcePoolRequest) | Delete a resource pool entity |
| UpdateResourcePool | [UpdateResourcePoolRequest](#peloton.api.v1alpha.respool.UpdateResourcePoolRequest) | [UpdateResourcePoolResponse](#peloton.api.v1alpha.respool.UpdateResourcePoolRequest) | Modify a resource pool entity |
| LookupResourcePoolID | [LookupResourcePoolIDRequest](#peloton.api.v1alpha.respool.LookupResourcePoolIDRequest) | [LookupResourcePoolIDResponse](#peloton.api.v1alpha.respool.LookupResourcePoolIDRequest) | Lookup the resource pool ID for a given resource pool path |
| QueryResourcePools | [QueryResourcePoolsRequest](#peloton.api.v1alpha.respool.QueryResourcePoolsRequest) | [QueryResourcePoolsResponse](#peloton.api.v1alpha.respool.QueryResourcePoolsRequest) | Query the resource pools. |

 



<a name="stateless.proto"/>
<p align="right"><a href="#top">Top</a></p>

## stateless.proto



<a name="peloton.api.v1alpha.job.stateless.CreateSpec"/>

### CreateSpec
Configuration of a job creation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batch_size | [uint32](#uint32) |  | Batch size for the creation which controls how many instances may be created at the same time. |
| max_instance_retries | [uint32](#uint32) |  | Maximum number of times a failing instance will be retried during the creation. If the value is 0, the instance can be retried for infinite times. |
| max_tolerable_instance_failures | [uint32](#uint32) |  | Maximum number of instance failures before the creation is declared to be failed. If the value is 0, there is no limit for max failure instances and the creation is marked successful even if all of the instances fail. |
| start_paused | [bool](#bool) |  | If set to true, indicates that the creation should start in the paused state, requiring an explicit resume to roll forward. |






<a name="peloton.api.v1alpha.job.stateless.JobInfo"/>

### JobInfo
Information of a job, such as job spec and status


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.JobID) |  | Job ID |
| spec | [JobSpec](#peloton.api.v1alpha.job.stateless.JobSpec) |  | Job configuration |
| status | [JobStatus](#peloton.api.v1alpha.job.stateless.JobStatus) |  | Job runtime status |






<a name="peloton.api.v1alpha.job.stateless.JobSpec"/>

### JobSpec
Stateless job configuration.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revision | [.peloton.api.v1alpha.peloton.Revision](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.Revision) |  | Revision of the job config |
| name | [string](#string) |  | Name of the job |
| owner | [string](#string) |  | Owner of the job |
| owning_team | [string](#string) |  | Owning team of the job |
| ldap_groups | [string](#string) | repeated | LDAP groups of the job |
| description | [string](#string) |  | Description of the job |
| labels | [.peloton.api.v1alpha.peloton.Label](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.Label) | repeated | List of user-defined labels for the job |
| instance_count | [uint32](#uint32) |  | Number of instances of the job |
| sla | [SlaSpec](#peloton.api.v1alpha.job.stateless.SlaSpec) |  | SLA config of the job |
| default_spec | [.peloton.api.v1alpha.pod.PodSpec](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.PodSpec) |  | Default pod configuration of the job |
| instance_spec | [JobSpec.InstanceSpecEntry](#peloton.api.v1alpha.job.stateless.JobSpec.InstanceSpecEntry) | repeated | Instance specific pod config which overwrites the default one |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.ResourcePoolID) |  | Resource Pool ID where this job belongs to |






<a name="peloton.api.v1alpha.job.stateless.JobSpec.InstanceSpecEntry"/>

### JobSpec.InstanceSpecEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [.peloton.api.v1alpha.pod.PodSpec](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.PodSpec) |  |  |






<a name="peloton.api.v1alpha.job.stateless.JobStatus"/>

### JobStatus
The current runtime status of a Job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revision | [.peloton.api.v1alpha.peloton.Revision](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.Revision) |  | Revision of the current job status. Version in the revision is incremented every time job status changes. Thus, it can be used to order the different job status updates. |
| state | [JobState](#peloton.api.v1alpha.job.stateless.JobState) |  | State of the job |
| creation_time | [string](#string) |  | The time when the job was created. The time is represented in RFC3339 form with UTC timezone. |
| pod_stats | [JobStatus.PodStatsEntry](#peloton.api.v1alpha.job.stateless.JobStatus.PodStatsEntry) | repeated | The number of pods grouped by each pod state. The map key is the pod.PodState in string format and the map value is the number of tasks in the particular state. |
| desired_state | [JobState](#peloton.api.v1alpha.job.stateless.JobState) |  | Goal state of the job. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control for all job write APIs. The current job configuration can be fetched based on the current resource version. |
| workflow_status | [WorkflowStatus](#peloton.api.v1alpha.job.stateless.WorkflowStatus) |  | Status of ongoing update/restart workflow. |
| pod_configuration_version_stats | [JobStatus.PodConfigurationVersionStatsEntry](#peloton.api.v1alpha.job.stateless.JobStatus.PodConfigurationVersionStatsEntry) | repeated | The number of tasks grouped by which configuration version they are on. The map key is the job configuration version and the map value is the number of tasks using that particular job configuration version. The job configuration version in the map key can be fed as the value of the entity version in the GetJobRequest to fetch the job configuration. |






<a name="peloton.api.v1alpha.job.stateless.JobStatus.PodConfigurationVersionStatsEntry"/>

### JobStatus.PodConfigurationVersionStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v1alpha.job.stateless.JobStatus.PodStatsEntry"/>

### JobStatus.PodStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.v1alpha.job.stateless.JobSummary"/>

### JobSummary
Summary of job spec and status. The summary will be returned by List
or Query API calls. These calls will return a large number of jobs,
so the content in the job summary has to be kept minimal.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.JobID) |  | Job ID |
| name | [string](#string) |  | Name of the job |
| owner | [string](#string) |  | Owner of the job |
| owning_team | [string](#string) |  | Owning team of the job |
| labels | [.peloton.api.v1alpha.peloton.Label](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.Label) | repeated | List of user-defined labels for the job |
| instance_count | [uint32](#uint32) |  | Number of instances of the job |
| respool_id | [.peloton.api.v1alpha.peloton.ResourcePoolID](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.ResourcePoolID) |  | Resource Pool ID where this job belongs to |
| status | [JobStatus](#peloton.api.v1alpha.job.stateless.JobStatus) |  | Job runtime status |






<a name="peloton.api.v1alpha.job.stateless.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for jobs. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.v1alpha.query.PaginationSpec](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |
| labels | [.peloton.api.v1alpha.peloton.Label](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.Label) | repeated | List of labels to query the jobs. Will match all jobs if the list is empty. |
| keywords | [string](#string) | repeated | List of keywords to query the jobs. Will match all jobs if the list is empty. When set, will do a wildcard match on owner, name, labels, description. |
| job_states | [JobState](#peloton.api.v1alpha.job.stateless.JobState) | repeated | List of job states to query the jobs. Will match all jobs if the list is empty. |
| respool | [.peloton.api.v1alpha.respool.ResourcePoolPath](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.respool.ResourcePoolPath) |  | The resource pool to query the jobs. Will match jobs from all resource pools if unset. |
| owner | [string](#string) |  | Query jobs by owner. This is case sensitive and will look for jobs with owner matching the exact owner string. Will match all jobs if owner is unset. |
| name | [string](#string) |  | Query jobs by name. This is case sensitive and will look for jobs with name matching the name string. Will support partial name match. Will match all jobs if name is unset. |
| creation_time_range | [.peloton.api.v1alpha.peloton.TimeRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.TimeRange) |  | Query jobs by creation time range. This will look for all jobs that were created within a specified time range. This search will operate based on job creation time. |
| completion_time_range | [.peloton.api.v1alpha.peloton.TimeRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.TimeRange) |  | Query jobs by completion time range. This will look for all jobs that were completed within a specified time range. This search will operate based on job completion time. |






<a name="peloton.api.v1alpha.job.stateless.SlaSpec"/>

### SlaSpec
SLA configuration for a stateless job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| priority | [uint32](#uint32) |  | Priority of a job. Higher value takes priority over lower value when making scheduling decisions as well as preemption decisions. |
| preemptible | [bool](#bool) |  | Whether all the job instances are preemptible. If so, it might be scheduled elastic resources from other resource pools and subject to preemption when the demands of other resource pools increase. For stateless jobs, this field will overrule preemptible configuration in the pod spec. |
| revocable | [bool](#bool) |  | Whether all the job instances are revocable. If so, it might be scheduled using revocable resources and subject to preemption when there is resource contention on the host. For stateless jobs, this field will overrule revocable configuration in the pod spec. |
| maximum_unavailable_instances | [uint32](#uint32) |  | Maximum number of job instances which can be unavailable at a given time. |






<a name="peloton.api.v1alpha.job.stateless.UpdateSpec"/>

### UpdateSpec
Configuration of a job update.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batch_size | [uint32](#uint32) |  | Batch size for the update which controls how many instances may be updated at the same time. |
| rollback_on_failure | [bool](#bool) |  | If configured, the update be automatically rolled back to the previous job configuration on failure. |
| max_instance_retries | [uint32](#uint32) |  | Maximum number of times a failing instance will be retried during the update. If the value is 0, the instance can be retried for infinite times. |
| max_tolerable_instance_failures | [uint32](#uint32) |  | Maximum number of instance failures before the update is declared to be failed. If the value is 0, there is no limit for max failure instances and the update is marked successful even if all of the instances fail. |
| start_paused | [bool](#bool) |  | If set to true, indicates that the update should start in the paused state, requiring an explicit resume to roll forward. |
| in_place | [bool](#bool) |  | If set to true, peloton would try to place the task restarted/updated on the host it previously run on. It is best effort, and has no guarantee of success. |






<a name="peloton.api.v1alpha.job.stateless.WorkflowEvent"/>

### WorkflowEvent
WorkflowEvents are workflow state change events for a job or pod
on workflow operations


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [WorkflowType](#peloton.api.v1alpha.job.stateless.WorkflowType) |  | Workflow type. |
| timestamp | [string](#string) |  | Timestamp of the event represented in RFC3339 form with UTC timezone. |
| state | [WorkflowState](#peloton.api.v1alpha.job.stateless.WorkflowState) |  | Current runtime state of the workflow. |






<a name="peloton.api.v1alpha.job.stateless.WorkflowInfo"/>

### WorkflowInfo
Information about a workflow including its status and specification


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [WorkflowStatus](#peloton.api.v1alpha.job.stateless.WorkflowStatus) |  | Workflow status |
| update_spec | [UpdateSpec](#peloton.api.v1alpha.job.stateless.UpdateSpec) |  | Update specification for update workflow |
| restart_batch_size | [uint32](#uint32) |  | Batch size provided for restart workflow |
| restart_ranges | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instance ranges provided for restart workflow |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |
| events | [WorkflowEvent](#peloton.api.v1alpha.job.stateless.WorkflowEvent) | repeated | job workflow events represents update state changes |
| instances_added | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances added by update workflow |
| instances_removed | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances removed by update workflow |
| instances_updated | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances updated by update workflow |






<a name="peloton.api.v1alpha.job.stateless.WorkflowStatus"/>

### WorkflowStatus
Runtime status of a job workflow.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [WorkflowType](#peloton.api.v1alpha.job.stateless.WorkflowType) |  | Workflow type. |
| state | [WorkflowState](#peloton.api.v1alpha.job.stateless.WorkflowState) |  | Current runtime state of the workflow. |
| num_instances_completed | [uint32](#uint32) |  | Number of instances completed. |
| num_instances_remaining | [uint32](#uint32) |  | Number of instances remaining. |
| num_instances_failed | [uint32](#uint32) |  | Number of instances which failed to come up after the workflow. |
| instances_current | [uint32](#uint32) | repeated | Current instances being operated on. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.EntityVersion) |  | Job version the workflow moved the job object to. |
| prev_version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless..peloton.api.v1alpha.peloton.EntityVersion) |  | Previous job version of the job object. |
| creation_time | [string](#string) |  | The time when the workflow was created. The time is represented in RFC3339 form with UTC timezone. |
| update_time | [string](#string) |  | The time when the workflow was last updated. The time is represented in RFC3339 form with UTC timezone. |
| prev_state | [WorkflowState](#peloton.api.v1alpha.job.stateless.WorkflowState) |  | Previous runtime state of the workflow. |





 


<a name="peloton.api.v1alpha.job.stateless.JobState"/>

### JobState
Runtime states of a Job.

| Name | Number | Description |
| ---- | ------ | ----------- |
| JOB_STATE_INVALID | 0 | Invalid job state. |
| JOB_STATE_INITIALIZED | 1 | The job has been initialized and persisted in DB. |
| JOB_STATE_PENDING | 2 | All tasks have been created and persisted in DB, but no task is RUNNING yet. |
| JOB_STATE_RUNNING | 3 | Any of the tasks in the job is in RUNNING state. |
| JOB_STATE_SUCCEEDED | 4 | All tasks in the job are in SUCCEEDED state. |
| JOB_STATE_FAILED | 5 | All tasks in the job are in terminated state and one or more tasks is in FAILED state. |
| JOB_STATE_KILLED | 6 | All tasks in the job are in terminated state and one or more tasks in the job is killed by the user. |
| JOB_STATE_KILLING | 7 | All tasks in the job have been requested to be killed by the user. |
| JOB_STATE_UNINITIALIZED | 8 | The job is partially created and is not ready to be scheduled |
| JOB_STATE_DELETED | 9 | The job has been deleted. |



<a name="peloton.api.v1alpha.job.stateless.WorkflowState"/>

### WorkflowState
Runtime state of a job workflow.

| Name | Number | Description |
| ---- | ------ | ----------- |
| WORKFLOW_STATE_INVALID | 0 | Invalid protobuf value |
| WORKFLOW_STATE_INITIALIZED | 1 | The operation has been created but not started yet. |
| WORKFLOW_STATE_ROLLING_FORWARD | 2 | The workflow is rolling forward |
| WORKFLOW_STATE_PAUSED | 3 | The workflow has been paused |
| WORKFLOW_STATE_SUCCEEDED | 4 | The workflow has completed successfully |
| WORKFLOW_STATE_ABORTED | 5 | The update was aborted/cancelled |
| WORKFLOW_STATE_FAILED | 6 | The workflow has failed to complete. |
| WORKFLOW_STATE_ROLLING_BACKWARD | 7 | The update is rolling backward |
| WORKFLOW_STATE_ROLLED_BACK | 8 | The update was rolled back due to failure |



<a name="peloton.api.v1alpha.job.stateless.WorkflowType"/>

### WorkflowType
The different types of job rolling workflows supported.

| Name | Number | Description |
| ---- | ------ | ----------- |
| WORKFLOW_TYPE_INVALID | 0 | Invalid protobuf value. |
| WORKFLOW_TYPE_UPDATE | 1 | Job update workflow. |
| WORKFLOW_TYPE_RESTART | 2 | Restart pods in a job. |


 

 

 



<a name="watch_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## watch_svc.proto



<a name="peloton.api.v1alpha.watch.svc.CancelRequest"/>

### CancelRequest
CancelRequest is request for method WatchService.Cancel


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| watch_id | [uint64](#uint64) |  | ID of the watch session to cancel. |






<a name="peloton.api.v1alpha.watch.svc.CancelResponse"/>

### CancelResponse
CancelRequest is response for method WatchService.Cancel
Return errors:
NOT_FOUND: Watch ID not found






<a name="peloton.api.v1alpha.watch.svc.WatchRequest"/>

### WatchRequest
WatchRequest is request for method WatchService.Watch. It
specifies the objects that should be monitored for changes.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start_revision | [uint64](#uint64) |  | The revision from which to start getting changes. If unspecified, the server will return changes after the current revision. The server may choose to maintain only a limited number of historical revisions; a start revision older than the oldest revision available at the server will result in an error and the watch stream will be closed. Note: Initial implementations will not support historical revisions, so if the client sets a value for this field, it will receive an OUT_OF_RANGE error immediately. |
| stateless_job_filter | [.peloton.api.v1alpha.watch.StatelessJobFilter](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.watch.StatelessJobFilter) |  | Criteria to select the stateless jobs to watch. If unset, no jobs will be watched. |
| pod_filter | [.peloton.api.v1alpha.watch.PodFilter](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.watch.PodFilter) |  | Criteria to select the pods to watch. If unset, no pods will be watched. |






<a name="peloton.api.v1alpha.watch.svc.WatchResponse"/>

### WatchResponse
WatchResponse is response method for WatchService.Watch. It
contains the objects that have changed.
Return errors:
OUT_OF_RANGE: Requested start-revision is too old
INVALID_ARGUMENT: Requested start-revision is newer than server revision
RESOURCE_EXHAUSTED: Number of concurrent watches exceeded
CANCELLED: Watch cancelled


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| watch_id | [uint64](#uint64) |  | Unique identifier for the watch session |
| revision | [uint64](#uint64) |  | Server revision when the response results were created |
| stateless_jobs | [.peloton.api.v1alpha.job.stateless.JobSummary](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.job.stateless.JobSummary) | repeated | Stateless jobs that have changed. |
| stateless_jobs_not_found | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.peloton.JobID) | repeated | Stateless job IDs that were not found. |
| pods | [.peloton.api.v1alpha.pod.PodSummary](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.pod.PodSummary) | repeated | Pods that have changed. |
| pods_not_found | [.peloton.api.v1alpha.peloton.PodName](#peloton.api.v1alpha.watch.svc..peloton.api.v1alpha.peloton.PodName) | repeated | Names of pods that were not found. |





 

 

 


<a name="peloton.api.v1alpha.watch.svc.WatchService"/>

### WatchService
Watch service defines the methods for getting notifications
on changes to Peloton objects. A watch is long-running request
where a client specifies the kind of objects that it is interested
in as well as a revision, either current or historical. The server
continuously streams back changes from that revision till the client
cancels the watch (or the connection is lost). The server may support
only a limited amount of historical revisions to keep the load on
the server reasonable. Historical revisions are mainly provided for
clients to recover from transient errors without having to rebuild
a snapshot of the system (which can be expensive for both sides).
Also, implementations may limit the number of concurrent watch
requests that can be serviced so that the server is not overloaded.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Watch | [WatchRequest](#peloton.api.v1alpha.watch.svc.WatchRequest) | [WatchResponse](#peloton.api.v1alpha.watch.svc.WatchRequest) | Create a watch to get notified about changes to Peloton objects. Changed objects are streamed back to the caller till the watch is cancelled. |
| Cancel | [CancelRequest](#peloton.api.v1alpha.watch.svc.CancelRequest) | [CancelResponse](#peloton.api.v1alpha.watch.svc.CancelRequest) | Cancel a watch. The watch stream will get an error indicating watch was cancelled and the stream will be closed. |

 



<a name="volume_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume_svc.proto



<a name="peloton.api.v1alpha.volume.svc.DeleteVolumeRequest"/>

### DeleteVolumeRequest
Request message for VolumeService.DeleteVolume method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volume_id | [.peloton.api.v1alpha.peloton.VolumeID](#peloton.api.v1alpha.volume.svc..peloton.api.v1alpha.peloton.VolumeID) |  | volume id for the delete request. |






<a name="peloton.api.v1alpha.volume.svc.DeleteVolumeResponse"/>

### DeleteVolumeResponse
Response message for VolumeService.DeleteVolume method.
Return errors:
NOT_FOUND:   if the volume is not found.






<a name="peloton.api.v1alpha.volume.svc.GetVolumeRequest"/>

### GetVolumeRequest
Request message for VolumeService.GetVolumes method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volume_id | [.peloton.api.v1alpha.peloton.VolumeID](#peloton.api.v1alpha.volume.svc..peloton.api.v1alpha.peloton.VolumeID) |  | the volume id. |






<a name="peloton.api.v1alpha.volume.svc.GetVolumeResponse"/>

### GetVolumeResponse
Response message for VolumeService.GetVolumes method.
Return errors:
NOT_FOUND:   if the volume is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.v1alpha.volume.PersistentVolumeInfo](#peloton.api.v1alpha.volume.svc..peloton.api.v1alpha.volume.PersistentVolumeInfo) |  | volume info result. |






<a name="peloton.api.v1alpha.volume.svc.ListVolumesRequest"/>

### ListVolumesRequest
Request message for VolumeService.ListVolumes method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.volume.svc..peloton.api.v1alpha.peloton.JobID) |  | job ID for the volumes. |






<a name="peloton.api.v1alpha.volume.svc.ListVolumesResponse"/>

### ListVolumesResponse
Response message for VolumeService.ListVolumes method.
Return errors:
NOT_FOUND:   if the job is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [ListVolumesResponse.VolumesEntry](#peloton.api.v1alpha.volume.svc.ListVolumesResponse.VolumesEntry) | repeated | volumes result map from volume uuid to volume info. |






<a name="peloton.api.v1alpha.volume.svc.ListVolumesResponse.VolumesEntry"/>

### ListVolumesResponse.VolumesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [.peloton.api.v1alpha.volume.PersistentVolumeInfo](#peloton.api.v1alpha.volume.svc..peloton.api.v1alpha.volume.PersistentVolumeInfo) |  |  |





 

 

 


<a name="peloton.api.v1alpha.volume.svc.VolumeService"/>

### VolumeService
Volume Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListVolumes | [ListVolumesRequest](#peloton.api.v1alpha.volume.svc.ListVolumesRequest) | [ListVolumesResponse](#peloton.api.v1alpha.volume.svc.ListVolumesRequest) | List associated volumes for given job. |
| GetVolume | [GetVolumeRequest](#peloton.api.v1alpha.volume.svc.GetVolumeRequest) | [GetVolumeResponse](#peloton.api.v1alpha.volume.svc.GetVolumeRequest) | Get volume data. |
| DeleteVolume | [DeleteVolumeRequest](#peloton.api.v1alpha.volume.svc.DeleteVolumeRequest) | [DeleteVolumeResponse](#peloton.api.v1alpha.volume.svc.DeleteVolumeRequest) | Delete a persistent volume. |

 



<a name="stateless_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## stateless_svc.proto



<a name="peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowRequest"/>

### AbortJobWorkflowRequest
Request message for JobService.AbortJobWorkflow method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowResponse"/>

### AbortJobWorkflowResponse
Response message for JobService.AbortJobWorkflow method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.CreateJobRequest"/>

### CreateJobRequest
Request message for JobService.CreateJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The unique job UUID specified by the client. This can be used by the client to re-create a deleted job. If unset, the server will create a new UUID for the job for each invocation. |
| spec | [.peloton.api.v1alpha.job.stateless.JobSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSpec) |  | The configuration of the job to be created. |
| secrets | [.peloton.api.v1alpha.peloton.Secret](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.Secret) | repeated | Experimental: This is a batch feature. The implementation is subject to change (or removal) from stateless. The list of secrets for this job |
| create_spec | [.peloton.api.v1alpha.job.stateless.CreateSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.CreateSpec) |  | The creation SLA specification. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.CreateJobResponse"/>

### CreateJobResponse
Response message for JobService.CreateJob method.
Return errors:
ALREADY_EXISTS:    if the job ID already exists
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID of the newly created job. Will be the same as the one in CreateJobRequest if provided. Otherwise, a new job ID will be generated by the server. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.DeleteJobRequest"/>

### DeleteJobRequest
Request message for JobService.DeleteJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job to be deleted. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |
| force | [bool](#bool) |  | If set to true, it will force a delete of the job even if it is running. The job will be first stopped and deleted. This step cannot be undone, and the job cannot be re-created (with same uuid) till the delete is complete. So, it is recommended to not set force to true. |






<a name="peloton.api.v1alpha.job.stateless.svc.DeleteJobResponse"/>

### DeleteJobResponse
Response message for JobService.DeleteJob method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid or job is still running.
FailedPrecondition:  if the job has not been stopped before delete.






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobCacheRequest"/>

### GetJobCacheRequest
Request message for JobService.GetJobCache method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobCacheResponse"/>

### GetJobCacheResponse
Response message for JobService.GetJobCache method.
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [.peloton.api.v1alpha.job.stateless.JobSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSpec) |  | The job configuration in cache of the matching job. |
| status | [.peloton.api.v1alpha.job.stateless.JobStatus](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobStatus) |  | The job runtime in cache of the matching job. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameRequest"/>

### GetJobIDFromJobNameRequest
Request message for JobService.GetJobIDFromJobName method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_name | [string](#string) |  | Job name to lookup for job UUID. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameResponse"/>

### GetJobIDFromJobNameResponse
Response message for JobService.GetJobIDFromJobName method.
Return errors:
NOT_FOUND:         if the job name is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) | repeated | The job UUIDs for the job name. Job UUIDs are sorted by descending create timestamp. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobRequest"/>

### GetJobRequest
Request message for JobService.GetJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to look up the job. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The version of the job object to fetch. If not provided, then the latest job configuration specification and runtime status are returned. If provided, only the job configuration specification (and no runtime) at a given version is returned. |
| summary_only | [bool](#bool) |  | If set to true, only return the job summary. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetJobResponse"/>

### GetJobResponse
Response message for JobService.GetJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_info | [.peloton.api.v1alpha.job.stateless.JobInfo](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobInfo) |  | The configuration specification and runtime status of the job. |
| summary | [.peloton.api.v1alpha.job.stateless.JobSummary](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSummary) |  | The job summary. |
| secrets | [.peloton.api.v1alpha.peloton.Secret](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.Secret) | repeated | The list of secrets for this job, secret.Value will be empty. SecretID and path will be populated, so that caller can identify which secret is associated with this job. |
| workflow_info | [.peloton.api.v1alpha.job.stateless.WorkflowInfo](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.WorkflowInfo) |  | Information about the current/last completed workflow including its state and specification. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffRequest"/>

### GetReplaceJobDiffRequest
Request message for JobService.GetReplaceJobDiffRequest method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to be updated. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. |
| spec | [.peloton.api.v1alpha.job.stateless.JobSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSpec) |  | The new job configuration to be applied. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffResponse"/>

### GetReplaceJobDiffResponse
Response message for JobService.GetReplaceJobDiff method.
Return errors:
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| instances_added | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances which are being added |
| instances_removed | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances which are being removed |
| instances_updated | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances which are being updated |
| instances_unchanged | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | Instances which are unchanged |






<a name="peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsRequest"/>

### GetWorkflowEventsRequest
Request message for JobService.GetWorkflowEvents


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to look up the job. |
| instance_id | [uint32](#uint32) |  | The instance to get workflow events. |






<a name="peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsResponse"/>

### GetWorkflowEventsResponse
Response message for JobService.GetWorkflowEvents
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [.peloton.api.v1alpha.job.stateless.WorkflowEvent](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.WorkflowEvent) | repeated | Workflow events for the given workflow |






<a name="peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesRequest"/>

### ListJobUpdatesRequest
Request message for JobService.ListJobUpdates method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier. |






<a name="peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesResponse"/>

### ListJobUpdatesResponse
Response message for JobService.ListJobUpdates method.
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| workflow_infos | [.peloton.api.v1alpha.job.stateless.WorkflowInfo](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.WorkflowInfo) | repeated |  |






<a name="peloton.api.v1alpha.job.stateless.svc.ListJobsRequest"/>

### ListJobsRequest
Request message for JobService.ListJobs method.






<a name="peloton.api.v1alpha.job.stateless.svc.ListJobsResponse"/>

### ListJobsResponse
Response message for JobService.ListJobs method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobs | [.peloton.api.v1alpha.job.stateless.JobSummary](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSummary) | repeated | List of all jobs. |






<a name="peloton.api.v1alpha.job.stateless.svc.ListPodsRequest"/>

### ListPodsRequest
Request message for JobService.ListPods method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier of the pods to list. |
| range | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) |  | The instance ID range of the pods to list. If unset, all pods in the job will be returned. |






<a name="peloton.api.v1alpha.job.stateless.svc.ListPodsResponse"/>

### ListPodsResponse
Response message for JobService.ListPods method.
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pods | [.peloton.api.v1alpha.pod.PodSummary](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.PodSummary) | repeated | Pod summary for all matching pods. |






<a name="peloton.api.v1alpha.job.stateless.svc.PatchJobRequest"/>

### PatchJobRequest
Request message for JobService.PatchJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to be updated. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |
| spec | [.peloton.api.v1alpha.job.stateless.JobSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSpec) |  | The new job configuration to be patched. |
| secrets | [.peloton.api.v1alpha.peloton.Secret](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.Secret) | repeated | The list of secrets for this job |
| update_spec | [.peloton.api.v1alpha.job.stateless.UpdateSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.UpdateSpec) |  | The update SLA specification. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.PatchJobResponse"/>

### PatchJobResponse
Response message for JobService.PatchJob method.
Return errors:
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowRequest"/>

### PauseJobWorkflowRequest
Request message for JobService.PauseJobWorkflow method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowResponse"/>

### PauseJobWorkflowResponse
Response message for JobService.PauseJobWorkflow method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.QueryJobsRequest"/>

### QueryJobsRequest
Request message for JobService.QueryJobs method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [.peloton.api.v1alpha.job.stateless.QuerySpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.QuerySpec) |  | The spec of query criteria for the jobs. |






<a name="peloton.api.v1alpha.job.stateless.svc.QueryJobsResponse"/>

### QueryJobsResponse
Response message for JobService.QueryJobs method.
Return errors:
INVALID_ARGUMENT:  if the resource pool path or job states are invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [.peloton.api.v1alpha.job.stateless.JobSummary](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSummary) | repeated | List of jobs that match the job query criteria. |
| pagination | [.peloton.api.v1alpha.query.Pagination](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.query.Pagination) |  | Pagination result of the job query. |
| spec | [.peloton.api.v1alpha.job.stateless.QuerySpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.QuerySpec) |  | Return the spec of query criteria from the request. |






<a name="peloton.api.v1alpha.job.stateless.svc.QueryPodsRequest"/>

### QueryPodsRequest
Request message for JobService.QueryPods method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier of the pods to query. |
| spec | [.peloton.api.v1alpha.pod.QuerySpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.QuerySpec) |  | The spec of query criteria for the pods. |
| pagination | [.peloton.api.v1alpha.query.PaginationSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |
| summary_only | [bool](#bool) |  | If set to true, only return the pod status and not the configuration. |






<a name="peloton.api.v1alpha.job.stateless.svc.QueryPodsResponse"/>

### QueryPodsResponse
Response message for JobService.QueryPods method.
Return errors:
NOT_FOUND:         if the job ID is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pods | [.peloton.api.v1alpha.pod.PodInfo](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.PodInfo) | repeated | List of pods that match the pod query criteria. |
| pagination | [.peloton.api.v1alpha.query.Pagination](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.query.Pagination) |  | Pagination result of the pod query. |






<a name="peloton.api.v1alpha.job.stateless.svc.RefreshJobRequest"/>

### RefreshJobRequest
Request message for JobService.RefreshJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.RefreshJobResponse"/>

### RefreshJobResponse
Response message for JobService.RefreshJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.






<a name="peloton.api.v1alpha.job.stateless.svc.ReplaceJobRequest"/>

### ReplaceJobRequest
Request message for JobService.ReplaceJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job ID to be updated. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |
| spec | [.peloton.api.v1alpha.job.stateless.JobSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.JobSpec) |  | The new job configuration to be applied. |
| secrets | [.peloton.api.v1alpha.peloton.Secret](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.Secret) | repeated | The list of secrets for this job |
| update_spec | [.peloton.api.v1alpha.job.stateless.UpdateSpec](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.job.stateless.UpdateSpec) |  | The update SLA specification. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.ReplaceJobResponse"/>

### ReplaceJobResponse
Response message for JobService.ReplaceJob method.
Return errors:
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.RestartJobRequest"/>

### RestartJobRequest
Request message for JobService.RestartJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job to restart. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |
| batch_size | [uint32](#uint32) |  | Batch size for the restart request which controls how many instances may be restarted at the same time. |
| ranges | [.peloton.api.v1alpha.pod.InstanceIDRange](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.pod.InstanceIDRange) | repeated | The pods to restart, default to all. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.RestartJobResponse"/>

### RestartJobResponse
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowRequest"/>

### ResumeJobWorkflowRequest
Request message for JobService.ResumeJobWorkflow method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job identifier. |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. |
| opaque_data | [.peloton.api.v1alpha.peloton.OpaqueData](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.OpaqueData) |  | Opaque data supplied by the client |






<a name="peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowResponse"/>

### ResumeJobWorkflowResponse
Response message for JobService.ResumeJobWorkflow method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.StartJobRequest"/>

### StartJobRequest
Request message for JobService.StartJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job to start |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |






<a name="peloton.api.v1alpha.job.stateless.svc.StartJobResponse"/>

### StartJobResponse
Response message for JobService.StartJob method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |






<a name="peloton.api.v1alpha.job.stateless.svc.StopJobRequest"/>

### StopJobRequest
Request message for JobService.StopJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job_id | [.peloton.api.v1alpha.peloton.JobID](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.JobID) |  | The job to stop |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The current version of the job. It is used to implement optimistic concurrency control. |






<a name="peloton.api.v1alpha.job.stateless.svc.StopJobResponse"/>

### StopJobResponse
Response message for JobService.StopJob method.
Response message for JobService.RestartJob method.
Return errors:
NOT_FOUND:         if the job ID is not found.
ABORTED:           if the job version is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [.peloton.api.v1alpha.peloton.EntityVersion](#peloton.api.v1alpha.job.stateless.svc..peloton.api.v1alpha.peloton.EntityVersion) |  | The new version of the job. |





 

 

 


<a name="peloton.api.v1alpha.job.stateless.svc.JobService"/>

### JobService
Job service defines the job related methods such as create, get, query and kill jobs.


Methods which mutate the state of the job.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateJob | [CreateJobRequest](#peloton.api.v1alpha.job.stateless.svc.CreateJobRequest) | [CreateJobResponse](#peloton.api.v1alpha.job.stateless.svc.CreateJobRequest) | Create a new job with the given configuration. |
| ReplaceJob | [ReplaceJobRequest](#peloton.api.v1alpha.job.stateless.svc.ReplaceJobRequest) | [ReplaceJobResponse](#peloton.api.v1alpha.job.stateless.svc.ReplaceJobRequest) | Replace the configuration of an existing job with the new configuration. The caller is expected to provide the entire job configuration including the fields which are unchanged. |
| PatchJob | [PatchJobRequest](#peloton.api.v1alpha.job.stateless.svc.PatchJobRequest) | [PatchJobResponse](#peloton.api.v1alpha.job.stateless.svc.PatchJobRequest) | Patch the configuration of an existing job. The caller is not expected to provide all the configuration fields and can provide only subset (e.g. provide only the fields which have changed). This is not supported yet. |
| RestartJob | [RestartJobRequest](#peloton.api.v1alpha.job.stateless.svc.RestartJobRequest) | [RestartJobResponse](#peloton.api.v1alpha.job.stateless.svc.RestartJobRequest) | Restart the pods specified in the request. |
| PauseJobWorkflow | [PauseJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowRequest) | [PauseJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.PauseJobWorkflowRequest) | Pause the current running workflow. If there is no current running workflow, or the current workflow is already paused, then the method is a no-op. |
| ResumeJobWorkflow | [ResumeJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowRequest) | [ResumeJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.ResumeJobWorkflowRequest) | Resume the current running workflow. If there is no current running workflow, or the current workflow is not paused, then the method is a no-op. |
| AbortJobWorkflow | [AbortJobWorkflowRequest](#peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowRequest) | [AbortJobWorkflowResponse](#peloton.api.v1alpha.job.stateless.svc.AbortJobWorkflowRequest) | Abort the current running workflow. If there is no current running workflow, then the method is a no-op. |
| StartJob | [StartJobRequest](#peloton.api.v1alpha.job.stateless.svc.StartJobRequest) | [StartJobResponse](#peloton.api.v1alpha.job.stateless.svc.StartJobRequest) | Start the pods specified in the request. |
| StopJob | [StopJobRequest](#peloton.api.v1alpha.job.stateless.svc.StopJobRequest) | [StopJobResponse](#peloton.api.v1alpha.job.stateless.svc.StopJobRequest) | Stop the pods specified in the request. |
| DeleteJob | [DeleteJobRequest](#peloton.api.v1alpha.job.stateless.svc.DeleteJobRequest) | [DeleteJobResponse](#peloton.api.v1alpha.job.stateless.svc.DeleteJobRequest) | Delete a job and all related state. |
| GetJob | [GetJobRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobRequest) | [GetJobResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobRequest) | Get the configuration and runtime status of a job. |
| GetJobIDFromJobName | [GetJobIDFromJobNameRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameRequest) | [GetJobIDFromJobNameResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobIDFromJobNameRequest) | Get the job UUID from job name. |
| GetWorkflowEvents | [GetWorkflowEventsRequest](#peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsRequest) | [GetWorkflowEventsResponse](#peloton.api.v1alpha.job.stateless.svc.GetWorkflowEventsRequest) | Get the events of the current / last completed workflow of a job |
| ListPods | [ListPodsRequest](#peloton.api.v1alpha.job.stateless.svc.ListPodsRequest) | [ListPodsResponse](#peloton.api.v1alpha.job.stateless.svc.ListPodsRequest) | List all pods in a job for a given range of pod IDs. |
| QueryPods | [QueryPodsRequest](#peloton.api.v1alpha.job.stateless.svc.QueryPodsRequest) | [QueryPodsResponse](#peloton.api.v1alpha.job.stateless.svc.QueryPodsRequest) | Query pod info in a job using a set of filters. |
| QueryJobs | [QueryJobsRequest](#peloton.api.v1alpha.job.stateless.svc.QueryJobsRequest) | [QueryJobsResponse](#peloton.api.v1alpha.job.stateless.svc.QueryJobsRequest) | Query the jobs using a set of filters. TODO find the appropriate service to put this method in. |
| ListJobs | [ListJobsRequest](#peloton.api.v1alpha.job.stateless.svc.ListJobsRequest) | [ListJobsResponse](#peloton.api.v1alpha.job.stateless.svc.ListJobsRequest) | Get summary for all jobs. Results are streamed back to the caller in batches and the stream is closed once all results have been sent. |
| ListJobUpdates | [ListJobUpdatesRequest](#peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesRequest) | [ListJobUpdatesResponse](#peloton.api.v1alpha.job.stateless.svc.ListJobUpdatesRequest) | List all updates (including current and previously completed) for a given job. |
| GetReplaceJobDiff | [GetReplaceJobDiffRequest](#peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffRequest) | [GetReplaceJobDiffResponse](#peloton.api.v1alpha.job.stateless.svc.GetReplaceJobDiffRequest) | Get the list of instances which will be added/removed/updated if the given job specification is applied via the ReplaceJob API. |
| RefreshJob | [RefreshJobRequest](#peloton.api.v1alpha.job.stateless.svc.RefreshJobRequest) | [RefreshJobResponse](#peloton.api.v1alpha.job.stateless.svc.RefreshJobRequest) | Allows user to load job runtime status from the database and re-execute the action associated with current state. |
| GetJobCache | [GetJobCacheRequest](#peloton.api.v1alpha.job.stateless.svc.GetJobCacheRequest) | [GetJobCacheResponse](#peloton.api.v1alpha.job.stateless.svc.GetJobCacheRequest) | Get the job state in the cache. |

 



## Scalar Value Types

| .proto Type | Notes | C++ Type | Java Type | Python Type |
| ----------- | ----- | -------- | --------- | ----------- |
| <a name="double" /> double |  | double | double | float |
| <a name="float" /> float |  | float | float | float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long |
| <a name="bool" /> bool |  | bool | boolean | boolean |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str |

