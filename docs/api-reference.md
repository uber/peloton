# Protocol Documentation
<a name="top"/>

## Table of Contents

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

