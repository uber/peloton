# Protocol Documentation
<a name="top"/>

## Table of Contents

- [mesos.proto](#mesos.proto)
    - [Address](#mesos.v1.Address)
    - [AgentID](#mesos.v1.AgentID)
    - [AgentInfo](#mesos.v1.AgentInfo)
    - [AgentInfo.Capability](#mesos.v1.AgentInfo.Capability)
    - [Attribute](#mesos.v1.Attribute)
    - [CapabilityInfo](#mesos.v1.CapabilityInfo)
    - [CgroupInfo](#mesos.v1.CgroupInfo)
    - [CgroupInfo.NetCls](#mesos.v1.CgroupInfo.NetCls)
    - [CheckInfo](#mesos.v1.CheckInfo)
    - [CheckInfo.Command](#mesos.v1.CheckInfo.Command)
    - [CheckInfo.Http](#mesos.v1.CheckInfo.Http)
    - [CheckStatusInfo](#mesos.v1.CheckStatusInfo)
    - [CheckStatusInfo.Command](#mesos.v1.CheckStatusInfo.Command)
    - [CheckStatusInfo.Http](#mesos.v1.CheckStatusInfo.Http)
    - [CommandInfo](#mesos.v1.CommandInfo)
    - [CommandInfo.URI](#mesos.v1.CommandInfo.URI)
    - [ContainerID](#mesos.v1.ContainerID)
    - [ContainerInfo](#mesos.v1.ContainerInfo)
    - [ContainerInfo.DockerInfo](#mesos.v1.ContainerInfo.DockerInfo)
    - [ContainerInfo.DockerInfo.PortMapping](#mesos.v1.ContainerInfo.DockerInfo.PortMapping)
    - [ContainerInfo.MesosInfo](#mesos.v1.ContainerInfo.MesosInfo)
    - [ContainerStatus](#mesos.v1.ContainerStatus)
    - [Credential](#mesos.v1.Credential)
    - [Credentials](#mesos.v1.Credentials)
    - [DiscoveryInfo](#mesos.v1.DiscoveryInfo)
    - [DurationInfo](#mesos.v1.DurationInfo)
    - [Environment](#mesos.v1.Environment)
    - [Environment.Variable](#mesos.v1.Environment.Variable)
    - [ExecutorID](#mesos.v1.ExecutorID)
    - [ExecutorInfo](#mesos.v1.ExecutorInfo)
    - [FileInfo](#mesos.v1.FileInfo)
    - [Filters](#mesos.v1.Filters)
    - [Flag](#mesos.v1.Flag)
    - [FrameworkID](#mesos.v1.FrameworkID)
    - [FrameworkInfo](#mesos.v1.FrameworkInfo)
    - [FrameworkInfo.Capability](#mesos.v1.FrameworkInfo.Capability)
    - [HealthCheck](#mesos.v1.HealthCheck)
    - [HealthCheck.HTTPCheckInfo](#mesos.v1.HealthCheck.HTTPCheckInfo)
    - [HealthCheck.TCPCheckInfo](#mesos.v1.HealthCheck.TCPCheckInfo)
    - [IcmpStatistics](#mesos.v1.IcmpStatistics)
    - [Image](#mesos.v1.Image)
    - [Image.Appc](#mesos.v1.Image.Appc)
    - [Image.Docker](#mesos.v1.Image.Docker)
    - [InverseOffer](#mesos.v1.InverseOffer)
    - [IpStatistics](#mesos.v1.IpStatistics)
    - [KillPolicy](#mesos.v1.KillPolicy)
    - [Label](#mesos.v1.Label)
    - [Labels](#mesos.v1.Labels)
    - [LinuxInfo](#mesos.v1.LinuxInfo)
    - [MachineID](#mesos.v1.MachineID)
    - [MachineInfo](#mesos.v1.MachineInfo)
    - [MasterInfo](#mesos.v1.MasterInfo)
    - [Metric](#mesos.v1.Metric)
    - [NetworkInfo](#mesos.v1.NetworkInfo)
    - [NetworkInfo.IPAddress](#mesos.v1.NetworkInfo.IPAddress)
    - [NetworkInfo.PortMapping](#mesos.v1.NetworkInfo.PortMapping)
    - [Offer](#mesos.v1.Offer)
    - [Offer.Operation](#mesos.v1.Offer.Operation)
    - [Offer.Operation.Create](#mesos.v1.Offer.Operation.Create)
    - [Offer.Operation.Destroy](#mesos.v1.Offer.Operation.Destroy)
    - [Offer.Operation.Launch](#mesos.v1.Offer.Operation.Launch)
    - [Offer.Operation.LaunchGroup](#mesos.v1.Offer.Operation.LaunchGroup)
    - [Offer.Operation.Reserve](#mesos.v1.Offer.Operation.Reserve)
    - [Offer.Operation.Unreserve](#mesos.v1.Offer.Operation.Unreserve)
    - [OfferID](#mesos.v1.OfferID)
    - [Parameter](#mesos.v1.Parameter)
    - [Parameters](#mesos.v1.Parameters)
    - [PerfStatistics](#mesos.v1.PerfStatistics)
    - [Port](#mesos.v1.Port)
    - [Ports](#mesos.v1.Ports)
    - [RLimitInfo](#mesos.v1.RLimitInfo)
    - [RLimitInfo.RLimit](#mesos.v1.RLimitInfo.RLimit)
    - [RateLimit](#mesos.v1.RateLimit)
    - [RateLimits](#mesos.v1.RateLimits)
    - [Request](#mesos.v1.Request)
    - [Resource](#mesos.v1.Resource)
    - [Resource.AllocationInfo](#mesos.v1.Resource.AllocationInfo)
    - [Resource.DiskInfo](#mesos.v1.Resource.DiskInfo)
    - [Resource.DiskInfo.Persistence](#mesos.v1.Resource.DiskInfo.Persistence)
    - [Resource.DiskInfo.Source](#mesos.v1.Resource.DiskInfo.Source)
    - [Resource.DiskInfo.Source.Mount](#mesos.v1.Resource.DiskInfo.Source.Mount)
    - [Resource.DiskInfo.Source.Path](#mesos.v1.Resource.DiskInfo.Source.Path)
    - [Resource.ReservationInfo](#mesos.v1.Resource.ReservationInfo)
    - [Resource.RevocableInfo](#mesos.v1.Resource.RevocableInfo)
    - [Resource.SharedInfo](#mesos.v1.Resource.SharedInfo)
    - [ResourceStatistics](#mesos.v1.ResourceStatistics)
    - [ResourceUsage](#mesos.v1.ResourceUsage)
    - [ResourceUsage.Executor](#mesos.v1.ResourceUsage.Executor)
    - [ResourceUsage.Executor.Task](#mesos.v1.ResourceUsage.Executor.Task)
    - [Role](#mesos.v1.Role)
    - [SNMPStatistics](#mesos.v1.SNMPStatistics)
    - [Secret](#mesos.v1.Secret)
    - [Secret.Reference](#mesos.v1.Secret.Reference)
    - [Secret.Value](#mesos.v1.Secret.Value)
    - [TTYInfo](#mesos.v1.TTYInfo)
    - [TTYInfo.WindowSize](#mesos.v1.TTYInfo.WindowSize)
    - [Task](#mesos.v1.Task)
    - [TaskGroupInfo](#mesos.v1.TaskGroupInfo)
    - [TaskID](#mesos.v1.TaskID)
    - [TaskInfo](#mesos.v1.TaskInfo)
    - [TaskStatus](#mesos.v1.TaskStatus)
    - [TcpStatistics](#mesos.v1.TcpStatistics)
    - [TimeInfo](#mesos.v1.TimeInfo)
    - [TrafficControlStatistics](#mesos.v1.TrafficControlStatistics)
    - [URL](#mesos.v1.URL)
    - [UdpStatistics](#mesos.v1.UdpStatistics)
    - [Unavailability](#mesos.v1.Unavailability)
    - [Value](#mesos.v1.Value)
    - [Value.Range](#mesos.v1.Value.Range)
    - [Value.Ranges](#mesos.v1.Value.Ranges)
    - [Value.Scalar](#mesos.v1.Value.Scalar)
    - [Value.Set](#mesos.v1.Value.Set)
    - [Value.Text](#mesos.v1.Value.Text)
    - [VersionInfo](#mesos.v1.VersionInfo)
    - [Volume](#mesos.v1.Volume)
    - [Volume.Source](#mesos.v1.Volume.Source)
    - [Volume.Source.DockerVolume](#mesos.v1.Volume.Source.DockerVolume)
    - [Volume.Source.SandboxPath](#mesos.v1.Volume.Source.SandboxPath)
    - [WeightInfo](#mesos.v1.WeightInfo)
  
    - [AgentInfo.Capability.Type](#mesos.v1.AgentInfo.Capability.Type)
    - [CapabilityInfo.Capability](#mesos.v1.CapabilityInfo.Capability)
    - [CheckInfo.Type](#mesos.v1.CheckInfo.Type)
    - [ContainerInfo.DockerInfo.Network](#mesos.v1.ContainerInfo.DockerInfo.Network)
    - [ContainerInfo.Type](#mesos.v1.ContainerInfo.Type)
    - [DiscoveryInfo.Visibility](#mesos.v1.DiscoveryInfo.Visibility)
    - [Environment.Variable.Type](#mesos.v1.Environment.Variable.Type)
    - [ExecutorInfo.Type](#mesos.v1.ExecutorInfo.Type)
    - [FrameworkInfo.Capability.Type](#mesos.v1.FrameworkInfo.Capability.Type)
    - [HealthCheck.Type](#mesos.v1.HealthCheck.Type)
    - [Image.Type](#mesos.v1.Image.Type)
    - [MachineInfo.Mode](#mesos.v1.MachineInfo.Mode)
    - [NetworkInfo.Protocol](#mesos.v1.NetworkInfo.Protocol)
    - [Offer.Operation.Type](#mesos.v1.Offer.Operation.Type)
    - [RLimitInfo.RLimit.Type](#mesos.v1.RLimitInfo.RLimit.Type)
    - [Resource.DiskInfo.Source.Type](#mesos.v1.Resource.DiskInfo.Source.Type)
    - [Secret.Type](#mesos.v1.Secret.Type)
    - [Status](#mesos.v1.Status)
    - [TaskState](#mesos.v1.TaskState)
    - [TaskStatus.Reason](#mesos.v1.TaskStatus.Reason)
    - [TaskStatus.Source](#mesos.v1.TaskStatus.Source)
    - [Value.Type](#mesos.v1.Value.Type)
    - [Volume.Mode](#mesos.v1.Volume.Mode)
    - [Volume.Source.SandboxPath.Type](#mesos.v1.Volume.Source.SandboxPath.Type)
    - [Volume.Source.Type](#mesos.v1.Volume.Source.Type)
  
  
  

- [peloton.proto](#peloton.proto)
    - [ChangeLog](#peloton.api.peloton.ChangeLog)
    - [JobID](#peloton.api.peloton.JobID)
    - [Label](#peloton.api.peloton.Label)
    - [ResourcePoolID](#peloton.api.peloton.ResourcePoolID)
    - [TaskID](#peloton.api.peloton.TaskID)
    - [VolumeID](#peloton.api.peloton.VolumeID)
  
  
  
  

- [agent.proto](#agent.proto)
    - [Call](#mesos.v1.agent.Call)
    - [Call.AttachContainerInput](#mesos.v1.agent.Call.AttachContainerInput)
    - [Call.AttachContainerOutput](#mesos.v1.agent.Call.AttachContainerOutput)
    - [Call.GetMetrics](#mesos.v1.agent.Call.GetMetrics)
    - [Call.KillNestedContainer](#mesos.v1.agent.Call.KillNestedContainer)
    - [Call.LaunchNestedContainer](#mesos.v1.agent.Call.LaunchNestedContainer)
    - [Call.LaunchNestedContainerSession](#mesos.v1.agent.Call.LaunchNestedContainerSession)
    - [Call.ListFiles](#mesos.v1.agent.Call.ListFiles)
    - [Call.ReadFile](#mesos.v1.agent.Call.ReadFile)
    - [Call.SetLoggingLevel](#mesos.v1.agent.Call.SetLoggingLevel)
    - [Call.WaitNestedContainer](#mesos.v1.agent.Call.WaitNestedContainer)
    - [ProcessIO](#mesos.v1.agent.ProcessIO)
    - [ProcessIO.Control](#mesos.v1.agent.ProcessIO.Control)
    - [ProcessIO.Control.Heartbeat](#mesos.v1.agent.ProcessIO.Control.Heartbeat)
    - [ProcessIO.Data](#mesos.v1.agent.ProcessIO.Data)
    - [Response](#mesos.v1.agent.Response)
    - [Response.GetContainers](#mesos.v1.agent.Response.GetContainers)
    - [Response.GetContainers.Container](#mesos.v1.agent.Response.GetContainers.Container)
    - [Response.GetExecutors](#mesos.v1.agent.Response.GetExecutors)
    - [Response.GetExecutors.Executor](#mesos.v1.agent.Response.GetExecutors.Executor)
    - [Response.GetFlags](#mesos.v1.agent.Response.GetFlags)
    - [Response.GetFrameworks](#mesos.v1.agent.Response.GetFrameworks)
    - [Response.GetFrameworks.Framework](#mesos.v1.agent.Response.GetFrameworks.Framework)
    - [Response.GetHealth](#mesos.v1.agent.Response.GetHealth)
    - [Response.GetLoggingLevel](#mesos.v1.agent.Response.GetLoggingLevel)
    - [Response.GetMetrics](#mesos.v1.agent.Response.GetMetrics)
    - [Response.GetState](#mesos.v1.agent.Response.GetState)
    - [Response.GetTasks](#mesos.v1.agent.Response.GetTasks)
    - [Response.GetVersion](#mesos.v1.agent.Response.GetVersion)
    - [Response.ListFiles](#mesos.v1.agent.Response.ListFiles)
    - [Response.ReadFile](#mesos.v1.agent.Response.ReadFile)
    - [Response.WaitNestedContainer](#mesos.v1.agent.Response.WaitNestedContainer)
  
    - [Call.AttachContainerInput.Type](#mesos.v1.agent.Call.AttachContainerInput.Type)
    - [Call.Type](#mesos.v1.agent.Call.Type)
    - [ProcessIO.Control.Type](#mesos.v1.agent.ProcessIO.Control.Type)
    - [ProcessIO.Data.Type](#mesos.v1.agent.ProcessIO.Data.Type)
    - [ProcessIO.Type](#mesos.v1.agent.ProcessIO.Type)
    - [Response.Type](#mesos.v1.agent.Response.Type)
  
  
  

- [allocator.proto](#allocator.proto)
    - [InverseOfferStatus](#mesos.v1.allocator.InverseOfferStatus)
  
    - [InverseOfferStatus.Status](#mesos.v1.allocator.InverseOfferStatus.Status)
  
  
  

- [executor.proto](#executor.proto)
    - [Call](#mesos.v1.executor.Call)
    - [Call.Message](#mesos.v1.executor.Call.Message)
    - [Call.Subscribe](#mesos.v1.executor.Call.Subscribe)
    - [Call.Update](#mesos.v1.executor.Call.Update)
    - [Event](#mesos.v1.executor.Event)
    - [Event.Acknowledged](#mesos.v1.executor.Event.Acknowledged)
    - [Event.Error](#mesos.v1.executor.Event.Error)
    - [Event.Kill](#mesos.v1.executor.Event.Kill)
    - [Event.Launch](#mesos.v1.executor.Event.Launch)
    - [Event.LaunchGroup](#mesos.v1.executor.Event.LaunchGroup)
    - [Event.Message](#mesos.v1.executor.Event.Message)
    - [Event.Subscribed](#mesos.v1.executor.Event.Subscribed)
  
    - [Call.Type](#mesos.v1.executor.Call.Type)
    - [Event.Type](#mesos.v1.executor.Event.Type)
  
  
  

- [maintenance.proto](#maintenance.proto)
    - [ClusterStatus](#mesos.v1.maintenance.ClusterStatus)
    - [ClusterStatus.DrainingMachine](#mesos.v1.maintenance.ClusterStatus.DrainingMachine)
    - [Schedule](#mesos.v1.maintenance.Schedule)
    - [Window](#mesos.v1.maintenance.Window)
  
  
  
  

- [quota.proto](#quota.proto)
    - [QuotaInfo](#mesos.v1.quota.QuotaInfo)
    - [QuotaRequest](#mesos.v1.quota.QuotaRequest)
    - [QuotaStatus](#mesos.v1.quota.QuotaStatus)
  
  
  
  

- [master.proto](#master.proto)
    - [Call](#mesos.v1.master.Call)
    - [Call.CreateVolumes](#mesos.v1.master.Call.CreateVolumes)
    - [Call.DestroyVolumes](#mesos.v1.master.Call.DestroyVolumes)
    - [Call.GetMetrics](#mesos.v1.master.Call.GetMetrics)
    - [Call.ListFiles](#mesos.v1.master.Call.ListFiles)
    - [Call.ReadFile](#mesos.v1.master.Call.ReadFile)
    - [Call.RemoveQuota](#mesos.v1.master.Call.RemoveQuota)
    - [Call.ReserveResources](#mesos.v1.master.Call.ReserveResources)
    - [Call.SetLoggingLevel](#mesos.v1.master.Call.SetLoggingLevel)
    - [Call.SetQuota](#mesos.v1.master.Call.SetQuota)
    - [Call.StartMaintenance](#mesos.v1.master.Call.StartMaintenance)
    - [Call.StopMaintenance](#mesos.v1.master.Call.StopMaintenance)
    - [Call.UnreserveResources](#mesos.v1.master.Call.UnreserveResources)
    - [Call.UpdateMaintenanceSchedule](#mesos.v1.master.Call.UpdateMaintenanceSchedule)
    - [Call.UpdateWeights](#mesos.v1.master.Call.UpdateWeights)
    - [Event](#mesos.v1.master.Event)
    - [Event.AgentAdded](#mesos.v1.master.Event.AgentAdded)
    - [Event.AgentRemoved](#mesos.v1.master.Event.AgentRemoved)
    - [Event.Subscribed](#mesos.v1.master.Event.Subscribed)
    - [Event.TaskAdded](#mesos.v1.master.Event.TaskAdded)
    - [Event.TaskUpdated](#mesos.v1.master.Event.TaskUpdated)
    - [Response](#mesos.v1.master.Response)
    - [Response.GetAgents](#mesos.v1.master.Response.GetAgents)
    - [Response.GetAgents.Agent](#mesos.v1.master.Response.GetAgents.Agent)
    - [Response.GetExecutors](#mesos.v1.master.Response.GetExecutors)
    - [Response.GetExecutors.Executor](#mesos.v1.master.Response.GetExecutors.Executor)
    - [Response.GetFlags](#mesos.v1.master.Response.GetFlags)
    - [Response.GetFrameworks](#mesos.v1.master.Response.GetFrameworks)
    - [Response.GetFrameworks.Framework](#mesos.v1.master.Response.GetFrameworks.Framework)
    - [Response.GetHealth](#mesos.v1.master.Response.GetHealth)
    - [Response.GetLoggingLevel](#mesos.v1.master.Response.GetLoggingLevel)
    - [Response.GetMaintenanceSchedule](#mesos.v1.master.Response.GetMaintenanceSchedule)
    - [Response.GetMaintenanceStatus](#mesos.v1.master.Response.GetMaintenanceStatus)
    - [Response.GetMaster](#mesos.v1.master.Response.GetMaster)
    - [Response.GetMetrics](#mesos.v1.master.Response.GetMetrics)
    - [Response.GetQuota](#mesos.v1.master.Response.GetQuota)
    - [Response.GetRoles](#mesos.v1.master.Response.GetRoles)
    - [Response.GetState](#mesos.v1.master.Response.GetState)
    - [Response.GetTasks](#mesos.v1.master.Response.GetTasks)
    - [Response.GetVersion](#mesos.v1.master.Response.GetVersion)
    - [Response.GetWeights](#mesos.v1.master.Response.GetWeights)
    - [Response.ListFiles](#mesos.v1.master.Response.ListFiles)
    - [Response.ReadFile](#mesos.v1.master.Response.ReadFile)
  
    - [Call.Type](#mesos.v1.master.Call.Type)
    - [Event.Type](#mesos.v1.master.Event.Type)
    - [Response.Type](#mesos.v1.master.Response.Type)
  
  
  

- [scheduler.proto](#scheduler.proto)
    - [Call](#mesos.v1.scheduler.Call)
    - [Call.Accept](#mesos.v1.scheduler.Call.Accept)
    - [Call.AcceptInverseOffers](#mesos.v1.scheduler.Call.AcceptInverseOffers)
    - [Call.Acknowledge](#mesos.v1.scheduler.Call.Acknowledge)
    - [Call.Decline](#mesos.v1.scheduler.Call.Decline)
    - [Call.DeclineInverseOffers](#mesos.v1.scheduler.Call.DeclineInverseOffers)
    - [Call.Kill](#mesos.v1.scheduler.Call.Kill)
    - [Call.Message](#mesos.v1.scheduler.Call.Message)
    - [Call.Reconcile](#mesos.v1.scheduler.Call.Reconcile)
    - [Call.Reconcile.Task](#mesos.v1.scheduler.Call.Reconcile.Task)
    - [Call.Request](#mesos.v1.scheduler.Call.Request)
    - [Call.Revive](#mesos.v1.scheduler.Call.Revive)
    - [Call.Shutdown](#mesos.v1.scheduler.Call.Shutdown)
    - [Call.Subscribe](#mesos.v1.scheduler.Call.Subscribe)
    - [Call.Suppress](#mesos.v1.scheduler.Call.Suppress)
    - [Event](#mesos.v1.scheduler.Event)
    - [Event.Error](#mesos.v1.scheduler.Event.Error)
    - [Event.Failure](#mesos.v1.scheduler.Event.Failure)
    - [Event.InverseOffers](#mesos.v1.scheduler.Event.InverseOffers)
    - [Event.Message](#mesos.v1.scheduler.Event.Message)
    - [Event.Offers](#mesos.v1.scheduler.Event.Offers)
    - [Event.Rescind](#mesos.v1.scheduler.Event.Rescind)
    - [Event.RescindInverseOffer](#mesos.v1.scheduler.Event.RescindInverseOffer)
    - [Event.Subscribed](#mesos.v1.scheduler.Event.Subscribed)
    - [Event.Update](#mesos.v1.scheduler.Event.Update)
  
    - [Call.Type](#mesos.v1.scheduler.Call.Type)
    - [Event.Type](#mesos.v1.scheduler.Event.Type)
  
  
  

- [changelog.proto](#changelog.proto)
    - [ChangeLog](#peloton.api.changelog.ChangeLog)
  
  
  
  

- [errors.proto](#errors.proto)
    - [InvalidRespool](#peloton.api.errors.InvalidRespool)
    - [JobGetRuntimeFail](#peloton.api.errors.JobGetRuntimeFail)
    - [JobNotFound](#peloton.api.errors.JobNotFound)
    - [UnknownError](#peloton.api.errors.UnknownError)
  
  
  
  

- [query.proto](#query.proto)
    - [OrderBy](#peloton.api.query.OrderBy)
    - [Pagination](#peloton.api.query.Pagination)
    - [PaginationSpec](#peloton.api.query.PaginationSpec)
    - [PropertyPath](#peloton.api.query.PropertyPath)
  
    - [OrderBy.Order](#peloton.api.query.OrderBy.Order)
  
  
  

- [task.proto](#task.proto)
    - [AndConstraint](#peloton.api.task.AndConstraint)
    - [BrowseSandboxFailure](#peloton.api.task.BrowseSandboxFailure)
    - [BrowseSandboxRequest](#peloton.api.task.BrowseSandboxRequest)
    - [BrowseSandboxResponse](#peloton.api.task.BrowseSandboxResponse)
    - [BrowseSandboxResponse.Error](#peloton.api.task.BrowseSandboxResponse.Error)
    - [Constraint](#peloton.api.task.Constraint)
    - [GetEventsRequest](#peloton.api.task.GetEventsRequest)
    - [GetEventsResponse](#peloton.api.task.GetEventsResponse)
    - [GetEventsResponse.Error](#peloton.api.task.GetEventsResponse.Error)
    - [GetEventsResponse.Events](#peloton.api.task.GetEventsResponse.Events)
    - [GetRequest](#peloton.api.task.GetRequest)
    - [GetResponse](#peloton.api.task.GetResponse)
    - [HealthCheckConfig](#peloton.api.task.HealthCheckConfig)
    - [HealthCheckConfig.CommandCheck](#peloton.api.task.HealthCheckConfig.CommandCheck)
    - [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange)
    - [InstanceRange](#peloton.api.task.InstanceRange)
    - [LabelConstraint](#peloton.api.task.LabelConstraint)
    - [ListRequest](#peloton.api.task.ListRequest)
    - [ListResponse](#peloton.api.task.ListResponse)
    - [ListResponse.Result](#peloton.api.task.ListResponse.Result)
    - [ListResponse.Result.ValueEntry](#peloton.api.task.ListResponse.Result.ValueEntry)
    - [OrConstraint](#peloton.api.task.OrConstraint)
    - [PersistentVolumeConfig](#peloton.api.task.PersistentVolumeConfig)
    - [PortConfig](#peloton.api.task.PortConfig)
    - [PreemptionPolicy](#peloton.api.task.PreemptionPolicy)
    - [QueryRequest](#peloton.api.task.QueryRequest)
    - [QueryResponse](#peloton.api.task.QueryResponse)
    - [QueryResponse.Error](#peloton.api.task.QueryResponse.Error)
    - [QuerySpec](#peloton.api.task.QuerySpec)
    - [ResourceConfig](#peloton.api.task.ResourceConfig)
    - [RestartPolicy](#peloton.api.task.RestartPolicy)
    - [RestartRequest](#peloton.api.task.RestartRequest)
    - [RestartResponse](#peloton.api.task.RestartResponse)
    - [RuntimeInfo](#peloton.api.task.RuntimeInfo)
    - [RuntimeInfo.PortsEntry](#peloton.api.task.RuntimeInfo.PortsEntry)
    - [StartRequest](#peloton.api.task.StartRequest)
    - [StartResponse](#peloton.api.task.StartResponse)
    - [StartResponse.Error](#peloton.api.task.StartResponse.Error)
    - [StopRequest](#peloton.api.task.StopRequest)
    - [StopResponse](#peloton.api.task.StopResponse)
    - [StopResponse.Error](#peloton.api.task.StopResponse.Error)
    - [TaskConfig](#peloton.api.task.TaskConfig)
    - [TaskEvent](#peloton.api.task.TaskEvent)
    - [TaskEventsError](#peloton.api.task.TaskEventsError)
    - [TaskInfo](#peloton.api.task.TaskInfo)
    - [TaskNotRunning](#peloton.api.task.TaskNotRunning)
    - [TaskStartFailure](#peloton.api.task.TaskStartFailure)
    - [TaskUpdateError](#peloton.api.task.TaskUpdateError)
  
    - [Constraint.Type](#peloton.api.task.Constraint.Type)
    - [HealthCheckConfig.Type](#peloton.api.task.HealthCheckConfig.Type)
    - [LabelConstraint.Condition](#peloton.api.task.LabelConstraint.Condition)
    - [LabelConstraint.Kind](#peloton.api.task.LabelConstraint.Kind)
    - [TaskEvent.Source](#peloton.api.task.TaskEvent.Source)
    - [TaskState](#peloton.api.task.TaskState)
  
  
    - [TaskManager](#peloton.api.task.TaskManager)
  

- [respool.proto](#respool.proto)
    - [CreateRequest](#peloton.api.respool.CreateRequest)
    - [CreateResponse](#peloton.api.respool.CreateResponse)
    - [CreateResponse.Error](#peloton.api.respool.CreateResponse.Error)
    - [DeleteRequest](#peloton.api.respool.DeleteRequest)
    - [DeleteResponse](#peloton.api.respool.DeleteResponse)
    - [DeleteResponse.Error](#peloton.api.respool.DeleteResponse.Error)
    - [GetRequest](#peloton.api.respool.GetRequest)
    - [GetResponse](#peloton.api.respool.GetResponse)
    - [GetResponse.Error](#peloton.api.respool.GetResponse.Error)
    - [InvalidResourcePoolConfig](#peloton.api.respool.InvalidResourcePoolConfig)
    - [InvalidResourcePoolPath](#peloton.api.respool.InvalidResourcePoolPath)
    - [LookupRequest](#peloton.api.respool.LookupRequest)
    - [LookupResponse](#peloton.api.respool.LookupResponse)
    - [LookupResponse.Error](#peloton.api.respool.LookupResponse.Error)
    - [QueryRequest](#peloton.api.respool.QueryRequest)
    - [QueryResponse](#peloton.api.respool.QueryResponse)
    - [QueryResponse.Error](#peloton.api.respool.QueryResponse.Error)
    - [ResourceConfig](#peloton.api.respool.ResourceConfig)
    - [ResourcePoolAlreadyExists](#peloton.api.respool.ResourcePoolAlreadyExists)
    - [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig)
    - [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo)
    - [ResourcePoolIsBusy](#peloton.api.respool.ResourcePoolIsBusy)
    - [ResourcePoolIsNotLeaf](#peloton.api.respool.ResourcePoolIsNotLeaf)
    - [ResourcePoolNotDeleted](#peloton.api.respool.ResourcePoolNotDeleted)
    - [ResourcePoolNotFound](#peloton.api.respool.ResourcePoolNotFound)
    - [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath)
    - [ResourcePoolPathNotFound](#peloton.api.respool.ResourcePoolPathNotFound)
    - [ResourceUsage](#peloton.api.respool.ResourceUsage)
    - [UpdateRequest](#peloton.api.respool.UpdateRequest)
    - [UpdateResponse](#peloton.api.respool.UpdateResponse)
    - [UpdateResponse.Error](#peloton.api.respool.UpdateResponse.Error)
  
    - [SchedulingPolicy](#peloton.api.respool.SchedulingPolicy)
  
  
    - [ResourceManager](#peloton.api.respool.ResourceManager)
  

- [job.proto](#job.proto)
    - [CreateRequest](#peloton.api.job.CreateRequest)
    - [CreateResponse](#peloton.api.job.CreateResponse)
    - [CreateResponse.Error](#peloton.api.job.CreateResponse.Error)
    - [DeleteRequest](#peloton.api.job.DeleteRequest)
    - [DeleteResponse](#peloton.api.job.DeleteResponse)
    - [DeleteResponse.Error](#peloton.api.job.DeleteResponse.Error)
    - [GetRequest](#peloton.api.job.GetRequest)
    - [GetResponse](#peloton.api.job.GetResponse)
    - [GetResponse.Error](#peloton.api.job.GetResponse.Error)
    - [InvalidJobConfig](#peloton.api.job.InvalidJobConfig)
    - [InvalidJobId](#peloton.api.job.InvalidJobId)
    - [JobAlreadyExists](#peloton.api.job.JobAlreadyExists)
    - [JobConfig](#peloton.api.job.JobConfig)
    - [JobConfig.InstanceConfigEntry](#peloton.api.job.JobConfig.InstanceConfigEntry)
    - [JobInfo](#peloton.api.job.JobInfo)
    - [JobNotFound](#peloton.api.job.JobNotFound)
    - [QueryRequest](#peloton.api.job.QueryRequest)
    - [QueryResponse](#peloton.api.job.QueryResponse)
    - [QueryResponse.Error](#peloton.api.job.QueryResponse.Error)
    - [QuerySpec](#peloton.api.job.QuerySpec)
    - [RuntimeInfo](#peloton.api.job.RuntimeInfo)
    - [RuntimeInfo.TaskStatsEntry](#peloton.api.job.RuntimeInfo.TaskStatsEntry)
    - [SlaConfig](#peloton.api.job.SlaConfig)
    - [UpdateRequest](#peloton.api.job.UpdateRequest)
    - [UpdateResponse](#peloton.api.job.UpdateResponse)
    - [UpdateResponse.Error](#peloton.api.job.UpdateResponse.Error)
  
    - [JobState](#peloton.api.job.JobState)
    - [JobType](#peloton.api.job.JobType)
  
  
    - [JobManager](#peloton.api.job.JobManager)
  

- [update.proto](#update.proto)
    - [UpdateConfig](#peloton.api.update.UpdateConfig)
    - [UpdateID](#peloton.api.update.UpdateID)
    - [UpdateInfo](#peloton.api.update.UpdateInfo)
    - [UpdateStatus](#peloton.api.update.UpdateStatus)
  
    - [State](#peloton.api.update.State)
  
  
  

- [volume.proto](#volume.proto)
    - [PersistentVolumeInfo](#peloton.api.volume.PersistentVolumeInfo)
  
    - [VolumeState](#peloton.api.volume.VolumeState)
  
  
  

- [eventstream.proto](#eventstream.proto)
    - [ClientUnsupported](#peloton.private.eventstream.ClientUnsupported)
    - [Event](#peloton.private.eventstream.Event)
    - [InitStreamRequest](#peloton.private.eventstream.InitStreamRequest)
    - [InitStreamResponse](#peloton.private.eventstream.InitStreamResponse)
    - [InitStreamResponse.Error](#peloton.private.eventstream.InitStreamResponse.Error)
    - [InvalidPurgeOffset](#peloton.private.eventstream.InvalidPurgeOffset)
    - [InvalidStreamID](#peloton.private.eventstream.InvalidStreamID)
    - [OffsetOutOfRange](#peloton.private.eventstream.OffsetOutOfRange)
    - [WaitForEventsRequest](#peloton.private.eventstream.WaitForEventsRequest)
    - [WaitForEventsResponse](#peloton.private.eventstream.WaitForEventsResponse)
    - [WaitForEventsResponse.Error](#peloton.private.eventstream.WaitForEventsResponse.Error)
  
    - [Event.Type](#peloton.private.eventstream.Event.Type)
  
  
    - [EventStreamService](#peloton.private.eventstream.EventStreamService)
  

- [resmgr.proto](#resmgr.proto)
    - [Placement](#peloton.private.resmgr.Placement)
    - [Task](#peloton.private.resmgr.Task)
  
    - [TaskType](#peloton.private.resmgr.TaskType)
  
  
  

- [resmgrsvc.proto](#resmgrsvc.proto)
    - [DequeueGangsFailure](#peloton.private.resmgr.DequeueGangsFailure)
    - [DequeueGangsRequest](#peloton.private.resmgr.DequeueGangsRequest)
    - [DequeueGangsResponse](#peloton.private.resmgr.DequeueGangsResponse)
    - [DequeueGangsResponse.Error](#peloton.private.resmgr.DequeueGangsResponse.Error)
    - [EnqueueGangsFailure](#peloton.private.resmgr.EnqueueGangsFailure)
    - [EnqueueGangsFailure.FailedTask](#peloton.private.resmgr.EnqueueGangsFailure.FailedTask)
    - [EnqueueGangsRequest](#peloton.private.resmgr.EnqueueGangsRequest)
    - [EnqueueGangsResponse](#peloton.private.resmgr.EnqueueGangsResponse)
    - [EnqueueGangsResponse.Error](#peloton.private.resmgr.EnqueueGangsResponse.Error)
    - [Gang](#peloton.private.resmgr.Gang)
    - [GetActiveTasksRequest](#peloton.private.resmgr.GetActiveTasksRequest)
    - [GetActiveTasksResponse](#peloton.private.resmgr.GetActiveTasksResponse)
    - [GetActiveTasksResponse.Error](#peloton.private.resmgr.GetActiveTasksResponse.Error)
    - [GetActiveTasksResponse.TaskStatesMapEntry](#peloton.private.resmgr.GetActiveTasksResponse.TaskStatesMapEntry)
    - [GetPlacementsFailure](#peloton.private.resmgr.GetPlacementsFailure)
    - [GetPlacementsRequest](#peloton.private.resmgr.GetPlacementsRequest)
    - [GetPlacementsResponse](#peloton.private.resmgr.GetPlacementsResponse)
    - [GetPlacementsResponse.Error](#peloton.private.resmgr.GetPlacementsResponse.Error)
    - [GetPreemptibleTasksFailure](#peloton.private.resmgr.GetPreemptibleTasksFailure)
    - [GetPreemptibleTasksRequest](#peloton.private.resmgr.GetPreemptibleTasksRequest)
    - [GetPreemptibleTasksResponse](#peloton.private.resmgr.GetPreemptibleTasksResponse)
    - [GetPreemptibleTasksResponse.Error](#peloton.private.resmgr.GetPreemptibleTasksResponse.Error)
    - [GetTasksByHostsRequest](#peloton.private.resmgr.GetTasksByHostsRequest)
    - [GetTasksByHostsResponse](#peloton.private.resmgr.GetTasksByHostsResponse)
    - [GetTasksByHostsResponse.Error](#peloton.private.resmgr.GetTasksByHostsResponse.Error)
    - [GetTasksByHostsResponse.HostTasksMapEntry](#peloton.private.resmgr.GetTasksByHostsResponse.HostTasksMapEntry)
    - [KillTasksError](#peloton.private.resmgr.KillTasksError)
    - [KillTasksRequest](#peloton.private.resmgr.KillTasksRequest)
    - [KillTasksResponse](#peloton.private.resmgr.KillTasksResponse)
    - [KillTasksResponse.Error](#peloton.private.resmgr.KillTasksResponse.Error)
    - [NotifyTaskUpdatesError](#peloton.private.resmgr.NotifyTaskUpdatesError)
    - [NotifyTaskUpdatesRequest](#peloton.private.resmgr.NotifyTaskUpdatesRequest)
    - [NotifyTaskUpdatesResponse](#peloton.private.resmgr.NotifyTaskUpdatesResponse)
    - [NotifyTaskUpdatesResponse.Error](#peloton.private.resmgr.NotifyTaskUpdatesResponse.Error)
    - [RequestTimedout](#peloton.private.resmgr.RequestTimedout)
    - [ResourcePoolNoPermission](#peloton.private.resmgr.ResourcePoolNoPermission)
    - [ResourcePoolNotFound](#peloton.private.resmgr.ResourcePoolNotFound)
    - [SetPlacementsFailure](#peloton.private.resmgr.SetPlacementsFailure)
    - [SetPlacementsFailure.FailedPlacement](#peloton.private.resmgr.SetPlacementsFailure.FailedPlacement)
    - [SetPlacementsRequest](#peloton.private.resmgr.SetPlacementsRequest)
    - [SetPlacementsResponse](#peloton.private.resmgr.SetPlacementsResponse)
    - [SetPlacementsResponse.Error](#peloton.private.resmgr.SetPlacementsResponse.Error)
    - [TaskList](#peloton.private.resmgr.TaskList)
    - [TasksNotFound](#peloton.private.resmgr.TasksNotFound)
  
  
  
    - [ResourceManagerService](#peloton.private.resmgr.ResourceManagerService)
  

- [job_svc.proto](#job_svc.proto)
    - [CreateJobRequest](#peloton.api.job.svc.CreateJobRequest)
    - [CreateJobResponse](#peloton.api.job.svc.CreateJobResponse)
    - [DeleteJobRequest](#peloton.api.job.svc.DeleteJobRequest)
    - [DeleteJobResponse](#peloton.api.job.svc.DeleteJobResponse)
    - [GetJobRequest](#peloton.api.job.svc.GetJobRequest)
    - [GetJobResponse](#peloton.api.job.svc.GetJobResponse)
    - [QueryJobsRequest](#peloton.api.job.svc.QueryJobsRequest)
    - [QueryJobsResponse](#peloton.api.job.svc.QueryJobsResponse)
    - [UpdateJobRequest](#peloton.api.job.svc.UpdateJobRequest)
    - [UpdateJobResponse](#peloton.api.job.svc.UpdateJobResponse)
  
  
  
    - [JobService](#peloton.api.job.svc.JobService)
  

- [respool_svc.proto](#respool_svc.proto)
    - [CreateResourcePoolRequest](#peloton.api.respool.CreateResourcePoolRequest)
    - [CreateResourcePoolResponse](#peloton.api.respool.CreateResourcePoolResponse)
    - [DeleteResourcePoolRequest](#peloton.api.respool.DeleteResourcePoolRequest)
    - [DeleteResourcePoolResponse](#peloton.api.respool.DeleteResourcePoolResponse)
    - [GetResourcePoolRequest](#peloton.api.respool.GetResourcePoolRequest)
    - [GetResourcePoolResponse](#peloton.api.respool.GetResourcePoolResponse)
    - [LookupResourcePoolIDRequest](#peloton.api.respool.LookupResourcePoolIDRequest)
    - [LookupResourcePoolIDResponse](#peloton.api.respool.LookupResourcePoolIDResponse)
    - [QueryResourcePoolsRequest](#peloton.api.respool.QueryResourcePoolsRequest)
    - [QueryResourcePoolsResponse](#peloton.api.respool.QueryResourcePoolsResponse)
    - [UpdateResourcePoolRequest](#peloton.api.respool.UpdateResourcePoolRequest)
    - [UpdateResourcePoolResponse](#peloton.api.respool.UpdateResourcePoolResponse)
  
  
  
    - [ResourcePoolService](#peloton.api.respool.ResourcePoolService)
  

- [task_svc.proto](#task_svc.proto)
    - [BrowseSandboxRequest](#peloton.api.task.svc.BrowseSandboxRequest)
    - [BrowseSandboxResponse](#peloton.api.task.svc.BrowseSandboxResponse)
    - [GetEventsRequest](#peloton.api.task.svc.GetEventsRequest)
    - [GetEventsResponse](#peloton.api.task.svc.GetEventsResponse)
    - [GetEventsResponse.Error](#peloton.api.task.svc.GetEventsResponse.Error)
    - [GetEventsResponse.Events](#peloton.api.task.svc.GetEventsResponse.Events)
    - [GetTaskRequest](#peloton.api.task.svc.GetTaskRequest)
    - [GetTaskResponse](#peloton.api.task.svc.GetTaskResponse)
    - [ListTasksRequest](#peloton.api.task.svc.ListTasksRequest)
    - [ListTasksResponse](#peloton.api.task.svc.ListTasksResponse)
    - [ListTasksResponse.TasksEntry](#peloton.api.task.svc.ListTasksResponse.TasksEntry)
    - [QueryTasksRequest](#peloton.api.task.svc.QueryTasksRequest)
    - [QueryTasksResponse](#peloton.api.task.svc.QueryTasksResponse)
    - [RestartTasksRequest](#peloton.api.task.svc.RestartTasksRequest)
    - [RestartTasksResponse](#peloton.api.task.svc.RestartTasksResponse)
    - [StartTasksRequest](#peloton.api.task.svc.StartTasksRequest)
    - [StartTasksResponse](#peloton.api.task.svc.StartTasksResponse)
    - [StopTasksRequest](#peloton.api.task.svc.StopTasksRequest)
    - [StopTasksResponse](#peloton.api.task.svc.StopTasksResponse)
    - [TaskEventsError](#peloton.api.task.svc.TaskEventsError)
  
  
  
    - [TaskService](#peloton.api.task.svc.TaskService)
  

- [update_svc.proto](#update_svc.proto)
    - [AbortUpdateRequest](#peloton.api.update.svc.AbortUpdateRequest)
    - [AbortUpdateResponse](#peloton.api.update.svc.AbortUpdateResponse)
    - [CreateUpdateRequest](#peloton.api.update.svc.CreateUpdateRequest)
    - [CreateUpdateResponse](#peloton.api.update.svc.CreateUpdateResponse)
    - [GetUpdateRequest](#peloton.api.update.svc.GetUpdateRequest)
    - [GetUpdateResponse](#peloton.api.update.svc.GetUpdateResponse)
    - [ListUpdatesRequest](#peloton.api.update.svc.ListUpdatesRequest)
    - [ListUpdatesResponse](#peloton.api.update.svc.ListUpdatesResponse)
    - [PauseUpdateRequest](#peloton.api.update.svc.PauseUpdateRequest)
    - [PauseUpdateResponse](#peloton.api.update.svc.PauseUpdateResponse)
    - [ResumeUpdateRequest](#peloton.api.update.svc.ResumeUpdateRequest)
    - [ResumeUpdateResponse](#peloton.api.update.svc.ResumeUpdateResponse)
    - [RollbackUpdateRequest](#peloton.api.update.svc.RollbackUpdateRequest)
    - [RollbackUpdateResponse](#peloton.api.update.svc.RollbackUpdateResponse)
  
  
  
    - [UpdateService](#peloton.api.update.svc.UpdateService)
  

- [volume_svc.proto](#volume_svc.proto)
    - [DeleteVolumeRequest](#peloton.api.volume.svc.DeleteVolumeRequest)
    - [DeleteVolumeResponse](#peloton.api.volume.svc.DeleteVolumeResponse)
    - [GetVolumeRequest](#peloton.api.volume.svc.GetVolumeRequest)
    - [GetVolumeResponse](#peloton.api.volume.svc.GetVolumeResponse)
    - [ListVolumesRequest](#peloton.api.volume.svc.ListVolumesRequest)
    - [ListVolumesResponse](#peloton.api.volume.svc.ListVolumesResponse)
    - [ListVolumesResponse.VolumesEntry](#peloton.api.volume.svc.ListVolumesResponse.VolumesEntry)
  
  
  
    - [VolumeService](#peloton.api.volume.svc.VolumeService)
  

- [hostsvc.proto](#hostsvc.proto)
    - [AcquireHostOffersFailure](#peloton.private.hostmgr.hostsvc.AcquireHostOffersFailure)
    - [AcquireHostOffersRequest](#peloton.private.hostmgr.hostsvc.AcquireHostOffersRequest)
    - [AcquireHostOffersResponse](#peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse)
    - [AcquireHostOffersResponse.Error](#peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.Error)
    - [AcquireHostOffersResponse.FilterResultCountsEntry](#peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.FilterResultCountsEntry)
    - [ClusterCapacityRequest](#peloton.private.hostmgr.hostsvc.ClusterCapacityRequest)
    - [ClusterCapacityResponse](#peloton.private.hostmgr.hostsvc.ClusterCapacityResponse)
    - [ClusterCapacityResponse.Error](#peloton.private.hostmgr.hostsvc.ClusterCapacityResponse.Error)
    - [ClusterUnavailable](#peloton.private.hostmgr.hostsvc.ClusterUnavailable)
    - [CreateVolumesRequest](#peloton.private.hostmgr.hostsvc.CreateVolumesRequest)
    - [CreateVolumesResponse](#peloton.private.hostmgr.hostsvc.CreateVolumesResponse)
    - [DestroyVolumesRequest](#peloton.private.hostmgr.hostsvc.DestroyVolumesRequest)
    - [DestroyVolumesResponse](#peloton.private.hostmgr.hostsvc.DestroyVolumesResponse)
    - [ExecutorOnAgent](#peloton.private.hostmgr.hostsvc.ExecutorOnAgent)
    - [HostFilter](#peloton.private.hostmgr.hostsvc.HostFilter)
    - [HostOffer](#peloton.private.hostmgr.hostsvc.HostOffer)
    - [InvalidArgument](#peloton.private.hostmgr.hostsvc.InvalidArgument)
    - [InvalidExecutors](#peloton.private.hostmgr.hostsvc.InvalidExecutors)
    - [InvalidHostFilter](#peloton.private.hostmgr.hostsvc.InvalidHostFilter)
    - [InvalidOffers](#peloton.private.hostmgr.hostsvc.InvalidOffers)
    - [InvalidTaskIDs](#peloton.private.hostmgr.hostsvc.InvalidTaskIDs)
    - [KillFailure](#peloton.private.hostmgr.hostsvc.KillFailure)
    - [KillTasksRequest](#peloton.private.hostmgr.hostsvc.KillTasksRequest)
    - [KillTasksResponse](#peloton.private.hostmgr.hostsvc.KillTasksResponse)
    - [KillTasksResponse.Error](#peloton.private.hostmgr.hostsvc.KillTasksResponse.Error)
    - [LaunchFailure](#peloton.private.hostmgr.hostsvc.LaunchFailure)
    - [LaunchTasksRequest](#peloton.private.hostmgr.hostsvc.LaunchTasksRequest)
    - [LaunchTasksResponse](#peloton.private.hostmgr.hostsvc.LaunchTasksResponse)
    - [LaunchTasksResponse.Error](#peloton.private.hostmgr.hostsvc.LaunchTasksResponse.Error)
    - [LaunchableTask](#peloton.private.hostmgr.hostsvc.LaunchableTask)
    - [LaunchableTask.PortsEntry](#peloton.private.hostmgr.hostsvc.LaunchableTask.PortsEntry)
    - [OfferOperation](#peloton.private.hostmgr.hostsvc.OfferOperation)
    - [OfferOperation.Create](#peloton.private.hostmgr.hostsvc.OfferOperation.Create)
    - [OfferOperation.Destroy](#peloton.private.hostmgr.hostsvc.OfferOperation.Destroy)
    - [OfferOperation.Launch](#peloton.private.hostmgr.hostsvc.OfferOperation.Launch)
    - [OfferOperation.Reserve](#peloton.private.hostmgr.hostsvc.OfferOperation.Reserve)
    - [OfferOperation.Unreserve](#peloton.private.hostmgr.hostsvc.OfferOperation.Unreserve)
    - [OfferOperationsRequest](#peloton.private.hostmgr.hostsvc.OfferOperationsRequest)
    - [OfferOperationsResponse](#peloton.private.hostmgr.hostsvc.OfferOperationsResponse)
    - [OfferOperationsResponse.Error](#peloton.private.hostmgr.hostsvc.OfferOperationsResponse.Error)
    - [OperationsFailure](#peloton.private.hostmgr.hostsvc.OperationsFailure)
    - [QuantityControl](#peloton.private.hostmgr.hostsvc.QuantityControl)
    - [ReleaseHostOffersRequest](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersRequest)
    - [ReleaseHostOffersResponse](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersResponse)
    - [ReleaseHostOffersResponse.Error](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersResponse.Error)
    - [ReserveResourcesRequest](#peloton.private.hostmgr.hostsvc.ReserveResourcesRequest)
    - [ReserveResourcesResponse](#peloton.private.hostmgr.hostsvc.ReserveResourcesResponse)
    - [Resource](#peloton.private.hostmgr.hostsvc.Resource)
    - [ResourceConstraint](#peloton.private.hostmgr.hostsvc.ResourceConstraint)
    - [ShutdownExecutorsRequest](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsRequest)
    - [ShutdownExecutorsResponse](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsResponse)
    - [ShutdownExecutorsResponse.Error](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsResponse.Error)
    - [ShutdownFailure](#peloton.private.hostmgr.hostsvc.ShutdownFailure)
    - [UnreserveResourcesRequest](#peloton.private.hostmgr.hostsvc.UnreserveResourcesRequest)
    - [UnreserveResourcesResponse](#peloton.private.hostmgr.hostsvc.UnreserveResourcesResponse)
    - [Volume](#peloton.private.hostmgr.hostsvc.Volume)
  
    - [HostFilterResult](#peloton.private.hostmgr.hostsvc.HostFilterResult)
    - [OfferOperation.Type](#peloton.private.hostmgr.hostsvc.OfferOperation.Type)
  
  
    - [InternalHostService](#peloton.private.hostmgr.hostsvc.InternalHostService)
  

- [taskqueue.proto](#taskqueue.proto)
    - [DequeueRequest](#peloton.private.resmgr.taskqueue.DequeueRequest)
    - [DequeueResponse](#peloton.private.resmgr.taskqueue.DequeueResponse)
    - [EnqueueRequest](#peloton.private.resmgr.taskqueue.EnqueueRequest)
    - [EnqueueResponse](#peloton.private.resmgr.taskqueue.EnqueueResponse)
  
  
  
    - [TaskQueue](#peloton.private.resmgr.taskqueue.TaskQueue)
  

- [Scalar Value Types](#scalar-value-types)



<a name="mesos.proto"/>
<p align="right"><a href="#top">Top</a></p>

## mesos.proto



<a name="mesos.v1.Address"/>

### Address
A network address.

TODO(bmahler): Use this more widely.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) | optional | May contain a hostname, IP address, or both. |
| ip | [string](#string) | optional |  |
| port | [int32](#int32) | required |  |






<a name="mesos.v1.AgentID"/>

### AgentID
A unique ID assigned to an agent. Currently, an agent gets a new ID
whenever it (re)registers with Mesos. Framework writers shouldn&#39;t
assume any binding between an agent ID and and a hostname.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.AgentInfo"/>

### AgentInfo
Describes an agent. Note that the &#39;id&#39; field is only available
after an agent is registered with the master, and is made available
here to facilitate re-registration.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) | required |  |
| port | [int32](#int32) | optional |  |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| attributes | [Attribute](#mesos.v1.Attribute) | repeated |  |
| id | [AgentID](#mesos.v1.AgentID) | optional |  |






<a name="mesos.v1.AgentInfo.Capability"/>

### AgentInfo.Capability



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [AgentInfo.Capability.Type](#mesos.v1.AgentInfo.Capability.Type) | optional | Enum fields should be optional, see: MESOS-4997. |






<a name="mesos.v1.Attribute"/>

### Attribute
Describes an attribute that can be set on a machine. For now,
attributes and resources share the same &#34;value&#34; type, but this may
change in the future and attributes may only be string based.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| type | [Value.Type](#mesos.v1.Value.Type) | required |  |
| scalar | [Value.Scalar](#mesos.v1.Value.Scalar) | optional |  |
| ranges | [Value.Ranges](#mesos.v1.Value.Ranges) | optional |  |
| set | [Value.Set](#mesos.v1.Value.Set) | optional |  |
| text | [Value.Text](#mesos.v1.Value.Text) | optional |  |






<a name="mesos.v1.CapabilityInfo"/>

### CapabilityInfo
Encapsulation of `Capabilities` supported by Linux.
Reference: http://linux.die.net/man/7/capabilities.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| capabilities | [CapabilityInfo.Capability](#mesos.v1.CapabilityInfo.Capability) | repeated |  |






<a name="mesos.v1.CgroupInfo"/>

### CgroupInfo
Linux control group (cgroup) information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| net_cls | [CgroupInfo.NetCls](#mesos.v1.CgroupInfo.NetCls) | optional |  |






<a name="mesos.v1.CgroupInfo.NetCls"/>

### CgroupInfo.NetCls
Configuration of a net_cls cgroup subsystem.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| classid | [uint32](#uint32) | optional | The 32-bit classid consists of two parts, a 16 bit major handle and a 16-bit minor handle. The major and minor handle are represented using the format 0xAAAABBBB, where 0xAAAA is the 16-bit major handle and 0xBBBB is the 16-bit minor handle. |






<a name="mesos.v1.CheckInfo"/>

### CheckInfo
Describes a general non-interpreting non-killing check for a task or
executor (or any arbitrary process/command). A type is picked by
specifying one of the optional fields. Specifying more than one type
is an error.

NOTE: This API is unstable and the related feature is experimental.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [CheckInfo.Type](#mesos.v1.CheckInfo.Type) | optional | The type of the check. |
| command | [CheckInfo.Command](#mesos.v1.CheckInfo.Command) | optional | Command check. |
| http | [CheckInfo.Http](#mesos.v1.CheckInfo.Http) | optional | HTTP check. |
| delay_seconds | [double](#double) | optional | Amount of time to wait to start checking the task after it transitions to `TASK_RUNNING` or `TASK_STARTING` if the latter is used by the executor. |
| interval_seconds | [double](#double) | optional | Interval between check attempts, i.e., amount of time to wait after the previous check finished or timed out to start the next check. |
| timeout_seconds | [double](#double) | optional | Amount of time to wait for the check to complete. Zero means infinite timeout.

After this timeout, the check attempt is aborted and no result is reported. Note that this may be considered a state change and hence may trigger a check status change delivery to the corresponding scheduler. See `CheckStatusInfo` for more details. |






<a name="mesos.v1.CheckInfo.Command"/>

### CheckInfo.Command
Describes a command check. If applicable, enters mount and/or network
namespaces of the task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [CommandInfo](#mesos.v1.CommandInfo) | required |  |






<a name="mesos.v1.CheckInfo.Http"/>

### CheckInfo.Http
Describes an HTTP check. Sends a GET request to
http://&lt;host&gt;:port/path. Note that &lt;host&gt; is not configurable and is
resolved automatically to 127.0.0.1.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| port | [uint32](#uint32) | required | Port to send the HTTP request. |
| path | [string](#string) | optional | HTTP request path. |






<a name="mesos.v1.CheckStatusInfo"/>

### CheckStatusInfo
Describes the status of a check. Type and the corresponding field, i.e.,
`command` or `http` must be set. If the result of the check is not available
(e.g., the check timed out), these fields must contain empty messages, i.e.,
`exit_code` or `status_code` will be unset.

NOTE: This API is unstable and the related feature is experimental.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [CheckInfo.Type](#mesos.v1.CheckInfo.Type) | optional | The type of the check this status corresponds to. |
| command | [CheckStatusInfo.Command](#mesos.v1.CheckStatusInfo.Command) | optional | Status of a command check. |
| http | [CheckStatusInfo.Http](#mesos.v1.CheckStatusInfo.Http) | optional | Status of an HTTP check. |






<a name="mesos.v1.CheckStatusInfo.Command"/>

### CheckStatusInfo.Command



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exit_code | [int32](#int32) | optional | Exit code of a command check. |






<a name="mesos.v1.CheckStatusInfo.Http"/>

### CheckStatusInfo.Http



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status_code | [uint32](#uint32) | optional | HTTP status code of an HTTP check. |






<a name="mesos.v1.CommandInfo"/>

### CommandInfo
Describes a command, executed via: &#39;/bin/sh -c value&#39;. Any URIs specified
are fetched before executing the command.  If the executable field for an
uri is set, executable file permission is set on the downloaded file.
Otherwise, if the downloaded file has a recognized archive extension
(currently [compressed] tar and zip) it is extracted into the executor&#39;s
working directory. This extraction can be disabled by setting `extract` to
false. In addition, any environment variables are set before executing
the command (so they can be used to &#34;parameterize&#34; your command).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| uris | [CommandInfo.URI](#mesos.v1.CommandInfo.URI) | repeated |  |
| environment | [Environment](#mesos.v1.Environment) | optional |  |
| shell | [bool](#bool) | optional | There are two ways to specify the command: 1) If &#39;shell == true&#39;, the command will be launched via shell 		(i.e., /bin/sh -c &#39;value&#39;). The &#39;value&#39; specified will be 		treated as the shell command. The &#39;arguments&#39; will be ignored. 2) If &#39;shell == false&#39;, the command will be launched by passing 		arguments to an executable. The &#39;value&#39; specified will be 		treated as the filename of the executable. The &#39;arguments&#39; 		will be treated as the arguments to the executable. This is 		similar to how POSIX exec families launch processes (i.e., 		execlp(value, arguments(0), arguments(1), ...)). NOTE: The field &#39;value&#39; is changed from &#39;required&#39; to &#39;optional&#39; in 0.20.0. It will only cause issues if a new framework is connecting to an old master. |
| value | [string](#string) | optional |  |
| arguments | [string](#string) | repeated |  |
| user | [string](#string) | optional | Enables executor and tasks to run as a specific user. If the user field is present both in FrameworkInfo and here, the CommandInfo user value takes precedence. |






<a name="mesos.v1.CommandInfo.URI"/>

### CommandInfo.URI



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |
| executable | [bool](#bool) | optional |  |
| extract | [bool](#bool) | optional | In case the fetched file is recognized as an archive, extract its contents into the sandbox. Note that a cached archive is not copied from the cache to the sandbox in case extraction originates from an archive in the cache. |
| cache | [bool](#bool) | optional | If this field is &#34;true&#34;, the fetcher cache will be used. If not, fetching bypasses the cache and downloads directly into the sandbox directory, no matter whether a suitable cache file is available or not. The former directs the fetcher to download to the file cache, then copy from there to the sandbox. Subsequent fetch attempts with the same URI will omit downloading and copy from the cache as long as the file is resident there. Cache files may get evicted at any time, which then leads to renewed downloading. See also &#34;docs/fetcher.md&#34; and &#34;docs/fetcher-cache-internals.md&#34;. |
| output_file | [string](#string) | optional | The fetcher&#39;s default behavior is to use the URI string&#39;s basename to name the local copy. If this field is provided, the local copy will be named with its value instead. If there is a directory component (which must be a relative path), the local copy will be stored in that subdirectory inside the sandbox. |






<a name="mesos.v1.ContainerID"/>

### ContainerID
ID used to uniquely identify a container. If the `parent` is not
specified, the ID is a UUID generated by the agent to uniquely
identify the container of an executor run. If the `parent` field is
specified, it represents a nested container.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |
| parent | [ContainerID](#mesos.v1.ContainerID) | optional |  |






<a name="mesos.v1.ContainerInfo"/>

### ContainerInfo
Describes a container configuration and allows extensible
configurations for different container implementations.

NOTE: `ContainerInfo` may be specified, e.g., by a task, even if no
container image is provided. In this case neither `MesosInfo` nor
`DockerInfo` is set, the required `type` must be `MESOS`. This is to
address a case when a task without an image, e.g., a shell script
with URIs, wants to use features originally designed for containers,
for example custom network isolation via `NetworkInfo`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ContainerInfo.Type](#mesos.v1.ContainerInfo.Type) | required |  |
| volumes | [Volume](#mesos.v1.Volume) | repeated |  |
| hostname | [string](#string) | optional |  |
| docker | [ContainerInfo.DockerInfo](#mesos.v1.ContainerInfo.DockerInfo) | optional | Only one of the following *Info messages should be set to match the type. |
| mesos | [ContainerInfo.MesosInfo](#mesos.v1.ContainerInfo.MesosInfo) | optional |  |
| network_infos | [NetworkInfo](#mesos.v1.NetworkInfo) | repeated | A list of network requests. A framework can request multiple IP addresses for the container. |
| linux_info | [LinuxInfo](#mesos.v1.LinuxInfo) | optional | Linux specific information for the container. |
| rlimit_info | [RLimitInfo](#mesos.v1.RLimitInfo) | optional | (POSIX only) rlimits of the container. |
| tty_info | [TTYInfo](#mesos.v1.TTYInfo) | optional | If specified a tty will be attached to the container entrypoint. |






<a name="mesos.v1.ContainerInfo.DockerInfo"/>

### ContainerInfo.DockerInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| image | [string](#string) | required | The docker image that is going to be passed to the registry. |
| network | [ContainerInfo.DockerInfo.Network](#mesos.v1.ContainerInfo.DockerInfo.Network) | optional |  |
| port_mappings | [ContainerInfo.DockerInfo.PortMapping](#mesos.v1.ContainerInfo.DockerInfo.PortMapping) | repeated |  |
| privileged | [bool](#bool) | optional |  |
| parameters | [Parameter](#mesos.v1.Parameter) | repeated | Allowing arbitrary parameters to be passed to docker CLI. Note that anything passed to this field is not guaranteed to be supported moving forward, as we might move away from the docker CLI. |
| force_pull_image | [bool](#bool) | optional | With this flag set to true, the docker containerizer will pull the docker image from the registry even if the image is already downloaded on the agent. |
| volume_driver | [string](#string) | optional | The name of volume driver plugin.

Since 1.0 |






<a name="mesos.v1.ContainerInfo.DockerInfo.PortMapping"/>

### ContainerInfo.DockerInfo.PortMapping



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_port | [uint32](#uint32) | required |  |
| container_port | [uint32](#uint32) | required |  |
| protocol | [string](#string) | optional | Protocol to expose as (ie: tcp, udp). |






<a name="mesos.v1.ContainerInfo.MesosInfo"/>

### ContainerInfo.MesosInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| image | [Image](#mesos.v1.Image) | optional |  |






<a name="mesos.v1.ContainerStatus"/>

### ContainerStatus
Container related information that is resolved during container
setup. The information is sent back to the framework as part of the
TaskStatus message.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [ContainerID](#mesos.v1.ContainerID) | optional |  |
| network_infos | [NetworkInfo](#mesos.v1.NetworkInfo) | repeated | This field can be reliably used to identify the container IP address. |
| cgroup_info | [CgroupInfo](#mesos.v1.CgroupInfo) | optional | Information about Linux control group (cgroup). |
| executor_pid | [uint32](#uint32) | optional | Information about Executor PID. |






<a name="mesos.v1.Credential"/>

### Credential
Credential used in various places for authentication and
authorization.

NOTE: A &#39;principal&#39; is different from &#39;FrameworkInfo.user&#39;. The
former is used for authentication and authorization while the
latter is used to determine the default user under which the
framework&#39;s executors/tasks are run.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| principal | [string](#string) | required |  |
| secret | [string](#string) | optional |  |






<a name="mesos.v1.Credentials"/>

### Credentials
Credentials used for framework authentication, HTTP authentication
(where the common &#39;username&#39; and &#39;password&#39; are captured as
&#39;principal&#39; and &#39;secret&#39; respectively), etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| credentials | [Credential](#mesos.v1.Credential) | repeated |  |






<a name="mesos.v1.DiscoveryInfo"/>

### DiscoveryInfo
Service discovery information.
The visibility field restricts discovery within a framework (FRAMEWORK),
within a Mesos cluster (CLUSTER), or places no restrictions (EXTERNAL).
Each port in the ports field also has an optional visibility field.
If visibility is specified for a port, it overrides the default service-wide
DiscoveryInfo.visibility for that port.
The environment, location, and version fields provide first class support for
common attributes used to differentiate between similar services. The
environment may receive values such as PROD/QA/DEV, the location field may
receive values like EAST-US/WEST-US/EUROPE/AMEA, and the version field may
receive values like v2.0/v0.9. The exact use of these fields is up to each
service discovery system.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| visibility | [DiscoveryInfo.Visibility](#mesos.v1.DiscoveryInfo.Visibility) | required |  |
| name | [string](#string) | optional |  |
| environment | [string](#string) | optional |  |
| location | [string](#string) | optional |  |
| version | [string](#string) | optional |  |
| ports | [Ports](#mesos.v1.Ports) | optional |  |
| labels | [Labels](#mesos.v1.Labels) | optional |  |






<a name="mesos.v1.DurationInfo"/>

### DurationInfo
Represents duration in nanoseconds.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nanoseconds | [int64](#int64) | required |  |






<a name="mesos.v1.Environment"/>

### Environment
Describes a collection of environment variables. This is used with
CommandInfo in order to set environment variables before running a
command. The contents of each variable may be specified as a string
or a Secret; only one of `value` and `secret` must be set.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| variables | [Environment.Variable](#mesos.v1.Environment.Variable) | repeated |  |






<a name="mesos.v1.Environment.Variable"/>

### Environment.Variable



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| type | [Environment.Variable.Type](#mesos.v1.Environment.Variable.Type) | optional | In Mesos 1.2, the `Environment.variables.value` message was made optional. The default type for `Environment.variables.type` is now VALUE, which requires `value` to be set, maintaining backward compatibility.

TODO(greggomann): The default can be removed in Mesos 2.1 (MESOS-7134). |
| value | [string](#string) | optional | Only one of `value` and `secret` must be set. |
| secret | [Secret](#mesos.v1.Secret) | optional |  |






<a name="mesos.v1.ExecutorID"/>

### ExecutorID
A framework-generated ID to distinguish an executor. Only one
executor with the same ID can be active on the same agent at a
time. However, reusing executor IDs is discouraged.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.ExecutorInfo"/>

### ExecutorInfo
Describes information about an executor.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ExecutorInfo.Type](#mesos.v1.ExecutorInfo.Type) | optional | For backwards compatibility, if this field is not set when using `LAUNCH` offer operation, Mesos will infer the type by checking if `command` is set (`CUSTOM`) or unset (`DEFAULT`). `type` must be set when using `LAUNCH_GROUP` offer operation.

TODO(vinod): Add support for explicitly setting `type` to `DEFAULT ` in `LAUNCH` offer operation. |
| executor_id | [ExecutorID](#mesos.v1.ExecutorID) | required |  |
| framework_id | [FrameworkID](#mesos.v1.FrameworkID) | optional | TODO(benh): Make this required. |
| command | [CommandInfo](#mesos.v1.CommandInfo) | optional |  |
| container | [ContainerInfo](#mesos.v1.ContainerInfo) | optional | Executor provided with a container will launch the container with the executor&#39;s CommandInfo and we expect the container to act as a Mesos executor. |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| name | [string](#string) | optional |  |
| source | [string](#string) | optional | &#39;source&#39; is an identifier style string used by frameworks to track the source of an executor. This is useful when it&#39;s possible for different executor ids to be related semantically.

NOTE: &#39;source&#39; is exposed alongside the resource usage of the executor via JSON on the agent. This allows users to import usage information into a time series database for monitoring.

This field is deprecated since 1.0. Please use labels for free-form metadata instead.

Since 1.0. |
| data | [bytes](#bytes) | optional | This field can be used to pass arbitrary bytes to an executor. |
| discovery | [DiscoveryInfo](#mesos.v1.DiscoveryInfo) | optional | Service discovery information for the executor. It is not interpreted or acted upon by Mesos. It is up to a service discovery system to use this information as needed and to handle executors without service discovery information. |
| shutdown_grace_period | [DurationInfo](#mesos.v1.DurationInfo) | optional | When shutting down an executor the agent will wait in a best-effort manner for the grace period specified here before forcibly destroying the container. The executor must not assume that it will always be allotted the full grace period, as the agent may decide to allot a shorter period and failures / forcible terminations may occur. |
| labels | [Labels](#mesos.v1.Labels) | optional | Labels are free-form key value pairs which are exposed through master and agent endpoints. Labels will not be interpreted or acted upon by Mesos itself. As opposed to the data field, labels will be kept in memory on master and agent processes. Therefore, labels should be used to tag executors with lightweight metadata. Labels should not contain duplicate key-value pairs. |






<a name="mesos.v1.FileInfo"/>

### FileInfo
Describes a File.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | required | Absolute path to the file. |
| nlink | [int32](#int32) | optional | Number of hard links. |
| size | [uint64](#uint64) | optional | Total size in bytes. |
| mtime | [TimeInfo](#mesos.v1.TimeInfo) | optional | Last modification time. |
| mode | [uint32](#uint32) | optional | Represents a file&#39;s mode and permission bits. The bits have the same definition on all systems and is portable. |
| uid | [string](#string) | optional | User ID of owner. |
| gid | [string](#string) | optional | Group ID of owner. |






<a name="mesos.v1.Filters"/>

### Filters
Describes possible filters that can be applied to unused resources
(see SchedulerDriver::launchTasks) to influence the allocator.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| refuse_seconds | [double](#double) | optional | Time to consider unused resources refused. Note that all unused resources will be considered refused and use the default value (below) regardless of whether Filters was passed to SchedulerDriver::launchTasks. You MUST pass Filters with this field set to change this behavior (i.e., get another offer which includes unused resources sooner or later than the default). |






<a name="mesos.v1.Flag"/>

### Flag
Flag consists of a name and optionally its value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| value | [string](#string) | optional |  |






<a name="mesos.v1.FrameworkID"/>

### FrameworkID
A unique ID assigned to a framework. A framework can reuse this ID
in order to do failover (see MesosSchedulerDriver).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.FrameworkInfo"/>

### FrameworkInfo
Describes a framework.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| user | [string](#string) | required | Used to determine the Unix user that an executor or task should be launched as.

When using the MesosSchedulerDriver, if the field is set to an empty string, it will automagically set it to the current user.

When using the HTTP Scheduler API, the user has to be set explicitly. |
| name | [string](#string) | required | Name of the framework that shows up in the Mesos Web UI. |
| id | [FrameworkID](#mesos.v1.FrameworkID) | optional | Note that &#39;id&#39; is only available after a framework has registered, however, it is included here in order to facilitate scheduler failover (i.e., if it is set then the MesosSchedulerDriver expects the scheduler is performing failover). |
| failover_timeout | [double](#double) | optional | The amount of time (in seconds) that the master will wait for the scheduler to failover before it tears down the framework by killing all its tasks/executors. This should be non-zero if a framework expects to reconnect after a failure and not lose its tasks/executors.

NOTE: To avoid accidental destruction of tasks, production frameworks typically set this to a large value (e.g., 1 week). |
| checkpoint | [bool](#bool) | optional | If set, agents running tasks started by this framework will write the framework pid, executor pids and status updates to disk. If the agent exits (e.g., due to a crash or as part of upgrading Mesos), this checkpointed data allows the restarted agent to reconnect to executors that were started by the old instance of the agent. Enabling checkpointing improves fault tolerance, at the cost of a (usually small) increase in disk I/O. |
| role | [string](#string) | optional | Roles are the entities to which allocations are made. The framework must have at least one role in order to be offered resources. Note that `role` is deprecated in favor of `roles` and only one of these fields must be used. Since we cannot distinguish between empty `roles` and the default unset `role`, we require that frameworks set the `MULTI_ROLE` capability if setting the `roles` field.

NOTE: The implmentation for supporting `roles` is not complete, DO NOT USE the `roles` field. |
| roles | [string](#string) | repeated | EXPERIMENTAL. |
| hostname | [string](#string) | optional | Used to indicate the current host from which the scheduler is registered in the Mesos Web UI. If set to an empty string Mesos will automagically set it to the current hostname if one is available. |
| principal | [string](#string) | optional | This field should match the credential&#39;s principal the framework uses for authentication. This field is used for framework API rate limiting and dynamic reservations. It should be set even if authentication is not enabled if these features are desired. |
| webui_url | [string](#string) | optional | This field allows a framework to advertise its web UI, so that the Mesos web UI can link to it. It is expected to be a full URL, for example http://my-scheduler.example.com:8080/. |
| capabilities | [FrameworkInfo.Capability](#mesos.v1.FrameworkInfo.Capability) | repeated | This field allows a framework to advertise its set of capabilities (e.g., ability to receive offers for revocable resources). |
| labels | [Labels](#mesos.v1.Labels) | optional | Labels are free-form key value pairs supplied by the framework scheduler (e.g., to describe additional functionality offered by the framework). These labels are not interpreted by Mesos itself. Labels should not contain duplicate key-value pairs. |






<a name="mesos.v1.FrameworkInfo.Capability"/>

### FrameworkInfo.Capability



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [FrameworkInfo.Capability.Type](#mesos.v1.FrameworkInfo.Capability.Type) | optional | Enum fields should be optional, see: MESOS-4997. |






<a name="mesos.v1.HealthCheck"/>

### HealthCheck
Describes a health check for a task or executor (or any arbitrary
process/command). A type is picked by specifying one of the
optional fields. Specifying more than one type is an error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| delay_seconds | [double](#double) | optional | Amount of time to wait to start health checking the task after it transitions to `TASK_RUNNING` or `TASK_STATING` if the latter is used by the executor. |
| interval_seconds | [double](#double) | optional | Interval between health checks, i.e., amount of time to wait after the previous health check finished or timed out to start the next health check. |
| timeout_seconds | [double](#double) | optional | Amount of time to wait for the health check to complete. After this timeout, the health check is aborted and treated as a failure. Zero means infinite timeout. |
| consecutive_failures | [uint32](#uint32) | optional | Number of consecutive failures until the task is killed by the executor. |
| grace_period_seconds | [double](#double) | optional | Amount of time after the task is launched during which health check failures are ignored. Once the a check succeeds for the first time, the grace period does not apply anymore. Note that it includes `delay_seconds`, i.e., setting `grace_period_seconds` &lt; `delay_seconds` has no effect. |
| type | [HealthCheck.Type](#mesos.v1.HealthCheck.Type) | optional | The type of health check. |
| command | [CommandInfo](#mesos.v1.CommandInfo) | optional | Command health check. |
| http | [HealthCheck.HTTPCheckInfo](#mesos.v1.HealthCheck.HTTPCheckInfo) | optional | HTTP health check. |
| tcp | [HealthCheck.TCPCheckInfo](#mesos.v1.HealthCheck.TCPCheckInfo) | optional | TCP health check. |






<a name="mesos.v1.HealthCheck.HTTPCheckInfo"/>

### HealthCheck.HTTPCheckInfo
Describes an HTTP health check. Sends a GET request to
scheme://&lt;host&gt;:port/path. Note that &lt;host&gt; is not configurable and is
resolved automatically, in most cases to 127.0.0.1. Default executors
treat return codes between 200 and 399 as success; custom executors
may employ a different strategy, e.g. leveraging the `statuses` field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| scheme | [string](#string) | optional | Currently &#34;http&#34; and &#34;https&#34; are supported. |
| port | [uint32](#uint32) | required | Port to send the HTTP request. |
| path | [string](#string) | optional | HTTP request path. |
| statuses | [uint32](#uint32) | repeated | NOTE: It is up to the custom executor to interpret and act on this field. Setting this field has no effect on the default executors.

TODO(haosdent): Deprecate this field when we add better support for success and possibly failure statuses, e.g. ranges of success and failure statuses. |






<a name="mesos.v1.HealthCheck.TCPCheckInfo"/>

### HealthCheck.TCPCheckInfo
Describes a TCP health check, i.e. based on establishing
a TCP connection to the specified port.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| port | [uint32](#uint32) | required | Port expected to be open. |






<a name="mesos.v1.IcmpStatistics"/>

### IcmpStatistics



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| InMsgs | [int64](#int64) | optional |  |
| InErrors | [int64](#int64) | optional |  |
| InCsumErrors | [int64](#int64) | optional |  |
| InDestUnreachs | [int64](#int64) | optional |  |
| InTimeExcds | [int64](#int64) | optional |  |
| InParmProbs | [int64](#int64) | optional |  |
| InSrcQuenchs | [int64](#int64) | optional |  |
| InRedirects | [int64](#int64) | optional |  |
| InEchos | [int64](#int64) | optional |  |
| InEchoReps | [int64](#int64) | optional |  |
| InTimestamps | [int64](#int64) | optional |  |
| InTimestampReps | [int64](#int64) | optional |  |
| InAddrMasks | [int64](#int64) | optional |  |
| InAddrMaskReps | [int64](#int64) | optional |  |
| OutMsgs | [int64](#int64) | optional |  |
| OutErrors | [int64](#int64) | optional |  |
| OutDestUnreachs | [int64](#int64) | optional |  |
| OutTimeExcds | [int64](#int64) | optional |  |
| OutParmProbs | [int64](#int64) | optional |  |
| OutSrcQuenchs | [int64](#int64) | optional |  |
| OutRedirects | [int64](#int64) | optional |  |
| OutEchos | [int64](#int64) | optional |  |
| OutEchoReps | [int64](#int64) | optional |  |
| OutTimestamps | [int64](#int64) | optional |  |
| OutTimestampReps | [int64](#int64) | optional |  |
| OutAddrMasks | [int64](#int64) | optional |  |
| OutAddrMaskReps | [int64](#int64) | optional |  |






<a name="mesos.v1.Image"/>

### Image
Describe an image used by tasks or executors. Note that it&#39;s only
for tasks or executors launched by MesosContainerizer currently.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Image.Type](#mesos.v1.Image.Type) | required |  |
| appc | [Image.Appc](#mesos.v1.Image.Appc) | optional | Only one of the following image messages should be set to match the type. |
| docker | [Image.Docker](#mesos.v1.Image.Docker) | optional |  |
| cached | [bool](#bool) | optional | With this flag set to false, the mesos containerizer will pull the docker/appc image from the registry even if the image is already downloaded on the agent. |






<a name="mesos.v1.Image.Appc"/>

### Image.Appc
Protobuf for specifying an Appc container image. See:
https://github.com/appc/spec/blob/master/spec/aci.md


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required | The name of the image. |
| id | [string](#string) | optional | An image ID is a string of the format &#34;hash-value&#34;, where &#34;hash&#34; is the hash algorithm used and &#34;value&#34; is the hex encoded string of the digest. Currently the only permitted hash algorithm is sha512. |
| labels | [Labels](#mesos.v1.Labels) | optional | Optional labels. Suggested labels: &#34;version&#34;, &#34;os&#34;, and &#34;arch&#34;. |






<a name="mesos.v1.Image.Docker"/>

### Image.Docker



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required | The name of the image. Expected format: [REGISTRY_HOST[:REGISTRY_PORT]/]REPOSITORY[:TAG|@TYPE:DIGEST]

See: https://docs.docker.com/reference/commandline/pull |
| credential | [Credential](#mesos.v1.Credential) | optional | Credential to authenticate with docker registry. NOTE: This is not encrypted, therefore framework and operators should enable SSL when passing this information. |






<a name="mesos.v1.InverseOffer"/>

### InverseOffer
A request to return some resources occupied by a framework.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [OfferID](#mesos.v1.OfferID) | required | This is the same OfferID as found in normal offers, which allows re-use of some of the OfferID-only messages. |
| url | [URL](#mesos.v1.URL) | optional | URL for reaching the agent running on the host. This enables some optimizations as described in MESOS-3012, such as allowing the scheduler driver to bypass the master and talk directly with an agent. |
| framework_id | [FrameworkID](#mesos.v1.FrameworkID) | required | The framework that should release its resources. If no specifics are provided (i.e. which agent), all the framework&#39;s resources are requested back. |
| agent_id | [AgentID](#mesos.v1.AgentID) | optional | Specified if the resources need to be released from a particular agent. All the framework&#39;s resources on this agent are requested back, unless further qualified by the `resources` field. |
| unavailability | [Unavailability](#mesos.v1.Unavailability) | required | This InverseOffer represents a planned unavailability event in the specified interval. Any tasks running on the given framework or agent may be killed when the interval arrives. Therefore, frameworks should aim to gracefully terminate tasks prior to the arrival of the interval.

For reserved resources, the resources are expected to be returned to the framework after the unavailability interval. This is an expectation, not a guarantee. For example, if the unavailability duration is not set, the resources may be removed permanently.

For other resources, there is no guarantee that requested resources will be returned after the unavailability interval. The allocator has no obligation to re-offer these resources to the prior framework after the unavailability. |
| resources | [Resource](#mesos.v1.Resource) | repeated | A list of resources being requested back from the framework, on the agent identified by `agent_id`. If no resources are specified then all resources are being requested back. For the purpose of maintenance, this field is always empty (maintenance always requests all resources back). |






<a name="mesos.v1.IpStatistics"/>

### IpStatistics



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| Forwarding | [int64](#int64) | optional |  |
| DefaultTTL | [int64](#int64) | optional |  |
| InReceives | [int64](#int64) | optional |  |
| InHdrErrors | [int64](#int64) | optional |  |
| InAddrErrors | [int64](#int64) | optional |  |
| ForwDatagrams | [int64](#int64) | optional |  |
| InUnknownProtos | [int64](#int64) | optional |  |
| InDiscards | [int64](#int64) | optional |  |
| InDelivers | [int64](#int64) | optional |  |
| OutRequests | [int64](#int64) | optional |  |
| OutDiscards | [int64](#int64) | optional |  |
| OutNoRoutes | [int64](#int64) | optional |  |
| ReasmTimeout | [int64](#int64) | optional |  |
| ReasmReqds | [int64](#int64) | optional |  |
| ReasmOKs | [int64](#int64) | optional |  |
| ReasmFails | [int64](#int64) | optional |  |
| FragOKs | [int64](#int64) | optional |  |
| FragFails | [int64](#int64) | optional |  |
| FragCreates | [int64](#int64) | optional |  |






<a name="mesos.v1.KillPolicy"/>

### KillPolicy
Describes a kill policy for a task. Currently does not express
different policies (e.g. hitting HTTP endpoints), only controls
how long to wait between graceful and forcible task kill:

graceful kill --------------&gt; forcible kill
grace_period

Kill policies are best-effort, because machine failures / forcible
terminations may occur.

NOTE: For executor-less command-based tasks, the kill is performed
via sending a signal to the task process: SIGTERM for the graceful
kill and SIGKILL for the forcible kill. For the docker executor-less
tasks the grace period is passed to &#39;docker stop --time&#39;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| grace_period | [DurationInfo](#mesos.v1.DurationInfo) | optional | The grace period specifies how long to wait before forcibly killing the task. It is recommended to attempt to gracefully kill the task (and send TASK_KILLING) to indicate that the graceful kill is in progress. Once the grace period elapses, if the task has not terminated, a forcible kill should occur. The task should not assume that it will always be allotted the full grace period. For example, the executor may be shutdown more quickly by the agent, or failures / forcible terminations may occur. |






<a name="mesos.v1.Label"/>

### Label
Key, value pair used to store free form user-data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) | required |  |
| value | [string](#string) | optional |  |






<a name="mesos.v1.Labels"/>

### Labels
Collection of labels. Labels should not contain duplicate key-value
pairs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| labels | [Label](#mesos.v1.Label) | repeated |  |






<a name="mesos.v1.LinuxInfo"/>

### LinuxInfo
Encapsulation for Linux specific configuration.
E.g, capabilities, limits etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| capability_info | [CapabilityInfo](#mesos.v1.CapabilityInfo) | optional | Represents the capability whitelist. |






<a name="mesos.v1.MachineID"/>

### MachineID
Represents a single machine, which may hold one or more agents.

NOTE: In order to match an agent to a machine, both the `hostname` and
`ip` must match the values advertised by the agent to the master.
Hostname is not case-sensitive.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) | optional |  |
| ip | [string](#string) | optional |  |






<a name="mesos.v1.MachineInfo"/>

### MachineInfo
Holds information about a single machine, its `mode`, and any other
relevant information which may affect the behavior of the machine.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [MachineID](#mesos.v1.MachineID) | required |  |
| mode | [MachineInfo.Mode](#mesos.v1.MachineInfo.Mode) | optional |  |
| unavailability | [Unavailability](#mesos.v1.Unavailability) | optional | Signifies that the machine may be unavailable during the given interval. See comments in `Unavailability` and for the `unavailability` fields in `Offer` and `InverseOffer` for more information. |






<a name="mesos.v1.MasterInfo"/>

### MasterInfo
Describes a master. This will probably have more fields in the
future which might be used, for example, to link a framework webui
to a master webui.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) | required |  |
| ip | [uint32](#uint32) | required | The IP address (only IPv4) as a packed 4-bytes integer, stored in network order. Deprecated, use `address.ip` instead. |
| port | [uint32](#uint32) | required | The TCP port the Master is listening on for incoming HTTP requests; deprecated, use `address.port` instead. |
| pid | [string](#string) | optional | In the default implementation, this will contain information about both the IP address, port and Master name; it should really not be relied upon by external tooling/frameworks and be considered an &#34;internal&#34; implementation field. |
| hostname | [string](#string) | optional | The server&#39;s hostname, if available; it may be unreliable in environments where the DNS configuration does not resolve internal hostnames (eg, some public cloud providers). Deprecated, use `address.hostname` instead. |
| version | [string](#string) | optional | The running Master version, as a string; taken from the generated &#34;master/version.hpp&#34;. |
| address | [Address](#mesos.v1.Address) | optional | The full IP address (supports both IPv4 and IPv6 formats) and supersedes the use of `ip`, `port` and `hostname`. Since Mesos 0.24. |






<a name="mesos.v1.Metric"/>

### Metric
Metric consists of a name and optionally its value.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| value | [double](#double) | optional |  |






<a name="mesos.v1.NetworkInfo"/>

### NetworkInfo
Describes a network request from a framework as well as network resolution
provided by Mesos.

A framework may request the network isolator on the Agent to isolate the
container in a network namespace and create a virtual network interface.
The `NetworkInfo` message describes the properties of that virtual
interface, including the IP addresses and network isolation policy
(network group membership).

The NetworkInfo message is not interpreted by the Master or Agent and is
intended to be used by Agent and Master modules implementing network
isolation. If the modules are missing, the message is simply ignored. In
future, the task launch will fail if there is no module providing the
network isolation capabilities (MESOS-3390).

An executor, Agent, or an Agent module may append NetworkInfos inside
TaskStatus::container_status to provide information such as the container IP
address and isolation groups.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ip_addresses | [NetworkInfo.IPAddress](#mesos.v1.NetworkInfo.IPAddress) | repeated | When included in a ContainerInfo, each of these represent a request for an IP address. Each request can specify an explicit address or the IP protocol to use.

When included in a TaskStatus message, these inform the framework scheduler about the IP addresses that are bound to the container interface. When there are no custom network isolator modules installed, this field is filled in automatically with the Agent IP address. |
| name | [string](#string) | optional | Name of the network which will be used by network isolator to determine the network that the container joins. It&#39;s up to the network isolator to decide how to interpret this field. |
| groups | [string](#string) | repeated | A group is the name given to a set of logically-related interfaces that are allowed to communicate among themselves. Network traffic is allowed between two container interfaces that share at least one network group. For example, one might want to create separate groups for isolating dev, testing, qa and prod deployment environments. |
| labels | [Labels](#mesos.v1.Labels) | optional | To tag certain metadata to be used by Isolator/IPAM, e.g., rack, etc. |
| port_mappings | [NetworkInfo.PortMapping](#mesos.v1.NetworkInfo.PortMapping) | repeated |  |






<a name="mesos.v1.NetworkInfo.IPAddress"/>

### NetworkInfo.IPAddress
Specifies a request for an IP address, or reports the assigned container
IP address.

Users can request an automatically assigned IP (for example, via an
IPAM service) or a specific IP by adding a NetworkInfo to the
ContainerInfo for a task.  On a request, specifying neither `protocol`
nor `ip_address` means that any available address may be assigned.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| protocol | [NetworkInfo.Protocol](#mesos.v1.NetworkInfo.Protocol) | optional | Specify IP address requirement. Set protocol to the desired value to request the network isolator on the Agent to assign an IP address to the container being launched. If a specific IP address is specified in ip_address, this field should not be set. |
| ip_address | [string](#string) | optional | Statically assigned IP provided by the Framework. This IP will be assigned to the container by the network isolator module on the Agent. This field should not be used with the protocol field above.

If an explicit address is requested but is unavailable, the network isolator should fail the task. |






<a name="mesos.v1.NetworkInfo.PortMapping"/>

### NetworkInfo.PortMapping
Specifies a port mapping request for the task on this network.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host_port | [uint32](#uint32) | required |  |
| container_port | [uint32](#uint32) | required |  |
| protocol | [string](#string) | optional | Protocol to expose as (ie: tcp, udp). |






<a name="mesos.v1.Offer"/>

### Offer
Describes some resources available on an agent. An offer only
contains resources from a single agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [OfferID](#mesos.v1.OfferID) | required |  |
| framework_id | [FrameworkID](#mesos.v1.FrameworkID) | required |  |
| agent_id | [AgentID](#mesos.v1.AgentID) | required |  |
| hostname | [string](#string) | required |  |
| url | [URL](#mesos.v1.URL) | optional | URL for reaching the agent running on the host. |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| attributes | [Attribute](#mesos.v1.Attribute) | repeated |  |
| executor_ids | [ExecutorID](#mesos.v1.ExecutorID) | repeated |  |
| unavailability | [Unavailability](#mesos.v1.Unavailability) | optional | Signifies that the resources in this Offer may be unavailable during the given interval. Any tasks launched using these resources may be killed when the interval arrives. For example, these resources may be part of a planned maintenance schedule.

This field only provides information about a planned unavailability. The unavailability interval may not necessarily start at exactly this interval, nor last for exactly the duration of this interval. The unavailability may also be forever! See comments in `Unavailability` for more details. |
| allocation_info | [Resource.AllocationInfo](#mesos.v1.Resource.AllocationInfo) | optional | An offer represents resources allocated to *one* of the roles managed by the scheduler. (Therefore, each `Offer.resources[i].allocation_info` will match the top level `Offer.allocation_info`).

NOTE: Implementation of this is in-progress, DO NOT USE! |






<a name="mesos.v1.Offer.Operation"/>

### Offer.Operation
Defines an operation that can be performed against offers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Offer.Operation.Type](#mesos.v1.Offer.Operation.Type) | optional |  |
| launch | [Offer.Operation.Launch](#mesos.v1.Offer.Operation.Launch) | optional |  |
| launch_group | [Offer.Operation.LaunchGroup](#mesos.v1.Offer.Operation.LaunchGroup) | optional |  |
| reserve | [Offer.Operation.Reserve](#mesos.v1.Offer.Operation.Reserve) | optional |  |
| unreserve | [Offer.Operation.Unreserve](#mesos.v1.Offer.Operation.Unreserve) | optional |  |
| create | [Offer.Operation.Create](#mesos.v1.Offer.Operation.Create) | optional |  |
| destroy | [Offer.Operation.Destroy](#mesos.v1.Offer.Operation.Destroy) | optional |  |






<a name="mesos.v1.Offer.Operation.Create"/>

### Offer.Operation.Create



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.Offer.Operation.Destroy"/>

### Offer.Operation.Destroy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.Offer.Operation.Launch"/>

### Offer.Operation.Launch
TODO(vinod): Deprecate this in favor of `LaunchGroup` below.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_infos | [TaskInfo](#mesos.v1.TaskInfo) | repeated |  |






<a name="mesos.v1.Offer.Operation.LaunchGroup"/>

### Offer.Operation.LaunchGroup
Unlike `Launch` above, all the tasks in a `task_group` are
atomically delivered to an executor.

`NetworkInfo` set on executor will be shared by all tasks in
the task group.

TODO(vinod): Any volumes set on executor could be used by a
task by explicitly setting `Volume.source` in its resources.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor | [ExecutorInfo](#mesos.v1.ExecutorInfo) | required |  |
| task_group | [TaskGroupInfo](#mesos.v1.TaskGroupInfo) | required |  |






<a name="mesos.v1.Offer.Operation.Reserve"/>

### Offer.Operation.Reserve



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.Offer.Operation.Unreserve"/>

### Offer.Operation.Unreserve



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.OfferID"/>

### OfferID
A unique ID assigned to an offer.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.Parameter"/>

### Parameter
A generic (key, value) pair used in various places for parameters.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) | required |  |
| value | [string](#string) | required |  |






<a name="mesos.v1.Parameters"/>

### Parameters
Collection of Parameter.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| parameter | [Parameter](#mesos.v1.Parameter) | repeated |  |






<a name="mesos.v1.PerfStatistics"/>

### PerfStatistics
Describes a sample of events from &#34;perf stat&#34;. Only available on
Linux.

NOTE: Each optional field matches the name of a perf event (see
&#34;perf list&#34;) with the following changes:
1. Names are downcased.
2. Hyphens (&#39;-&#39;) are replaced with underscores (&#39;_&#39;).
3. Events with alternate names use the name &#34;perf stat&#34; returns,
e.g., for the event &#34;cycles OR cpu-cycles&#34; perf always returns
cycles.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [double](#double) | required | Start of sample interval, in seconds since the Epoch. |
| duration | [double](#double) | required | Duration of sample interval, in seconds. |
| cycles | [uint64](#uint64) | optional | Hardware event. |
| stalled_cycles_frontend | [uint64](#uint64) | optional |  |
| stalled_cycles_backend | [uint64](#uint64) | optional |  |
| instructions | [uint64](#uint64) | optional |  |
| cache_references | [uint64](#uint64) | optional |  |
| cache_misses | [uint64](#uint64) | optional |  |
| branches | [uint64](#uint64) | optional |  |
| branch_misses | [uint64](#uint64) | optional |  |
| bus_cycles | [uint64](#uint64) | optional |  |
| ref_cycles | [uint64](#uint64) | optional |  |
| cpu_clock | [double](#double) | optional | Software event. |
| task_clock | [double](#double) | optional |  |
| page_faults | [uint64](#uint64) | optional |  |
| minor_faults | [uint64](#uint64) | optional |  |
| major_faults | [uint64](#uint64) | optional |  |
| context_switches | [uint64](#uint64) | optional |  |
| cpu_migrations | [uint64](#uint64) | optional |  |
| alignment_faults | [uint64](#uint64) | optional |  |
| emulation_faults | [uint64](#uint64) | optional |  |
| l1_dcache_loads | [uint64](#uint64) | optional | Hardware cache event. |
| l1_dcache_load_misses | [uint64](#uint64) | optional |  |
| l1_dcache_stores | [uint64](#uint64) | optional |  |
| l1_dcache_store_misses | [uint64](#uint64) | optional |  |
| l1_dcache_prefetches | [uint64](#uint64) | optional |  |
| l1_dcache_prefetch_misses | [uint64](#uint64) | optional |  |
| l1_icache_loads | [uint64](#uint64) | optional |  |
| l1_icache_load_misses | [uint64](#uint64) | optional |  |
| l1_icache_prefetches | [uint64](#uint64) | optional |  |
| l1_icache_prefetch_misses | [uint64](#uint64) | optional |  |
| llc_loads | [uint64](#uint64) | optional |  |
| llc_load_misses | [uint64](#uint64) | optional |  |
| llc_stores | [uint64](#uint64) | optional |  |
| llc_store_misses | [uint64](#uint64) | optional |  |
| llc_prefetches | [uint64](#uint64) | optional |  |
| llc_prefetch_misses | [uint64](#uint64) | optional |  |
| dtlb_loads | [uint64](#uint64) | optional |  |
| dtlb_load_misses | [uint64](#uint64) | optional |  |
| dtlb_stores | [uint64](#uint64) | optional |  |
| dtlb_store_misses | [uint64](#uint64) | optional |  |
| dtlb_prefetches | [uint64](#uint64) | optional |  |
| dtlb_prefetch_misses | [uint64](#uint64) | optional |  |
| itlb_loads | [uint64](#uint64) | optional |  |
| itlb_load_misses | [uint64](#uint64) | optional |  |
| branch_loads | [uint64](#uint64) | optional |  |
| branch_load_misses | [uint64](#uint64) | optional |  |
| node_loads | [uint64](#uint64) | optional |  |
| node_load_misses | [uint64](#uint64) | optional |  |
| node_stores | [uint64](#uint64) | optional |  |
| node_store_misses | [uint64](#uint64) | optional |  |
| node_prefetches | [uint64](#uint64) | optional |  |
| node_prefetch_misses | [uint64](#uint64) | optional |  |






<a name="mesos.v1.Port"/>

### Port
Named port used for service discovery.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| number | [uint32](#uint32) | required | Port number on which the framework exposes a service. |
| name | [string](#string) | optional | Name of the service hosted on this port. |
| protocol | [string](#string) | optional | Layer 4-7 protocol on which the framework exposes its services. |
| visibility | [DiscoveryInfo.Visibility](#mesos.v1.DiscoveryInfo.Visibility) | optional | This field restricts discovery within a framework (FRAMEWORK), within a Mesos cluster (CLUSTER), or places no restrictions (EXTERNAL). The visibility setting for a Port overrides the general visibility setting in the DiscoveryInfo. |
| labels | [Labels](#mesos.v1.Labels) | optional | This can be used to decorate the message with metadata to be interpreted by external applications such as firewalls. |






<a name="mesos.v1.Ports"/>

### Ports
Collection of ports.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ports | [Port](#mesos.v1.Port) | repeated |  |






<a name="mesos.v1.RLimitInfo"/>

### RLimitInfo
Encapsulation for POSIX rlimits, see
http://pubs.opengroup.org/onlinepubs/009695399/functions/getrlimit.html.
Note that some types might only be defined for Linux.
We use a custom prefix to avoid conflict with existing system macros
(e.g., `RLIMIT_CPU` or `NOFILE`).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rlimits | [RLimitInfo.RLimit](#mesos.v1.RLimitInfo.RLimit) | repeated |  |






<a name="mesos.v1.RLimitInfo.RLimit"/>

### RLimitInfo.RLimit



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [RLimitInfo.RLimit.Type](#mesos.v1.RLimitInfo.RLimit.Type) | optional |  |
| hard | [uint64](#uint64) | optional | Either both are set or both are not set. If both are not set, it represents unlimited. If both are set, we require `soft` &lt;= `hard`. |
| soft | [uint64](#uint64) | optional |  |






<a name="mesos.v1.RateLimit"/>

### RateLimit
Rate (queries per second, QPS) limit for messages from a framework to master.
Strictly speaking they are the combined rate from all frameworks of the same
principal.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| qps | [double](#double) | optional | Leaving QPS unset gives it unlimited rate (i.e., not throttled), which also implies unlimited capacity. |
| principal | [string](#string) | required | Principal of framework(s) to be throttled. Should match FrameworkInfo.principal and Credential.principal (if using authentication). |
| capacity | [uint64](#uint64) | optional | Max number of outstanding messages from frameworks of this principal allowed by master before the next message is dropped and an error is sent back to the sender. Messages received before the capacity is reached are still going to be processed after the error is sent. If unspecified, this principal is assigned unlimited capacity. NOTE: This value is ignored if &#39;qps&#39; is not set. |






<a name="mesos.v1.RateLimits"/>

### RateLimits
Collection of RateLimit.
Frameworks without rate limits defined here are not throttled unless
&#39;aggregate_default_qps&#39; is specified.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limits | [RateLimit](#mesos.v1.RateLimit) | repeated | Items should have unique principals. |
| aggregate_default_qps | [double](#double) | optional | All the frameworks not specified in &#39;limits&#39; get this default rate. This rate is an aggregate rate for all of them, i.e., their combined traffic is throttled together at this rate. |
| aggregate_default_capacity | [uint64](#uint64) | optional | All the frameworks not specified in &#39;limits&#39; get this default capacity. This is an aggregate value similar to &#39;aggregate_default_qps&#39;. |






<a name="mesos.v1.Request"/>

### Request
Describes a request for resources that can be used by a framework
to proactively influence the allocator.  If &#39;agent_id&#39; is provided
then this request is assumed to only apply to resources on that
agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [AgentID](#mesos.v1.AgentID) | optional |  |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.Resource"/>

### Resource
Describes a resource on a machine. The `name` field is a string
like &#34;cpus&#34; or &#34;mem&#34; that indicates which kind of resource this is;
the rest of the fields describe the properties of the resource. A
resource can take on one of three types: scalar (double), a list of
finite and discrete ranges (e.g., [1-10, 20-30]), or a set of
items. A resource is described using the standard protocol buffer
&#34;union&#34; trick.

Note that &#34;disk&#34; and &#34;mem&#34; resources are scalar values expressed in
megabytes. Fractional &#34;cpus&#34; values are allowed (e.g., &#34;0.5&#34;),
which correspond to partial shares of a CPU.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| type | [Value.Type](#mesos.v1.Value.Type) | required |  |
| scalar | [Value.Scalar](#mesos.v1.Value.Scalar) | optional |  |
| ranges | [Value.Ranges](#mesos.v1.Value.Ranges) | optional |  |
| set | [Value.Set](#mesos.v1.Value.Set) | optional |  |
| role | [string](#string) | optional | The role that this resource is reserved for. If &#34;*&#34;, this indicates that the resource is unreserved. Otherwise, the resource will only be offered to frameworks that belong to this role. |
| allocation_info | [Resource.AllocationInfo](#mesos.v1.Resource.AllocationInfo) | optional |  |
| reservation | [Resource.ReservationInfo](#mesos.v1.Resource.ReservationInfo) | optional | If this is set, this resource was dynamically reserved by an operator or a framework. Otherwise, this resource is either unreserved or statically reserved by an operator via the --resources flag. |
| disk | [Resource.DiskInfo](#mesos.v1.Resource.DiskInfo) | optional |  |
| revocable | [Resource.RevocableInfo](#mesos.v1.Resource.RevocableInfo) | optional | If this is set, the resources are revocable, i.e., any tasks or executors launched using these resources could get preempted or throttled at any time. This could be used by frameworks to run best effort tasks that do not need strict uptime or performance guarantees. Note that if this is set, &#39;disk&#39; or &#39;reservation&#39; cannot be set. |
| shared | [Resource.SharedInfo](#mesos.v1.Resource.SharedInfo) | optional | If this is set, the resources are shared, i.e. multiple tasks can be launched using this resource and all of them shall refer to the same physical resource on the cluster. Note that only persistent volumes can be shared currently. |






<a name="mesos.v1.Resource.AllocationInfo"/>

### Resource.AllocationInfo
This was initially introduced to support MULTI_ROLE capable
frameworks. Frameworks that are not MULTI_ROLE capable can
continue to assume that the offered resources are allocated
to their role.

NOTE: Implementation of this is in-progress, DO NOT USE!


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) | optional | If set, this resource is allocated to a role. Note that in the future, this may be unset and the scheduler may be responsible for allocating to one of its roles. |






<a name="mesos.v1.Resource.DiskInfo"/>

### Resource.DiskInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| persistence | [Resource.DiskInfo.Persistence](#mesos.v1.Resource.DiskInfo.Persistence) | optional |  |
| volume | [Volume](#mesos.v1.Volume) | optional | Describes how this disk resource will be mounted in the container. If not set, the disk resource will be used as the sandbox. Otherwise, it will be mounted according to the &#39;container_path&#39; inside &#39;volume&#39;. The &#39;host_path&#39; inside &#39;volume&#39; is ignored. NOTE: If &#39;volume&#39; is set but &#39;persistence&#39; is not set, the volume will be automatically garbage collected after task/executor terminates. Currently, if &#39;persistence&#39; is set, &#39;volume&#39; must be set. |
| source | [Resource.DiskInfo.Source](#mesos.v1.Resource.DiskInfo.Source) | optional |  |






<a name="mesos.v1.Resource.DiskInfo.Persistence"/>

### Resource.DiskInfo.Persistence
Describes a persistent disk volume.

A persistent disk volume will not be automatically garbage
collected if the task/executor/agent terminates, but will be
re-offered to the framework(s) belonging to the &#39;role&#39;.

NOTE: Currently, we do not allow persistent disk volumes
without a reservation (i.e., &#39;role&#39; cannot be &#39;*&#39;).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) | required | A unique ID for the persistent disk volume. This ID must be unique per role on each agent. Although it is possible to use the same ID on different agents in the cluster and to reuse IDs after a volume with that ID has been destroyed, both practices are discouraged. |
| principal | [string](#string) | optional | This field indicates the principal of the operator or framework that created this volume. It is used in conjunction with the &#34;destroy&#34; ACL to determine whether an entity attempting to destroy the volume is permitted to do so.

NOTE: This field is optional, while the `principal` found in `ReservationInfo` is required. This field is optional to allow for the possibility of volume creation without a principal, though currently it must be provided.

NOTE: This field should match the FrameworkInfo.principal of the framework that created the volume. |






<a name="mesos.v1.Resource.DiskInfo.Source"/>

### Resource.DiskInfo.Source
Describes where a disk originates from.
TODO(jmlvanre): Add support for BLOCK devices.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Resource.DiskInfo.Source.Type](#mesos.v1.Resource.DiskInfo.Source.Type) | required |  |
| path | [Resource.DiskInfo.Source.Path](#mesos.v1.Resource.DiskInfo.Source.Path) | optional |  |
| mount | [Resource.DiskInfo.Source.Mount](#mesos.v1.Resource.DiskInfo.Source.Mount) | optional |  |






<a name="mesos.v1.Resource.DiskInfo.Source.Mount"/>

### Resource.DiskInfo.Source.Mount
A mounted file-system set up by the Agent administrator. This
can only be used exclusively: a framework cannot accept a
partial amount of this disk.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| root | [string](#string) | required | Path to mount point (e.g., /mnt/raid/disk0). |






<a name="mesos.v1.Resource.DiskInfo.Source.Path"/>

### Resource.DiskInfo.Source.Path
A folder that can be located on a separate disk device. This
can be shared and carved up as necessary between frameworks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| root | [string](#string) | required | Path to the folder (e.g., /mnt/raid/disk0). |






<a name="mesos.v1.Resource.ReservationInfo"/>

### Resource.ReservationInfo
Describes a dynamic reservation. A dynamic reservation is
acquired by an operator via the &#39;/reserve&#39; HTTP endpoint or by
a framework via the offer cycle by sending back an
&#39;Offer::Operation::Reserve&#39; message.
NOTE: We currently do not allow frameworks with role &#34;*&#34; to
make dynamic reservations.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| principal | [string](#string) | optional | Indicates the principal, if any, of the framework or operator that reserved this resource. If reserved by a framework, the field should match the `FrameworkInfo.principal`. It is used in conjunction with the `UnreserveResources` ACL to determine whether the entity attempting to unreserve this resource is permitted to do so. |
| labels | [Labels](#mesos.v1.Labels) | optional | Labels are free-form key value pairs that can be used to associate arbitrary metadata with a reserved resource. For example, frameworks can use labels to identify the intended purpose for a portion of the resources the framework has reserved at a given agent. Labels should not contain duplicate key-value pairs. |






<a name="mesos.v1.Resource.RevocableInfo"/>

### Resource.RevocableInfo







<a name="mesos.v1.Resource.SharedInfo"/>

### Resource.SharedInfo
Allow the resource to be shared across tasks.






<a name="mesos.v1.ResourceStatistics"/>

### ResourceStatistics
A snapshot of resource usage statistics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timestamp | [double](#double) | required | Snapshot time, in seconds since the Epoch. |
| processes | [uint32](#uint32) | optional |  |
| threads | [uint32](#uint32) | optional |  |
| cpus_user_time_secs | [double](#double) | optional | CPU Usage Information: Total CPU time spent in user mode, and kernel mode. |
| cpus_system_time_secs | [double](#double) | optional |  |
| cpus_limit | [double](#double) | optional | Number of CPUs allocated. |
| cpus_nr_periods | [uint32](#uint32) | optional | cpu.stat on process throttling (for contention issues). |
| cpus_nr_throttled | [uint32](#uint32) | optional |  |
| cpus_throttled_time_secs | [double](#double) | optional |  |
| mem_total_bytes | [uint64](#uint64) | optional | mem_total_bytes was added in 0.23.0 to represent the total memory of a process in RAM (as opposed to in Swap). This was previously reported as mem_rss_bytes, which was also changed in 0.23.0 to represent only the anonymous memory usage, to keep in sync with Linux kernel&#39;s (arguably erroneous) use of terminology. |
| mem_total_memsw_bytes | [uint64](#uint64) | optional | Total memory &#43; swap usage. This is set if swap is enabled. |
| mem_limit_bytes | [uint64](#uint64) | optional | Hard memory limit for a container. |
| mem_soft_limit_bytes | [uint64](#uint64) | optional | Soft memory limit for a container. |
| mem_file_bytes | [uint64](#uint64) | optional | TODO(chzhcn) mem_file_bytes and mem_anon_bytes are deprecated in 0.23.0 and will be removed in 0.24.0. |
| mem_anon_bytes | [uint64](#uint64) | optional |  |
| mem_cache_bytes | [uint64](#uint64) | optional | mem_cache_bytes is added in 0.23.0 to represent page cache usage. |
| mem_rss_bytes | [uint64](#uint64) | optional | Since 0.23.0, mem_rss_bytes is changed to represent only anonymous memory usage. Note that neither its requiredness, type, name nor numeric tag has been changed. |
| mem_mapped_file_bytes | [uint64](#uint64) | optional |  |
| mem_swap_bytes | [uint64](#uint64) | optional | This is only set if swap is enabled. |
| mem_unevictable_bytes | [uint64](#uint64) | optional |  |
| mem_low_pressure_counter | [uint64](#uint64) | optional | Number of occurrences of different levels of memory pressure events reported by memory cgroup. Pressure listening (re)starts with these values set to 0 when agent (re)starts. See https://www.kernel.org/doc/Documentation/cgroups/memory.txt for more details. |
| mem_medium_pressure_counter | [uint64](#uint64) | optional |  |
| mem_critical_pressure_counter | [uint64](#uint64) | optional |  |
| disk_limit_bytes | [uint64](#uint64) | optional | Disk Usage Information for executor working directory. |
| disk_used_bytes | [uint64](#uint64) | optional |  |
| perf | [PerfStatistics](#mesos.v1.PerfStatistics) | optional | Perf statistics. |
| net_rx_packets | [uint64](#uint64) | optional | Network Usage Information: |
| net_rx_bytes | [uint64](#uint64) | optional |  |
| net_rx_errors | [uint64](#uint64) | optional |  |
| net_rx_dropped | [uint64](#uint64) | optional |  |
| net_tx_packets | [uint64](#uint64) | optional |  |
| net_tx_bytes | [uint64](#uint64) | optional |  |
| net_tx_errors | [uint64](#uint64) | optional |  |
| net_tx_dropped | [uint64](#uint64) | optional |  |
| net_tcp_rtt_microsecs_p50 | [double](#double) | optional | The kernel keeps track of RTT (round-trip time) for its TCP sockets. RTT is a way to tell the latency of a container. |
| net_tcp_rtt_microsecs_p90 | [double](#double) | optional |  |
| net_tcp_rtt_microsecs_p95 | [double](#double) | optional |  |
| net_tcp_rtt_microsecs_p99 | [double](#double) | optional |  |
| net_tcp_active_connections | [double](#double) | optional |  |
| net_tcp_time_wait_connections | [double](#double) | optional |  |
| net_traffic_control_statistics | [TrafficControlStatistics](#mesos.v1.TrafficControlStatistics) | repeated | Network traffic flowing into or out of a container can be delayed or dropped due to congestion or policy inside and outside the container. |
| net_snmp_statistics | [SNMPStatistics](#mesos.v1.SNMPStatistics) | optional | Network SNMP statistics for each container. |






<a name="mesos.v1.ResourceUsage"/>

### ResourceUsage
Describes a snapshot of the resource usage for executors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executors | [ResourceUsage.Executor](#mesos.v1.ResourceUsage.Executor) | repeated |  |
| total | [Resource](#mesos.v1.Resource) | repeated | Agent&#39;s total resources including checkpointed dynamic reservations and persistent volumes. |






<a name="mesos.v1.ResourceUsage.Executor"/>

### ResourceUsage.Executor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_info | [ExecutorInfo](#mesos.v1.ExecutorInfo) | required |  |
| allocated | [Resource](#mesos.v1.Resource) | repeated | This includes resources used by the executor itself as well as its active tasks. |
| statistics | [ResourceStatistics](#mesos.v1.ResourceStatistics) | optional | Current resource usage. If absent, the containerizer cannot provide resource usage. |
| container_id | [ContainerID](#mesos.v1.ContainerID) | required | The container id for the executor specified in the executor_info field. |
| tasks | [ResourceUsage.Executor.Task](#mesos.v1.ResourceUsage.Executor.Task) | repeated | Non-terminal tasks. |






<a name="mesos.v1.ResourceUsage.Executor.Task"/>

### ResourceUsage.Executor.Task



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| id | [TaskID](#mesos.v1.TaskID) | required |  |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| labels | [Labels](#mesos.v1.Labels) | optional |  |






<a name="mesos.v1.Role"/>

### Role
Describes a Role. Roles can be used to specify that certain resources are
reserved for the use of one or more frameworks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| weight | [double](#double) | required |  |
| frameworks | [FrameworkID](#mesos.v1.FrameworkID) | repeated |  |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.SNMPStatistics"/>

### SNMPStatistics



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ip_stats | [IpStatistics](#mesos.v1.IpStatistics) | optional |  |
| icmp_stats | [IcmpStatistics](#mesos.v1.IcmpStatistics) | optional |  |
| tcp_stats | [TcpStatistics](#mesos.v1.TcpStatistics) | optional |  |
| udp_stats | [UdpStatistics](#mesos.v1.UdpStatistics) | optional |  |






<a name="mesos.v1.Secret"/>

### Secret
Secret used to pass privileged information. It is designed to provide
pass-by-value or pass-by-reference semantics, where the REFERENCE type can be
used by custom modules which interact with a secure back-end.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Secret.Type](#mesos.v1.Secret.Type) | optional |  |
| reference | [Secret.Reference](#mesos.v1.Secret.Reference) | optional | Only one of `reference` and `value` must be set. |
| value | [Secret.Value](#mesos.v1.Secret.Value) | optional |  |






<a name="mesos.v1.Secret.Reference"/>

### Secret.Reference
Can be used by modules to refer to a secret stored in a secure back-end.
The `key` field is provided to permit reference to a single value within a
secret containing arbitrary key-value pairs.

For example, given a back-end secret store with a secret named
&#34;my-secret&#34; containing the following key-value pairs:

{
&#34;username&#34;: &#34;my-user&#34;,
&#34;password&#34;: &#34;my-password
}

the username could be referred to in a `Secret` by specifying
&#34;my-secret&#34; for the `name` and &#34;username&#34; for the `key`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| key | [string](#string) | optional |  |






<a name="mesos.v1.Secret.Value"/>

### Secret.Value
Used to pass the value of a secret.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.TTYInfo"/>

### TTYInfo
Describes the information about (pseudo) TTY that can
be attached to a process running in a container.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| window_size | [TTYInfo.WindowSize](#mesos.v1.TTYInfo.WindowSize) | optional |  |






<a name="mesos.v1.TTYInfo.WindowSize"/>

### TTYInfo.WindowSize



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rows | [uint32](#uint32) | required |  |
| columns | [uint32](#uint32) | required |  |






<a name="mesos.v1.Task"/>

### Task
Describes a task, similar to `TaskInfo`.

`Task` is used in some of the Mesos messages found below.
`Task` is used instead of `TaskInfo` if:
1) we need additional IDs, such as a specific
framework, executor, or agent; or
2) we do not need the additional data, such as the command run by the
task or the health checks.  These additional fields may be large and
unnecessary for some Mesos messages.

`Task` is generally constructed from a `TaskInfo`.  See protobuf::createTask.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| task_id | [TaskID](#mesos.v1.TaskID) | required |  |
| framework_id | [FrameworkID](#mesos.v1.FrameworkID) | required |  |
| executor_id | [ExecutorID](#mesos.v1.ExecutorID) | optional |  |
| agent_id | [AgentID](#mesos.v1.AgentID) | required |  |
| state | [TaskState](#mesos.v1.TaskState) | required | Latest state of the task. |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| statuses | [TaskStatus](#mesos.v1.TaskStatus) | repeated |  |
| status_update_state | [TaskState](#mesos.v1.TaskState) | optional | These fields correspond to the state and uuid of the latest status update forwarded to the master. NOTE: Either both the fields must be set or both must be unset. |
| status_update_uuid | [bytes](#bytes) | optional |  |
| labels | [Labels](#mesos.v1.Labels) | optional |  |
| discovery | [DiscoveryInfo](#mesos.v1.DiscoveryInfo) | optional | Service discovery information for the task. It is not interpreted or acted upon by Mesos. It is up to a service discovery system to use this information as needed and to handle tasks without service discovery information. |
| container | [ContainerInfo](#mesos.v1.ContainerInfo) | optional | Container information for the task. |
| user | [string](#string) | optional | Specific user under which task is running. |






<a name="mesos.v1.TaskGroupInfo"/>

### TaskGroupInfo
Describes a group of tasks that belong to an executor. The
executor will receive the task group in a single message to
allow the group to be launched &#34;atomically&#34;.

NOTES:
1) `NetworkInfo` must not be set inside task&#39;s `ContainerInfo`.
2) `TaskInfo.executor` doesn&#39;t need to set. If set, it should match
`LaunchGroup.executor`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [TaskInfo](#mesos.v1.TaskInfo) | repeated |  |






<a name="mesos.v1.TaskID"/>

### TaskID
A framework-generated ID to distinguish a task. The ID must remain
unique while the task is active. A framework can reuse an ID _only_
if the previous task with the same ID has reached a terminal state
(e.g., TASK_FINISHED, TASK_LOST, TASK_KILLED, etc.). However,
reusing task IDs is strongly discouraged (MESOS-2198).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.TaskInfo"/>

### TaskInfo
Describes a task. Passed from the scheduler all the way to an
executor (see SchedulerDriver::launchTasks and
Executor::launchTask). Either ExecutorInfo or CommandInfo should be set.
A different executor can be used to launch this task, and subsequent tasks
meant for the same executor can reuse the same ExecutorInfo struct.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) | required |  |
| task_id | [TaskID](#mesos.v1.TaskID) | required |  |
| agent_id | [AgentID](#mesos.v1.AgentID) | required |  |
| resources | [Resource](#mesos.v1.Resource) | repeated |  |
| executor | [ExecutorInfo](#mesos.v1.ExecutorInfo) | optional |  |
| command | [CommandInfo](#mesos.v1.CommandInfo) | optional |  |
| container | [ContainerInfo](#mesos.v1.ContainerInfo) | optional | Task provided with a container will launch the container as part of this task paired with the task&#39;s CommandInfo. |
| health_check | [HealthCheck](#mesos.v1.HealthCheck) | optional | A health check for the task. Implemented for executor-less command-based tasks. For tasks that specify an executor, it is the executor&#39;s responsibility to implement the health checking. |
| check | [CheckInfo](#mesos.v1.CheckInfo) | optional | A general check for the task. Implemented for all built-in executors. For tasks that specify an executor, it is the executor&#39;s responsibility to implement checking support. Executors should (all built-in executors will) neither interpret nor act on the check&#39;s result.

NOTE: Check support in built-in executors is experimental.

TODO(alexr): Consider supporting multiple checks per task. |
| kill_policy | [KillPolicy](#mesos.v1.KillPolicy) | optional | A kill policy for the task. Implemented for executor-less command-based and docker tasks. For tasks that specify an executor, it is the executor&#39;s responsibility to implement the kill policy. |
| data | [bytes](#bytes) | optional |  |
| labels | [Labels](#mesos.v1.Labels) | optional | Labels are free-form key value pairs which are exposed through master and agent endpoints. Labels will not be interpreted or acted upon by Mesos itself. As opposed to the data field, labels will be kept in memory on master and agent processes. Therefore, labels should be used to tag tasks with light-weight meta-data. Labels should not contain duplicate key-value pairs. |
| discovery | [DiscoveryInfo](#mesos.v1.DiscoveryInfo) | optional | Service discovery information for the task. It is not interpreted or acted upon by Mesos. It is up to a service discovery system to use this information as needed and to handle tasks without service discovery information. |






<a name="mesos.v1.TaskStatus"/>

### TaskStatus
Describes the current status of a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_id | [TaskID](#mesos.v1.TaskID) | required |  |
| state | [TaskState](#mesos.v1.TaskState) | required |  |
| message | [string](#string) | optional | Possible message explaining state. |
| source | [TaskStatus.Source](#mesos.v1.TaskStatus.Source) | optional |  |
| reason | [TaskStatus.Reason](#mesos.v1.TaskStatus.Reason) | optional |  |
| data | [bytes](#bytes) | optional |  |
| agent_id | [AgentID](#mesos.v1.AgentID) | optional |  |
| executor_id | [ExecutorID](#mesos.v1.ExecutorID) | optional | TODO(benh): Use in master/agent. |
| timestamp | [double](#double) | optional |  |
| uuid | [bytes](#bytes) | optional | Statuses that are delivered reliably to the scheduler will include a &#39;uuid&#39;. The status is considered delivered once it is acknowledged by the scheduler. Schedulers can choose to either explicitly acknowledge statuses or let the scheduler driver implicitly acknowledge (default).

TODO(bmahler): This is currently overwritten in the scheduler driver and executor driver, but executors will need to set this to a valid RFC-4122 UUID if using the HTTP API. |
| healthy | [bool](#bool) | optional | Describes whether the task has been determined to be healthy (true) or unhealthy (false) according to the `health_check` field in `TaskInfo`. |
| check_status | [CheckStatusInfo](#mesos.v1.CheckStatusInfo) | optional | Contains check status for the check specified in the corresponding `TaskInfo`. If no check has been specified, this field must be absent, otherwise it must be present even if the check status is not available yet. If the status update is triggered for a different reason than `REASON_TASK_CHECK_STATUS_UPDATED`, this field will contain the last known value.

NOTE: A check-related task status update is triggered if and only if the value or presence of any field in `CheckStatusInfo` changes.

NOTE: Check support in built-in executors is experimental. |
| labels | [Labels](#mesos.v1.Labels) | optional | Labels are free-form key value pairs which are exposed through master and agent endpoints. Labels will not be interpreted or acted upon by Mesos itself. As opposed to the data field, labels will be kept in memory on master and agent processes. Therefore, labels should be used to tag TaskStatus message with light-weight meta-data. Labels should not contain duplicate key-value pairs. |
| container_status | [ContainerStatus](#mesos.v1.ContainerStatus) | optional | Container related information that is resolved dynamically such as network address. |
| unreachable_time | [TimeInfo](#mesos.v1.TimeInfo) | optional | The time (according to the master&#39;s clock) when the agent where this task was running became unreachable. This is only set on status updates for tasks running on agents that are unreachable (e.g., partitioned away from the master). |






<a name="mesos.v1.TcpStatistics"/>

### TcpStatistics



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| RtoAlgorithm | [int64](#int64) | optional |  |
| RtoMin | [int64](#int64) | optional |  |
| RtoMax | [int64](#int64) | optional |  |
| MaxConn | [int64](#int64) | optional |  |
| ActiveOpens | [int64](#int64) | optional |  |
| PassiveOpens | [int64](#int64) | optional |  |
| AttemptFails | [int64](#int64) | optional |  |
| EstabResets | [int64](#int64) | optional |  |
| CurrEstab | [int64](#int64) | optional |  |
| InSegs | [int64](#int64) | optional |  |
| OutSegs | [int64](#int64) | optional |  |
| RetransSegs | [int64](#int64) | optional |  |
| InErrs | [int64](#int64) | optional |  |
| OutRsts | [int64](#int64) | optional |  |
| InCsumErrors | [int64](#int64) | optional |  |






<a name="mesos.v1.TimeInfo"/>

### TimeInfo
Represents time since the epoch, in nanoseconds.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| nanoseconds | [int64](#int64) | required |  |






<a name="mesos.v1.TrafficControlStatistics"/>

### TrafficControlStatistics
When the network bandwidth caps are enabled and the container
is over its limit, outbound packets may be either delayed or
dropped completely either because it exceeds the maximum bandwidth
allocation for a single container (the cap) or because the combined
network traffic of multiple containers on the host exceeds the
transmit capacity of the host (the share). We can report the
following statistics for each of these conditions exported directly
from the Linux Traffic Control Queueing Discipline.

id         : name of the limiter, e.g. &#39;tx_bw_cap&#39;
backlog    : number of packets currently delayed
bytes      : total bytes seen
drops      : number of packets dropped in total
overlimits : number of packets which exceeded allocation
packets    : total packets seen
qlen       : number of packets currently queued
rate_bps   : throughput in bytes/sec
rate_pps   : throughput in packets/sec
requeues   : number of times a packet has been delayed due to
locking or device contention issues

More information on the operation of Linux Traffic Control can be
found at http://www.lartc.org/lartc.html.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) | required |  |
| backlog | [uint64](#uint64) | optional |  |
| bytes | [uint64](#uint64) | optional |  |
| drops | [uint64](#uint64) | optional |  |
| overlimits | [uint64](#uint64) | optional |  |
| packets | [uint64](#uint64) | optional |  |
| qlen | [uint64](#uint64) | optional |  |
| ratebps | [uint64](#uint64) | optional |  |
| ratepps | [uint64](#uint64) | optional |  |
| requeues | [uint64](#uint64) | optional |  |






<a name="mesos.v1.URL"/>

### URL
Represents a URL.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| scheme | [string](#string) | required |  |
| address | [Address](#mesos.v1.Address) | required |  |
| path | [string](#string) | optional |  |
| query | [Parameter](#mesos.v1.Parameter) | repeated |  |
| fragment | [string](#string) | optional |  |






<a name="mesos.v1.UdpStatistics"/>

### UdpStatistics



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| InDatagrams | [int64](#int64) | optional |  |
| NoPorts | [int64](#int64) | optional |  |
| InErrors | [int64](#int64) | optional |  |
| OutDatagrams | [int64](#int64) | optional |  |
| RcvbufErrors | [int64](#int64) | optional |  |
| SndbufErrors | [int64](#int64) | optional |  |
| InCsumErrors | [int64](#int64) | optional |  |
| IgnoredMulti | [int64](#int64) | optional |  |






<a name="mesos.v1.Unavailability"/>

### Unavailability
Represents an interval, from a given start time over a given duration.
This interval pertains to an unavailability event, such as maintenance,
and is not a generic interval.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| start | [TimeInfo](#mesos.v1.TimeInfo) | required |  |
| duration | [DurationInfo](#mesos.v1.DurationInfo) | optional | When added to `start`, this represents the end of the interval. If unspecified, the duration is assumed to be infinite. |






<a name="mesos.v1.Value"/>

### Value
Describes an Attribute or Resource &#34;value&#34;. A value is described
using the standard protocol buffer &#34;union&#34; trick.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Value.Type](#mesos.v1.Value.Type) | required |  |
| scalar | [Value.Scalar](#mesos.v1.Value.Scalar) | optional |  |
| ranges | [Value.Ranges](#mesos.v1.Value.Ranges) | optional |  |
| set | [Value.Set](#mesos.v1.Value.Set) | optional |  |
| text | [Value.Text](#mesos.v1.Value.Text) | optional |  |






<a name="mesos.v1.Value.Range"/>

### Value.Range



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| begin | [uint64](#uint64) | required |  |
| end | [uint64](#uint64) | required |  |






<a name="mesos.v1.Value.Ranges"/>

### Value.Ranges



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| range | [Value.Range](#mesos.v1.Value.Range) | repeated |  |






<a name="mesos.v1.Value.Scalar"/>

### Value.Scalar



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [double](#double) | required | Scalar values are represented using floating point. To reduce the chance of unpredictable floating point behavior due to roundoff error, Mesos only supports three decimal digits of precision for scalar resource values. That is, floating point values are converted to a fixed point format that supports three decimal digits of precision, and then converted back to floating point on output. Any additional precision in scalar resource values is discarded (via rounding). |






<a name="mesos.v1.Value.Set"/>

### Value.Set



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| item | [string](#string) | repeated |  |






<a name="mesos.v1.Value.Text"/>

### Value.Text



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) | required |  |






<a name="mesos.v1.VersionInfo"/>

### VersionInfo
Version information of a component.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) | required |  |
| build_date | [string](#string) | optional |  |
| build_time | [double](#double) | optional |  |
| build_user | [string](#string) | optional |  |
| git_sha | [string](#string) | optional |  |
| git_branch | [string](#string) | optional |  |
| git_tag | [string](#string) | optional |  |






<a name="mesos.v1.Volume"/>

### Volume
Describes a volume mapping either from host to container or vice
versa. Both paths can either refer to a directory or a file.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| mode | [Volume.Mode](#mesos.v1.Volume.Mode) | required | TODO(gyliu513): Make this as `optional` after deprecation cycle of 1.0. |
| container_path | [string](#string) | required | Path pointing to a directory or file in the container. If the path is a relative path, it is relative to the container work directory. If the path is an absolute path, that path must already exist. |
| host_path | [string](#string) | optional | Absolute path pointing to a directory or file on the host or a path relative to the container work directory. |
| image | [Image](#mesos.v1.Image) | optional | The source of the volume is an Image which describes a root filesystem which will be provisioned by Mesos. |
| source | [Volume.Source](#mesos.v1.Volume.Source) | optional |  |






<a name="mesos.v1.Volume.Source"/>

### Volume.Source
Describes where a volume originates from.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Volume.Source.Type](#mesos.v1.Volume.Source.Type) | optional | Enum fields should be optional, see: MESOS-4997. |
| docker_volume | [Volume.Source.DockerVolume](#mesos.v1.Volume.Source.DockerVolume) | optional | The source of the volume created by docker volume driver. |
| sandbox_path | [Volume.Source.SandboxPath](#mesos.v1.Volume.Source.SandboxPath) | optional |  |






<a name="mesos.v1.Volume.Source.DockerVolume"/>

### Volume.Source.DockerVolume



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| driver | [string](#string) | optional | Driver of the volume, it can be flocker, convoy, raxrey etc. |
| name | [string](#string) | required | Name of the volume. |
| driver_options | [Parameters](#mesos.v1.Parameters) | optional | Volume driver specific options. |






<a name="mesos.v1.Volume.Source.SandboxPath"/>

### Volume.Source.SandboxPath
Describe a path from a container&#39;s sandbox. The container can
be the current container (SELF), or its parent container
(PARENT). PARENT allows all child containers to share a volume
from their parent container&#39;s sandbox. It&#39;ll be an error if
the current container is a top level container.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Volume.Source.SandboxPath.Type](#mesos.v1.Volume.Source.SandboxPath.Type) | optional |  |
| path | [string](#string) | required | A path relative to the corresponding container&#39;s sandbox. Note that upwards traversal (i.e. ../../abc) is not allowed. |






<a name="mesos.v1.WeightInfo"/>

### WeightInfo
Named WeightInfo to indicate resource allocation
priority between the different roles.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| weight | [double](#double) | required |  |
| role | [string](#string) | optional | Related role name. |





 


<a name="mesos.v1.AgentInfo.Capability.Type"/>

### AgentInfo.Capability.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This must be the first enum value in this list, to ensure that if &#39;type&#39; is not set, the default value is UNKNOWN. This enables enum values to be added in a backwards-compatible way. See: MESOS-4997. |
| MULTI_ROLE | 1 | This expresses the ability for the agent to be able to launch tasks of a &#39;multi-role&#39; framework.

NOTE: The implementation for supporting multiple roles is not complete, DO NOT USE THIS.

EXPERIMENTAL. |



<a name="mesos.v1.CapabilityInfo.Capability"/>

### CapabilityInfo.Capability
We start the actual values at an offset(1000) because Protobuf 2
uses the first value as the default one. Separating the default
value from the real first value helps to disambiguate them. This
is especially valuable for backward compatibility.
See: MESOS-4997.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| CHOWN | 1000 |  |
| DAC_OVERRIDE | 1001 |  |
| DAC_READ_SEARCH | 1002 |  |
| FOWNER | 1003 |  |
| FSETID | 1004 |  |
| KILL | 1005 |  |
| SETGID | 1006 |  |
| SETUID | 1007 |  |
| SETPCAP | 1008 |  |
| LINUX_IMMUTABLE | 1009 |  |
| NET_BIND_SERVICE | 1010 |  |
| NET_BROADCAST | 1011 |  |
| NET_ADMIN | 1012 |  |
| NET_RAW | 1013 |  |
| IPC_LOCK | 1014 |  |
| IPC_OWNER | 1015 |  |
| SYS_MODULE | 1016 |  |
| SYS_RAWIO | 1017 |  |
| SYS_CHROOT | 1018 |  |
| SYS_PTRACE | 1019 |  |
| SYS_PACCT | 1020 |  |
| SYS_ADMIN | 1021 |  |
| SYS_BOOT | 1022 |  |
| SYS_NICE | 1023 |  |
| SYS_RESOURCE | 1024 |  |
| SYS_TIME | 1025 |  |
| SYS_TTY_CONFIG | 1026 |  |
| MKNOD | 1027 |  |
| LEASE | 1028 |  |
| AUDIT_WRITE | 1029 |  |
| AUDIT_CONTROL | 1030 |  |
| SETFCAP | 1031 |  |
| MAC_OVERRIDE | 1032 |  |
| MAC_ADMIN | 1033 |  |
| SYSLOG | 1034 |  |
| WAKE_ALARM | 1035 |  |
| BLOCK_SUSPEND | 1036 |  |
| AUDIT_READ | 1037 |  |



<a name="mesos.v1.CheckInfo.Type"/>

### CheckInfo.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| COMMAND | 1 |  |
| HTTP | 2 |  |



<a name="mesos.v1.ContainerInfo.DockerInfo.Network"/>

### ContainerInfo.DockerInfo.Network
Network options.

| Name | Number | Description |
| ---- | ------ | ----------- |
| HOST | 1 |  |
| BRIDGE | 2 |  |
| NONE | 3 |  |
| USER | 4 |  |



<a name="mesos.v1.ContainerInfo.Type"/>

### ContainerInfo.Type
All container implementation types.

| Name | Number | Description |
| ---- | ------ | ----------- |
| DOCKER | 1 |  |
| MESOS | 2 |  |



<a name="mesos.v1.DiscoveryInfo.Visibility"/>

### DiscoveryInfo.Visibility


| Name | Number | Description |
| ---- | ------ | ----------- |
| FRAMEWORK | 0 |  |
| CLUSTER | 1 |  |
| EXTERNAL | 2 |  |



<a name="mesos.v1.Environment.Variable.Type"/>

### Environment.Variable.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| VALUE | 1 |  |
| SECRET | 2 |  |



<a name="mesos.v1.ExecutorInfo.Type"/>

### ExecutorInfo.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| DEFAULT | 1 | Mesos provides a simple built-in default executor that frameworks can leverage to run shell commands and containers.

NOTES:

1) `command` must not be set when using a default executor.

2) Default executor only accepts a *single* `LAUNCH` or `LAUNCH_GROUP` offer operation.

3) If `container` is set, `container.type` must be `MESOS` and `container.mesos.image` must not be set. |
| CUSTOM | 2 | For frameworks that need custom functionality to run tasks, a `CUSTOM` executor can be used. Note that `command` must be set when using a `CUSTOM` executor. |



<a name="mesos.v1.FrameworkInfo.Capability.Type"/>

### FrameworkInfo.Capability.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This must be the first enum value in this list, to ensure that if &#39;type&#39; is not set, the default value is UNKNOWN. This enables enum values to be added in a backwards-compatible way. See: MESOS-4997. |
| REVOCABLE_RESOURCES | 1 | Receive offers with revocable resources. See &#39;Resource&#39; message for details. |
| TASK_KILLING_STATE | 2 | Receive the TASK_KILLING TaskState when a task is being killed by an executor. The executor will examine this capability to determine whether it can send TASK_KILLING. |
| GPU_RESOURCES | 3 | Indicates whether the framework is aware of GPU resources. Frameworks that are aware of GPU resources are expected to avoid placing non-GPU workloads on GPU agents, in order to avoid occupying a GPU agent and preventing GPU workloads from running! Currently, if a framework is unaware of GPU resources, it will not be offered *any* of the resources on an agent with GPUs. This restriction is in place because we do not have a revocation mechanism that ensures GPU workloads can evict GPU agent occupants if necessary.

TODO(bmahler): As we add revocation we can relax the restriction here. See MESOS-5634 for more information. |
| SHARED_RESOURCES | 4 | Receive offers with resources that are shared. |
| PARTITION_AWARE | 5 | Indicates that (1) the framework is prepared to handle the following TaskStates: TASK_UNREACHABLE, TASK_DROPPED, TASK_GONE, TASK_GONE_BY_OPERATOR, and TASK_UNKNOWN, and (2) the framework will assume responsibility for managing partitioned tasks that reregister with the master.

Frameworks that enable this capability can define how they would like to handle partitioned tasks. Frameworks will receive TASK_UNREACHABLE for tasks on agents that are partitioned from the master. If/when a partitioned agent reregisters, tasks on the agent that were started by PARTITION_AWARE frameworks will not killed.

Without this capability, frameworks will receive TASK_LOST for tasks on partitioned agents; such tasks will be killed by Mesos when the agent reregisters (unless the master has failed over). |
| MULTI_ROLE | 6 | This expresses the ability for the framework to be &#34;multi-tenant&#34; via using the newly introduced `roles` field, and examining `Offer.allocation_info` to determine which role the offers are being made to. We also expect that &#34;single-tenant&#34; schedulers eventually provide this and move away from the deprecated `role` field.

NOTE: The implementation for supporting multiple roles is not complete, DO NOT USE THIS.

EXPERIMENTAL. |



<a name="mesos.v1.HealthCheck.Type"/>

### HealthCheck.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| COMMAND | 1 |  |
| HTTP | 2 |  |
| TCP | 3 |  |



<a name="mesos.v1.Image.Type"/>

### Image.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| APPC | 1 |  |
| DOCKER | 2 |  |



<a name="mesos.v1.MachineInfo.Mode"/>

### MachineInfo.Mode
Describes the several states that a machine can be in.  A `Mode`
applies to a machine and to all associated agents on the machine.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UP | 1 | In this mode, a machine is behaving normally; offering resources, executing tasks, etc. |
| DRAINING | 2 | In this mode, all agents on the machine are expected to cooperate with frameworks to drain resources. In general, draining is done ahead of a pending `unavailability`. The resources should be drained so as to maximize utilization prior to the maintenance but without knowingly violating the frameworks&#39; requirements. |
| DOWN | 3 | In this mode, a machine is not running any tasks and will not offer any of its resources. Agents on the machine will not be allowed to register with the master. |



<a name="mesos.v1.NetworkInfo.Protocol"/>

### NetworkInfo.Protocol


| Name | Number | Description |
| ---- | ------ | ----------- |
| IPv4 | 1 |  |
| IPv6 | 2 |  |



<a name="mesos.v1.Offer.Operation.Type"/>

### Offer.Operation.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| LAUNCH | 1 |  |
| LAUNCH_GROUP | 6 |  |
| RESERVE | 2 |  |
| UNRESERVE | 3 |  |
| CREATE | 4 |  |
| DESTROY | 5 |  |



<a name="mesos.v1.RLimitInfo.RLimit.Type"/>

### RLimitInfo.RLimit.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| RLMT_AS | 1 |  |
| RLMT_CORE | 2 |  |
| RLMT_CPU | 3 |  |
| RLMT_DATA | 4 |  |
| RLMT_FSIZE | 5 |  |
| RLMT_LOCKS | 6 |  |
| RLMT_MEMLOCK | 7 |  |
| RLMT_MSGQUEUE | 8 |  |
| RLMT_NICE | 9 |  |
| RLMT_NOFILE | 10 |  |
| RLMT_NPROC | 11 |  |
| RLMT_RSS | 12 |  |
| RLMT_RTPRIO | 13 |  |
| RLMT_RTTIME | 14 |  |
| RLMT_SIGPENDING | 15 |  |
| RLMT_STACK | 16 |  |



<a name="mesos.v1.Resource.DiskInfo.Source.Type"/>

### Resource.DiskInfo.Source.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| PATH | 1 |  |
| MOUNT | 2 |  |



<a name="mesos.v1.Secret.Type"/>

### Secret.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| REFERENCE | 1 |  |
| VALUE | 2 |  |



<a name="mesos.v1.Status"/>

### Status
Status is used to indicate the state of the scheduler and executor
driver after function calls.

| Name | Number | Description |
| ---- | ------ | ----------- |
| DRIVER_NOT_STARTED | 1 |  |
| DRIVER_RUNNING | 2 |  |
| DRIVER_ABORTED | 3 |  |
| DRIVER_STOPPED | 4 |  |



<a name="mesos.v1.TaskState"/>

### TaskState
Describes possible task states. IMPORTANT: Mesos assumes tasks that
enter terminal states (see below) imply the task is no longer
running and thus clean up any thing associated with the task
(ultimately offering any resources being consumed by that task to
another task).

| Name | Number | Description |
| ---- | ------ | ----------- |
| TASK_STAGING | 6 | Initial state. Framework status updates should not use. |
| TASK_STARTING | 0 | The task is being launched by the executor. |
| TASK_RUNNING | 1 |  |
| TASK_KILLING | 8 | NOTE: This should only be sent when the framework has the TASK_KILLING_STATE capability.

The task is being killed by the executor. |
| TASK_FINISHED | 2 | TERMINAL: The task finished successfully. |
| TASK_FAILED | 3 | TERMINAL: The task failed to finish successfully. |
| TASK_KILLED | 4 | TERMINAL: The task was killed by the executor. |
| TASK_ERROR | 7 | TERMINAL: The task description contains an error. |
| TASK_LOST | 5 | In Mesos 1.3, this will only be sent when the framework does NOT opt-in to the PARTITION_AWARE capability.

TERMINAL: The task failed but can be rescheduled. |
| TASK_DROPPED | 9 | The task failed to launch because of a transient error. The task&#39;s executor never started running. Unlike TASK_ERROR, the task description is valid -- attempting to launch the task again may be successful.

TERMINAL. |
| TASK_UNREACHABLE | 10 | The task was running on an agent that has lost contact with the master, typically due to a network failure or partition. The task may or may not still be running. |
| TASK_GONE | 11 | The task is no longer running. This can occur if the agent has been terminated along with all of its tasks (e.g., the host that was running the agent was rebooted). It might also occur if the task was terminated due to an agent or containerizer error, or if the task was preempted by the QoS controller in an oversubscription scenario.

TERMINAL. |
| TASK_GONE_BY_OPERATOR | 12 | The task was running on an agent that the master cannot contact; the operator has asserted that the agent has been shutdown, but this has not been directly confirmed by the master. If the operator is correct, the task is not running and this is a terminal state; if the operator is mistaken, the task may still be running and might return to RUNNING in the future. |
| TASK_UNKNOWN | 13 | The master has no knowledge of the task. This is typically because either (a) the master never had knowledge of the task, or (b) the master forgot about the task because it garbage collected its metadata about the task. The task may or may not still be running. |



<a name="mesos.v1.TaskStatus.Reason"/>

### TaskStatus.Reason
Detailed reason for the task status update.

TODO(bmahler): Differentiate between agent removal reasons
(e.g. unhealthy vs. unregistered for maintenance).

| Name | Number | Description |
| ---- | ------ | ----------- |
| REASON_COMMAND_EXECUTOR_FAILED | 0 | TODO(jieyu): The default value when a caller doesn&#39;t check for presence is 0 and so ideally the 0 reason is not a valid one. Since this is not used anywhere, consider removing this reason. |
| REASON_CONTAINER_LAUNCH_FAILED | 21 |  |
| REASON_CONTAINER_LIMITATION | 19 |  |
| REASON_CONTAINER_LIMITATION_DISK | 20 |  |
| REASON_CONTAINER_LIMITATION_MEMORY | 8 |  |
| REASON_CONTAINER_PREEMPTED | 17 |  |
| REASON_CONTAINER_UPDATE_FAILED | 22 |  |
| REASON_EXECUTOR_REGISTRATION_TIMEOUT | 23 |  |
| REASON_EXECUTOR_REREGISTRATION_TIMEOUT | 24 |  |
| REASON_EXECUTOR_TERMINATED | 1 |  |
| REASON_EXECUTOR_UNREGISTERED | 2 |  |
| REASON_FRAMEWORK_REMOVED | 3 |  |
| REASON_GC_ERROR | 4 |  |
| REASON_INVALID_FRAMEWORKID | 5 |  |
| REASON_INVALID_OFFERS | 6 |  |
| REASON_IO_SWITCHBOARD_EXITED | 27 |  |
| REASON_MASTER_DISCONNECTED | 7 |  |
| REASON_RECONCILIATION | 9 |  |
| REASON_RESOURCES_UNKNOWN | 18 |  |
| REASON_AGENT_DISCONNECTED | 10 |  |
| REASON_AGENT_REMOVED | 11 |  |
| REASON_AGENT_RESTARTED | 12 |  |
| REASON_AGENT_UNKNOWN | 13 |  |
| REASON_TASK_CHECK_STATUS_UPDATED | 28 |  |
| REASON_TASK_GROUP_INVALID | 25 |  |
| REASON_TASK_GROUP_UNAUTHORIZED | 26 |  |
| REASON_TASK_INVALID | 14 |  |
| REASON_TASK_UNAUTHORIZED | 15 |  |
| REASON_TASK_UNKNOWN | 16 |  |



<a name="mesos.v1.TaskStatus.Source"/>

### TaskStatus.Source
Describes the source of the task status update.

| Name | Number | Description |
| ---- | ------ | ----------- |
| SOURCE_MASTER | 0 |  |
| SOURCE_AGENT | 1 |  |
| SOURCE_EXECUTOR | 2 |  |



<a name="mesos.v1.Value.Type"/>

### Value.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| SCALAR | 0 |  |
| RANGES | 1 |  |
| SET | 2 |  |
| TEXT | 3 |  |



<a name="mesos.v1.Volume.Mode"/>

### Volume.Mode


| Name | Number | Description |
| ---- | ------ | ----------- |
| RW | 1 | read-write. |
| RO | 2 | read-only. |



<a name="mesos.v1.Volume.Source.SandboxPath.Type"/>

### Volume.Source.SandboxPath.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| SELF | 1 |  |
| PARENT | 2 |  |



<a name="mesos.v1.Volume.Source.Type"/>

### Volume.Source.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This must be the first enum value in this list, to ensure that if &#39;type&#39; is not set, the default value is UNKNOWN. This enables enum values to be added in a backwards-compatible way. See: MESOS-4997. |
| DOCKER_VOLUME | 1 | TODO(gyliu513): Add HOST_PATH and IMAGE as volume source type. |
| SANDBOX_PATH | 2 |  |


 

 

 



<a name="peloton.proto"/>
<p align="right"><a href="#top">Top</a></p>

## peloton.proto



<a name="peloton.api.peloton.ChangeLog"/>

### ChangeLog
Change log of an entity info, such as Job config etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [uint64](#uint64) |  | Version number of the entity info which is monotonically increasing. Clients can use this to guide against race conditions using MVCC. |
| createdAt | [uint64](#uint64) |  | The timestamp when the entity info is created |
| updatedAt | [uint64](#uint64) |  | The timestamp when the entity info is updated |
| updatedBy | [string](#string) |  | The user or service that updated the entity info |






<a name="peloton.api.peloton.JobID"/>

### JobID
A unique ID assigned to a Job. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.peloton.Label"/>

### Label
Key, value pair used to store free form user-data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="peloton.api.peloton.ResourcePoolID"/>

### ResourcePoolID
A unique ID assigned to a Resource Pool. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.peloton.TaskID"/>

### TaskID
A unique ID assigned to a Task (aka job instance). The task ID is in
the format of JobID-&lt;InstanceID&gt;.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.peloton.VolumeID"/>

### VolumeID
A unique ID assigned to a Volume. This is a UUID in RFC4122 format.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 

 

 

 



<a name="agent.proto"/>
<p align="right"><a href="#top">Top</a></p>

## agent.proto



<a name="mesos.v1.agent.Call"/>

### Call
Calls that can be sent to the v1 agent API.

A call is described using the standard protocol buffer &#34;union&#34;
trick, see
https://developers.google.com/protocol-buffers/docs/techniques#union.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Call.Type](#mesos.v1.agent.Call.Type) | optional |  |
| get_metrics | [Call.GetMetrics](#mesos.v1.agent.Call.GetMetrics) | optional |  |
| set_logging_level | [Call.SetLoggingLevel](#mesos.v1.agent.Call.SetLoggingLevel) | optional |  |
| list_files | [Call.ListFiles](#mesos.v1.agent.Call.ListFiles) | optional |  |
| read_file | [Call.ReadFile](#mesos.v1.agent.Call.ReadFile) | optional |  |
| launch_nested_container | [Call.LaunchNestedContainer](#mesos.v1.agent.Call.LaunchNestedContainer) | optional |  |
| wait_nested_container | [Call.WaitNestedContainer](#mesos.v1.agent.Call.WaitNestedContainer) | optional |  |
| kill_nested_container | [Call.KillNestedContainer](#mesos.v1.agent.Call.KillNestedContainer) | optional |  |
| launch_nested_container_session | [Call.LaunchNestedContainerSession](#mesos.v1.agent.Call.LaunchNestedContainerSession) | optional |  |
| attach_container_input | [Call.AttachContainerInput](#mesos.v1.agent.Call.AttachContainerInput) | optional |  |
| attach_container_output | [Call.AttachContainerOutput](#mesos.v1.agent.Call.AttachContainerOutput) | optional |  |






<a name="mesos.v1.agent.Call.AttachContainerInput"/>

### Call.AttachContainerInput
Attaches the caller to the STDIN of the entry point of the container.
Clients can use this to stream input data to a container.
Note that this call needs to be made on a persistent connection by
streaming a CONTAINER_ID message followed by one or more PROCESS_IO
messages.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Call.AttachContainerInput.Type](#mesos.v1.agent.Call.AttachContainerInput.Type) | optional |  |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | optional |  |
| process_io | [ProcessIO](#mesos.v1.agent.ProcessIO) | optional |  |






<a name="mesos.v1.agent.Call.AttachContainerOutput"/>

### Call.AttachContainerOutput
Attaches the caller to the STDOUT and STDERR of the entrypoint of
the container. Clients can use this to stream output/error from the
container. This call will result in a streaming response of `ProcessIO`;
so this call needs to be made on a persistent connection.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |






<a name="mesos.v1.agent.Call.GetMetrics"/>

### Call.GetMetrics
Provides a snapshot of the current metrics tracked by the agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timeout | [.mesos.v1.DurationInfo](#mesos.v1.agent..mesos.v1.DurationInfo) | optional | If set, `timeout` would be used to determines the maximum amount of time the API will take to respond. If the timeout is exceeded, some metrics may not be included in the response. |






<a name="mesos.v1.agent.Call.KillNestedContainer"/>

### Call.KillNestedContainer
Kills the nested container. Currently only supports SIGKILL.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |






<a name="mesos.v1.agent.Call.LaunchNestedContainer"/>

### Call.LaunchNestedContainer
Launches a nested container within an executor&#39;s tree of containers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |
| command | [.mesos.v1.CommandInfo](#mesos.v1.agent..mesos.v1.CommandInfo) | optional |  |
| container | [.mesos.v1.ContainerInfo](#mesos.v1.agent..mesos.v1.ContainerInfo) | optional |  |






<a name="mesos.v1.agent.Call.LaunchNestedContainerSession"/>

### Call.LaunchNestedContainerSession
Launches a nested container within an executor&#39;s tree of containers.
The differences between this call and `LaunchNestedContainer` are:
1) The container&#39;s life-cycle is tied to the lifetime of the
connection used to make this call, i.e., if the connection ever
breaks, the container will be destroyed.
2) The nested container shares the same namespaces and cgroups as
its parent container.
3) Results in a streaming response of type `ProcessIO`. So the call
needs to be made on a persistent connection.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |
| command | [.mesos.v1.CommandInfo](#mesos.v1.agent..mesos.v1.CommandInfo) | optional |  |
| container | [.mesos.v1.ContainerInfo](#mesos.v1.agent..mesos.v1.ContainerInfo) | optional |  |






<a name="mesos.v1.agent.Call.ListFiles"/>

### Call.ListFiles
Provides the file listing for a directory.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | required |  |






<a name="mesos.v1.agent.Call.ReadFile"/>

### Call.ReadFile
Reads data from a file.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | required | The path of file. |
| offset | [uint64](#uint64) | required | Initial offset in file to start reading from. |
| length | [uint64](#uint64) | optional | The maximum number of bytes to read. The read length is capped at 16 memory pages. |






<a name="mesos.v1.agent.Call.SetLoggingLevel"/>

### Call.SetLoggingLevel
Sets the logging verbosity level for a specified duration. Mesos uses
[glog](https://github.com/google/glog) for logging. The library only uses
verbose logging which means nothing will be output unless the verbosity
level is set (by default it&#39;s 0, libprocess uses levels 1, 2, and 3).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | [uint32](#uint32) | required | The verbosity level. |
| duration | [.mesos.v1.DurationInfo](#mesos.v1.agent..mesos.v1.DurationInfo) | required | The duration to keep verbosity level toggled. After this duration, the verbosity level of log would revert to the original level. |






<a name="mesos.v1.agent.Call.WaitNestedContainer"/>

### Call.WaitNestedContainer
Waits for the nested container to terminate and receives the exit status.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |






<a name="mesos.v1.agent.ProcessIO"/>

### ProcessIO
Streaming response to `Call::LAUNCH_NESTED_CONTAINER_SESSION` and
`Call::ATTACH_CONTAINER_OUTPUT`.

This message is also used to stream request data for
`Call::ATTACH_CONTAINER_INPUT`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ProcessIO.Type](#mesos.v1.agent.ProcessIO.Type) | optional |  |
| data | [ProcessIO.Data](#mesos.v1.agent.ProcessIO.Data) | optional |  |
| control | [ProcessIO.Control](#mesos.v1.agent.ProcessIO.Control) | optional |  |






<a name="mesos.v1.agent.ProcessIO.Control"/>

### ProcessIO.Control



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ProcessIO.Control.Type](#mesos.v1.agent.ProcessIO.Control.Type) | optional |  |
| tty_info | [.mesos.v1.TTYInfo](#mesos.v1.agent..mesos.v1.TTYInfo) | optional |  |
| heartbeat | [ProcessIO.Control.Heartbeat](#mesos.v1.agent.ProcessIO.Control.Heartbeat) | optional |  |






<a name="mesos.v1.agent.ProcessIO.Control.Heartbeat"/>

### ProcessIO.Control.Heartbeat



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| interval | [.mesos.v1.DurationInfo](#mesos.v1.agent..mesos.v1.DurationInfo) | optional |  |






<a name="mesos.v1.agent.ProcessIO.Data"/>

### ProcessIO.Data



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ProcessIO.Data.Type](#mesos.v1.agent.ProcessIO.Data.Type) | optional |  |
| data | [bytes](#bytes) | optional |  |






<a name="mesos.v1.agent.Response"/>

### Response
Synchronous responses for all calls made to the v1 agent API.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Response.Type](#mesos.v1.agent.Response.Type) | optional |  |
| get_health | [Response.GetHealth](#mesos.v1.agent.Response.GetHealth) | optional |  |
| get_flags | [Response.GetFlags](#mesos.v1.agent.Response.GetFlags) | optional |  |
| get_version | [Response.GetVersion](#mesos.v1.agent.Response.GetVersion) | optional |  |
| get_metrics | [Response.GetMetrics](#mesos.v1.agent.Response.GetMetrics) | optional |  |
| get_logging_level | [Response.GetLoggingLevel](#mesos.v1.agent.Response.GetLoggingLevel) | optional |  |
| list_files | [Response.ListFiles](#mesos.v1.agent.Response.ListFiles) | optional |  |
| read_file | [Response.ReadFile](#mesos.v1.agent.Response.ReadFile) | optional |  |
| get_state | [Response.GetState](#mesos.v1.agent.Response.GetState) | optional |  |
| get_containers | [Response.GetContainers](#mesos.v1.agent.Response.GetContainers) | optional |  |
| get_frameworks | [Response.GetFrameworks](#mesos.v1.agent.Response.GetFrameworks) | optional |  |
| get_executors | [Response.GetExecutors](#mesos.v1.agent.Response.GetExecutors) | optional |  |
| get_tasks | [Response.GetTasks](#mesos.v1.agent.Response.GetTasks) | optional |  |
| wait_nested_container | [Response.WaitNestedContainer](#mesos.v1.agent.Response.WaitNestedContainer) | optional |  |






<a name="mesos.v1.agent.Response.GetContainers"/>

### Response.GetContainers
Information about containers running on this agent. It contains
ContainerStatus and ResourceStatistics along with some metadata
of the containers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| containers | [Response.GetContainers.Container](#mesos.v1.agent.Response.GetContainers.Container) | repeated |  |






<a name="mesos.v1.agent.Response.GetContainers.Container"/>

### Response.GetContainers.Container



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.agent..mesos.v1.FrameworkID) | required |  |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.agent..mesos.v1.ExecutorID) | required |  |
| executor_name | [string](#string) | required |  |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.agent..mesos.v1.ContainerID) | required |  |
| container_status | [.mesos.v1.ContainerStatus](#mesos.v1.agent..mesos.v1.ContainerStatus) | optional |  |
| resource_statistics | [.mesos.v1.ResourceStatistics](#mesos.v1.agent..mesos.v1.ResourceStatistics) | optional |  |






<a name="mesos.v1.agent.Response.GetExecutors"/>

### Response.GetExecutors
Lists information about all the executors known to the agent at the
current time.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executors | [Response.GetExecutors.Executor](#mesos.v1.agent.Response.GetExecutors.Executor) | repeated |  |
| completed_executors | [Response.GetExecutors.Executor](#mesos.v1.agent.Response.GetExecutors.Executor) | repeated |  |






<a name="mesos.v1.agent.Response.GetExecutors.Executor"/>

### Response.GetExecutors.Executor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_info | [.mesos.v1.ExecutorInfo](#mesos.v1.agent..mesos.v1.ExecutorInfo) | required |  |






<a name="mesos.v1.agent.Response.GetFlags"/>

### Response.GetFlags
Contains the flag configuration of the agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flags | [.mesos.v1.Flag](#mesos.v1.agent..mesos.v1.Flag) | repeated |  |






<a name="mesos.v1.agent.Response.GetFrameworks"/>

### Response.GetFrameworks
Information about all the frameworks known to the agent at the current
time.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| frameworks | [Response.GetFrameworks.Framework](#mesos.v1.agent.Response.GetFrameworks.Framework) | repeated |  |
| completed_frameworks | [Response.GetFrameworks.Framework](#mesos.v1.agent.Response.GetFrameworks.Framework) | repeated |  |






<a name="mesos.v1.agent.Response.GetFrameworks.Framework"/>

### Response.GetFrameworks.Framework



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_info | [.mesos.v1.FrameworkInfo](#mesos.v1.agent..mesos.v1.FrameworkInfo) | required |  |






<a name="mesos.v1.agent.Response.GetHealth"/>

### Response.GetHealth
`healthy` would be true if the agent is healthy. Delayed responses are also
indicative of the poor health of the agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| healthy | [bool](#bool) | required |  |






<a name="mesos.v1.agent.Response.GetLoggingLevel"/>

### Response.GetLoggingLevel
Contains the logging level of the agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | [uint32](#uint32) | required |  |






<a name="mesos.v1.agent.Response.GetMetrics"/>

### Response.GetMetrics
Contains a snapshot of the current metrics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metrics | [.mesos.v1.Metric](#mesos.v1.agent..mesos.v1.Metric) | repeated |  |






<a name="mesos.v1.agent.Response.GetState"/>

### Response.GetState
Contains full state of the agent i.e. information about the tasks,
frameworks and executors running in the cluster.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| get_tasks | [Response.GetTasks](#mesos.v1.agent.Response.GetTasks) | optional |  |
| get_executors | [Response.GetExecutors](#mesos.v1.agent.Response.GetExecutors) | optional |  |
| get_frameworks | [Response.GetFrameworks](#mesos.v1.agent.Response.GetFrameworks) | optional |  |






<a name="mesos.v1.agent.Response.GetTasks"/>

### Response.GetTasks
Lists information about all the tasks known to the agent at the current
time.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pending_tasks | [.mesos.v1.Task](#mesos.v1.agent..mesos.v1.Task) | repeated | Tasks that are pending in the agent&#39;s queue before an executor is launched. |
| queued_tasks | [.mesos.v1.Task](#mesos.v1.agent..mesos.v1.Task) | repeated | Tasks that are enqueued for a launched executor that has not yet registered. |
| launched_tasks | [.mesos.v1.Task](#mesos.v1.agent..mesos.v1.Task) | repeated | Tasks that are running. |
| terminated_tasks | [.mesos.v1.Task](#mesos.v1.agent..mesos.v1.Task) | repeated | Tasks that are terminated but pending updates. |
| completed_tasks | [.mesos.v1.Task](#mesos.v1.agent..mesos.v1.Task) | repeated | Tasks that are terminated and updates acked. |






<a name="mesos.v1.agent.Response.GetVersion"/>

### Response.GetVersion
Contains the version information of the agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_info | [.mesos.v1.VersionInfo](#mesos.v1.agent..mesos.v1.VersionInfo) | required |  |






<a name="mesos.v1.agent.Response.ListFiles"/>

### Response.ListFiles
Contains the file listing(similar to `ls -l`) for a directory.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| file_infos | [.mesos.v1.FileInfo](#mesos.v1.agent..mesos.v1.FileInfo) | repeated |  |






<a name="mesos.v1.agent.Response.ReadFile"/>

### Response.ReadFile
Contains the file data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint64](#uint64) | required | The size of file (in bytes). |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.agent.Response.WaitNestedContainer"/>

### Response.WaitNestedContainer
Returns termination information about the nested container.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exit_status | [int32](#int32) | optional |  |





 


<a name="mesos.v1.agent.Call.AttachContainerInput.Type"/>

### Call.AttachContainerInput.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| CONTAINER_ID | 1 |  |
| PROCESS_IO | 2 |  |



<a name="mesos.v1.agent.Call.Type"/>

### Call.Type
If a call of type `Call::FOO` requires additional parameters they can be
included in the corresponding `Call::Foo` message. Similarly, if a call
receives a synchronous response it will be returned as a `Response`
message of type `Response::FOO`; see `Call::LaunchNestedContainerSession`
and `Call::AttachContainerOutput` for exceptions.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| GET_HEALTH | 1 | Retrieves the agent&#39;s health status. |
| GET_FLAGS | 2 | Retrieves the agent&#39;s flag configuration. |
| GET_VERSION | 3 | Retrieves the agent&#39;s version information. |
| GET_METRICS | 4 | See &#39;GetMetrics&#39; below. |
| GET_LOGGING_LEVEL | 5 | Retrieves the agent&#39;s logging level. |
| SET_LOGGING_LEVEL | 6 | See &#39;SetLoggingLevel&#39; below. |
| LIST_FILES | 7 |  |
| READ_FILE | 8 | See &#39;ReadFile&#39; below. |
| GET_STATE | 9 |  |
| GET_CONTAINERS | 10 |  |
| GET_FRAMEWORKS | 11 | Retrieves the information about known frameworks. |
| GET_EXECUTORS | 12 | Retrieves the information about known executors. |
| GET_TASKS | 13 | Retrieves the information about known tasks. |
| LAUNCH_NESTED_CONTAINER | 14 | Calls for managing nested containers underneath an executor&#39;s container.

See &#39;LaunchNestedContainer&#39; below. |
| WAIT_NESTED_CONTAINER | 15 | See &#39;WaitNestedContainer&#39; below. |
| KILL_NESTED_CONTAINER | 16 | See &#39;KillNestedContainer&#39; below. |
| LAUNCH_NESTED_CONTAINER_SESSION | 17 | See &#39;LaunchNestedContainerSession&#39; below. |
| ATTACH_CONTAINER_INPUT | 18 | See &#39;AttachContainerInput&#39; below. |
| ATTACH_CONTAINER_OUTPUT | 19 | see &#39;AttachContainerOutput&#39; below. |



<a name="mesos.v1.agent.ProcessIO.Control.Type"/>

### ProcessIO.Control.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| TTY_INFO | 1 |  |
| HEARTBEAT | 2 |  |



<a name="mesos.v1.agent.ProcessIO.Data.Type"/>

### ProcessIO.Data.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| STDIN | 1 |  |
| STDOUT | 2 |  |
| STDERR | 3 |  |



<a name="mesos.v1.agent.ProcessIO.Type"/>

### ProcessIO.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| DATA | 1 |  |
| CONTROL | 2 |  |



<a name="mesos.v1.agent.Response.Type"/>

### Response.Type
Each of the responses of type `FOO` corresponds to `Foo` message below.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| GET_HEALTH | 1 | See &#39;GetHealth&#39; below. |
| GET_FLAGS | 2 | See &#39;GetFlags&#39; below. |
| GET_VERSION | 3 | See &#39;GetVersion&#39; below. |
| GET_METRICS | 4 | See &#39;GetMetrics&#39; below. |
| GET_LOGGING_LEVEL | 5 | See &#39;GetLoggingLevel&#39; below. |
| LIST_FILES | 6 |  |
| READ_FILE | 7 | See &#39;ReadFile&#39; below. |
| GET_STATE | 8 |  |
| GET_CONTAINERS | 9 |  |
| GET_FRAMEWORKS | 10 | See &#39;GetFrameworks&#39; below. |
| GET_EXECUTORS | 11 | See &#39;GetExecutors&#39; below. |
| GET_TASKS | 12 | See &#39;GetTasks&#39; below. |
| WAIT_NESTED_CONTAINER | 13 | See &#39;WaitNestedContainer&#39; below. |


 

 

 



<a name="allocator.proto"/>
<p align="right"><a href="#top">Top</a></p>

## allocator.proto



<a name="mesos.v1.allocator.InverseOfferStatus"/>

### InverseOfferStatus
Describes the status of an inverse offer.

This is a protobuf so as to be able to share the status to inverse offers
through endpoints such as the maintenance status endpoint.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [InverseOfferStatus.Status](#mesos.v1.allocator.InverseOfferStatus.Status) | required |  |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.allocator..mesos.v1.FrameworkID) | required |  |
| timestamp | [.mesos.v1.TimeInfo](#mesos.v1.allocator..mesos.v1.TimeInfo) | required | Time, since the epoch, when this status was last updated. |





 


<a name="mesos.v1.allocator.InverseOfferStatus.Status"/>

### InverseOfferStatus.Status


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 1 | We have not received a response yet. This is the default state before receiving a response. |
| ACCEPT | 2 | The framework is ok with the inverse offer. This means it will not violate any SLAs and will attempt to evacuate any tasks running on the agent. If the tasks are not evacuated by the framework, the operator can manually shut down the slave knowing that the framework will not have violated its SLAs. |
| DECLINE | 3 | The framework wants to block the maintenance operation from happening. An example would be that it cannot meet its SLA by losing resources. |


 

 

 



<a name="executor.proto"/>
<p align="right"><a href="#top">Top</a></p>

## executor.proto



<a name="mesos.v1.executor.Call"/>

### Call
Executor call API.

Like Event, a Call is described using the standard protocol buffer
&#34;union&#34; trick (see above).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.executor..mesos.v1.ExecutorID) | required | Identifies the executor which generated this call. |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.executor..mesos.v1.FrameworkID) | required |  |
| type | [Call.Type](#mesos.v1.executor.Call.Type) | optional | Type of the call, indicates which optional field below should be present if that type has a nested message definition. In case type is SUBSCRIBED, no message needs to be set. See comments on `Event::Type` above on the reasoning behind this field being optional. |
| subscribe | [Call.Subscribe](#mesos.v1.executor.Call.Subscribe) | optional |  |
| update | [Call.Update](#mesos.v1.executor.Call.Update) | optional |  |
| message | [Call.Message](#mesos.v1.executor.Call.Message) | optional |  |






<a name="mesos.v1.executor.Call.Message"/>

### Call.Message
Sends arbitrary binary data to the scheduler. Note that Mesos
neither interprets this data nor makes any guarantees about the
delivery of this message to the scheduler.
See &#39;Message&#39; in the &#39;Events&#39; section.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.executor.Call.Subscribe"/>

### Call.Subscribe
Request to subscribe with the agent. If subscribing after a disconnection,
it must include a list of all the tasks and updates which haven&#39;t been
acknowledged by the scheduler.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| unacknowledged_tasks | [.mesos.v1.TaskInfo](#mesos.v1.executor..mesos.v1.TaskInfo) | repeated |  |
| unacknowledged_updates | [Call.Update](#mesos.v1.executor.Call.Update) | repeated |  |






<a name="mesos.v1.executor.Call.Update"/>

### Call.Update
Notifies the scheduler that a task has transitioned from one
state to another. Status updates should be used by executors
to reliably communicate the status of the tasks that they
manage. It is crucial that a terminal update (see TaskState
in v1/mesos.proto) is sent to the scheduler as soon as the task
terminates, in order for Mesos to release the resources allocated
to the task. It is the responsibility of the scheduler to
explicitly acknowledge the receipt of a status update. See
&#39;Acknowledged&#39; in the &#39;Events&#39; section above for the semantics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [.mesos.v1.TaskStatus](#mesos.v1.executor..mesos.v1.TaskStatus) | required |  |






<a name="mesos.v1.executor.Event"/>

### Event
Executor event API.

An event is described using the standard protocol buffer &#34;union&#34;
trick, see https://developers.google.com/protocol-buffers/docs/techniques#union.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#mesos.v1.executor.Event.Type) | optional | Type of the event, indicates which optional field below should be present if that type has a nested message definition. Enum fields should be optional, see: MESOS-4997. |
| subscribed | [Event.Subscribed](#mesos.v1.executor.Event.Subscribed) | optional |  |
| acknowledged | [Event.Acknowledged](#mesos.v1.executor.Event.Acknowledged) | optional |  |
| launch | [Event.Launch](#mesos.v1.executor.Event.Launch) | optional |  |
| launch_group | [Event.LaunchGroup](#mesos.v1.executor.Event.LaunchGroup) | optional |  |
| kill | [Event.Kill](#mesos.v1.executor.Event.Kill) | optional |  |
| message | [Event.Message](#mesos.v1.executor.Event.Message) | optional |  |
| error | [Event.Error](#mesos.v1.executor.Event.Error) | optional |  |






<a name="mesos.v1.executor.Event.Acknowledged"/>

### Event.Acknowledged
Received when the agent acknowledges the receipt of status
update. Schedulers are responsible for explicitly acknowledging
the receipt of status updates that have &#39;update.status().uuid()&#39;
field set. Unacknowledged updates can be retried by the executor.
They should also be sent by the executor whenever it
re-subscribes.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_id | [.mesos.v1.TaskID](#mesos.v1.executor..mesos.v1.TaskID) | required |  |
| uuid | [bytes](#bytes) | required |  |






<a name="mesos.v1.executor.Event.Error"/>

### Event.Error
Received in case the executor sends invalid calls (e.g.,
required values not set).
TODO(arojas): Remove this once the old executor driver is no
longer supported. With HTTP API all errors will be signaled via
HTTP response codes.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) | required |  |






<a name="mesos.v1.executor.Event.Kill"/>

### Event.Kill
Received when the scheduler wants to kill a specific task. Once
the task is terminated, the executor should send a TASK_KILLED
(or TASK_FAILED) update. The terminal update is necessary so
Mesos can release the resources associated with the task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_id | [.mesos.v1.TaskID](#mesos.v1.executor..mesos.v1.TaskID) | required |  |
| kill_policy | [.mesos.v1.KillPolicy](#mesos.v1.executor..mesos.v1.KillPolicy) | optional | If set, overrides any previously specified kill policy for this task. This includes &#39;TaskInfo.kill_policy&#39; and &#39;Executor.kill.kill_policy&#39;. Can be used to forcefully kill a task which is already being killed. |






<a name="mesos.v1.executor.Event.Launch"/>

### Event.Launch
Received when the framework attempts to launch a task. Once
the task is successfully launched, the executor must respond with
a TASK_RUNNING update (See TaskState in v1/mesos.proto).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [.mesos.v1.TaskInfo](#mesos.v1.executor..mesos.v1.TaskInfo) | required |  |






<a name="mesos.v1.executor.Event.LaunchGroup"/>

### Event.LaunchGroup
Received when the framework attempts to launch a group of tasks atomically.
Similar to `Launch` above the executor must send TASK_RUNNING updates for
tasks that are successfully launched.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_group | [.mesos.v1.TaskGroupInfo](#mesos.v1.executor..mesos.v1.TaskGroupInfo) | required |  |






<a name="mesos.v1.executor.Event.Message"/>

### Event.Message
Received when a custom message generated by the scheduler is
forwarded by the agent. Note that this message is not
interpreted by Mesos and is only forwarded (without reliability
guarantees) to the executor. It is up to the scheduler to retry
if the message is dropped for any reason.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.executor.Event.Subscribed"/>

### Event.Subscribed
First event received when the executor subscribes.
The &#39;id&#39; field in the &#39;framework_info&#39; will be set.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_info | [.mesos.v1.ExecutorInfo](#mesos.v1.executor..mesos.v1.ExecutorInfo) | required |  |
| framework_info | [.mesos.v1.FrameworkInfo](#mesos.v1.executor..mesos.v1.FrameworkInfo) | required |  |
| agent_info | [.mesos.v1.AgentInfo](#mesos.v1.executor..mesos.v1.AgentInfo) | required |  |
| container_id | [.mesos.v1.ContainerID](#mesos.v1.executor..mesos.v1.ContainerID) | optional | Uniquely identifies the container of an executor run. |





 


<a name="mesos.v1.executor.Call.Type"/>

### Call.Type
Possible call types, followed by message definitions if
applicable.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | See comments above on `Event::Type` for more details on this enum value. |
| SUBSCRIBE | 1 | See &#39;Subscribe&#39; below. |
| UPDATE | 2 | See &#39;Update&#39; below. |
| MESSAGE | 3 | See &#39;Message&#39; below. |



<a name="mesos.v1.executor.Event.Type"/>

### Event.Type
Possible event types, followed by message definitions if
applicable.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This must be the first enum value in this list, to ensure that if &#39;type&#39; is not set, the default value is UNKNOWN. This enables enum values to be added in a backwards-compatible way. See: MESOS-4997. |
| SUBSCRIBED | 1 | See &#39;Subscribed&#39; below. |
| LAUNCH | 2 | See &#39;Launch&#39; below. |
| LAUNCH_GROUP | 8 | See &#39;LaunchGroup&#39; below. |
| KILL | 3 | See &#39;Kill&#39; below. |
| ACKNOWLEDGED | 4 | See &#39;Acknowledged&#39; below. |
| MESSAGE | 5 | See &#39;Message&#39; below. |
| ERROR | 6 | See &#39;Error&#39; below. |
| SHUTDOWN | 7 | Received when the agent asks the executor to shutdown/kill itself. The executor is then required to kill all its active tasks, send `TASK_KILLED` status updates and gracefully exit. The executor should terminate within a `MESOS_EXECUTOR_SHUTDOWN_GRACE_PERIOD` (an environment variable set by the agent upon executor startup); it can be configured via `ExecutorInfo.shutdown_grace_period`. If the executor fails to do so, the agent will forcefully destroy the container where the executor is running. The agent would then send `TASK_LOST` updates for any remaining active tasks of this executor.

NOTE: The executor must not assume that it will always be allotted the full grace period, as the agent may decide to allot a shorter period and failures / forcible terminations may occur.

TODO(alexr): Consider adding a duration field into the `Shutdown` message so that the agent can communicate when a shorter period has been allotted. |


 

 

 



<a name="maintenance.proto"/>
<p align="right"><a href="#top">Top</a></p>

## maintenance.proto



<a name="mesos.v1.maintenance.ClusterStatus"/>

### ClusterStatus
Represents the maintenance status of each machine in the cluster.
The lists correspond to the `MachineInfo.Mode` enumeration.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| draining_machines | [ClusterStatus.DrainingMachine](#mesos.v1.maintenance.ClusterStatus.DrainingMachine) | repeated |  |
| down_machines | [.mesos.v1.MachineID](#mesos.v1.maintenance..mesos.v1.MachineID) | repeated |  |






<a name="mesos.v1.maintenance.ClusterStatus.DrainingMachine"/>

### ClusterStatus.DrainingMachine



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.mesos.v1.MachineID](#mesos.v1.maintenance..mesos.v1.MachineID) | required |  |
| statuses | [.mesos.v1.allocator.InverseOfferStatus](#mesos.v1.maintenance..mesos.v1.allocator.InverseOfferStatus) | repeated | A list of the most recent responses to inverse offers from frameworks running on this draining machine. |






<a name="mesos.v1.maintenance.Schedule"/>

### Schedule
A list of maintenance windows.
For example, this may represent a rolling restart of agents.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| windows | [Window](#mesos.v1.maintenance.Window) | repeated |  |






<a name="mesos.v1.maintenance.Window"/>

### Window
A set of machines scheduled to go into maintenance
in the same `unavailability`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| machine_ids | [.mesos.v1.MachineID](#mesos.v1.maintenance..mesos.v1.MachineID) | repeated | Machines affected by this maintenance window. |
| unavailability | [.mesos.v1.Unavailability](#mesos.v1.maintenance..mesos.v1.Unavailability) | required | Interval during which this set of machines is expected to be down. |





 

 

 

 



<a name="quota.proto"/>
<p align="right"><a href="#top">Top</a></p>

## quota.proto



<a name="mesos.v1.quota.QuotaInfo"/>

### QuotaInfo
TODO(joerg84): Add limits, i.e. upper bounds of resources that a
role is allowed to use.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) | optional | Quota is granted per role and not per framework, similar to dynamic reservations. |
| principal | [string](#string) | optional | Principal which set the quota. Currently only operators can set quotas. |
| guarantee | [.mesos.v1.Resource](#mesos.v1.quota..mesos.v1.Resource) | repeated | The guarantee that these resources are allocatable for the above role. NOTE: `guarantee.role` should not specify any role except &#39;*&#39;, because quota does not reserve specific resources. |






<a name="mesos.v1.quota.QuotaRequest"/>

### QuotaRequest
`QuotaRequest` provides a schema for set quota JSON requests.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| force | [bool](#bool) | optional | Disables the capacity heuristic check if set to `true`. |
| role | [string](#string) | optional | The role for which to set quota. |
| guarantee | [.mesos.v1.Resource](#mesos.v1.quota..mesos.v1.Resource) | repeated | The requested guarantee that these resources will be allocatable for the above role. |






<a name="mesos.v1.quota.QuotaStatus"/>

### QuotaStatus
`QuotaStatus` describes the internal representation for the /quota/status
response.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| infos | [QuotaInfo](#mesos.v1.quota.QuotaInfo) | repeated | Quotas which are currently set, i.e. known to the master. |





 

 

 

 



<a name="master.proto"/>
<p align="right"><a href="#top">Top</a></p>

## master.proto



<a name="mesos.v1.master.Call"/>

### Call
Calls that can be sent to the v1 master API.

A call is described using the standard protocol buffer &#34;union&#34;
trick, see
https://developers.google.com/protocol-buffers/docs/techniques#union.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Call.Type](#mesos.v1.master.Call.Type) | optional |  |
| get_metrics | [Call.GetMetrics](#mesos.v1.master.Call.GetMetrics) | optional |  |
| set_logging_level | [Call.SetLoggingLevel](#mesos.v1.master.Call.SetLoggingLevel) | optional |  |
| list_files | [Call.ListFiles](#mesos.v1.master.Call.ListFiles) | optional |  |
| read_file | [Call.ReadFile](#mesos.v1.master.Call.ReadFile) | optional |  |
| update_weights | [Call.UpdateWeights](#mesos.v1.master.Call.UpdateWeights) | optional |  |
| reserve_resources | [Call.ReserveResources](#mesos.v1.master.Call.ReserveResources) | optional |  |
| unreserve_resources | [Call.UnreserveResources](#mesos.v1.master.Call.UnreserveResources) | optional |  |
| create_volumes | [Call.CreateVolumes](#mesos.v1.master.Call.CreateVolumes) | optional |  |
| destroy_volumes | [Call.DestroyVolumes](#mesos.v1.master.Call.DestroyVolumes) | optional |  |
| update_maintenance_schedule | [Call.UpdateMaintenanceSchedule](#mesos.v1.master.Call.UpdateMaintenanceSchedule) | optional |  |
| start_maintenance | [Call.StartMaintenance](#mesos.v1.master.Call.StartMaintenance) | optional |  |
| stop_maintenance | [Call.StopMaintenance](#mesos.v1.master.Call.StopMaintenance) | optional |  |
| set_quota | [Call.SetQuota](#mesos.v1.master.Call.SetQuota) | optional |  |
| remove_quota | [Call.RemoveQuota](#mesos.v1.master.Call.RemoveQuota) | optional |  |






<a name="mesos.v1.master.Call.CreateVolumes"/>

### Call.CreateVolumes
Create persistent volumes on reserved resources. The request is forwarded
asynchronously to the Mesos agent where the reserved resources are located.
That asynchronous message may not be delivered or creating the volumes at
the agent might fail. Volume creation can be verified by sending a
`GET_VOLUMES` call.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |
| volumes | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.master.Call.DestroyVolumes"/>

### Call.DestroyVolumes
Destroy persistent volumes. The request is forwarded asynchronously to the
Mesos agent where the reserved resources are located. That asynchronous
message may not be delivered or destroying the volumes at the agent might
fail. Volume deletion can be verified by sending a `GET_VOLUMES` call.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |
| volumes | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.master.Call.GetMetrics"/>

### Call.GetMetrics
Provides a snapshot of the current metrics tracked by the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timeout | [.mesos.v1.DurationInfo](#mesos.v1.master..mesos.v1.DurationInfo) | optional | If set, `timeout` would be used to determines the maximum amount of time the API will take to respond. If the timeout is exceeded, some metrics may not be included in the response. |






<a name="mesos.v1.master.Call.ListFiles"/>

### Call.ListFiles
Provides the file listing for a directory.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | required |  |






<a name="mesos.v1.master.Call.ReadFile"/>

### Call.ReadFile
Reads data from a file.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [string](#string) | required | The path of file. |
| offset | [uint64](#uint64) | required | Initial offset in file to start reading from. |
| length | [uint64](#uint64) | optional | The maximum number of bytes to read. The read length is capped at 16 memory pages. |






<a name="mesos.v1.master.Call.RemoveQuota"/>

### Call.RemoveQuota



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) | required |  |






<a name="mesos.v1.master.Call.ReserveResources"/>

### Call.ReserveResources
Reserve resources dynamically on a specific agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |
| resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.master.Call.SetLoggingLevel"/>

### Call.SetLoggingLevel
Sets the logging verbosity level for a specified duration. Mesos uses
[glog](https://github.com/google/glog) for logging. The library only uses
verbose logging which means nothing will be output unless the verbosity
level is set (by default it&#39;s 0, libprocess uses levels 1, 2, and 3).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | [uint32](#uint32) | required | The verbosity level. |
| duration | [.mesos.v1.DurationInfo](#mesos.v1.master..mesos.v1.DurationInfo) | required | The duration to keep verbosity level toggled. After this duration, the verbosity level of log would revert to the original level. |






<a name="mesos.v1.master.Call.SetQuota"/>

### Call.SetQuota
Sets the quota for resources to be used by a particular role.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| quota_request | [.mesos.v1.quota.QuotaRequest](#mesos.v1.master..mesos.v1.quota.QuotaRequest) | required |  |






<a name="mesos.v1.master.Call.StartMaintenance"/>

### Call.StartMaintenance
Starts the maintenance of the cluster, this would bring a set of machines
down.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| machines | [.mesos.v1.MachineID](#mesos.v1.master..mesos.v1.MachineID) | repeated |  |






<a name="mesos.v1.master.Call.StopMaintenance"/>

### Call.StopMaintenance
Stops the maintenance of the cluster, this would bring a set of machines
back up.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| machines | [.mesos.v1.MachineID](#mesos.v1.master..mesos.v1.MachineID) | repeated |  |






<a name="mesos.v1.master.Call.UnreserveResources"/>

### Call.UnreserveResources
Unreserve resources dynamically on a specific agent.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |
| resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.master.Call.UpdateMaintenanceSchedule"/>

### Call.UpdateMaintenanceSchedule
Updates the cluster&#39;s maintenance schedule.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| schedule | [.mesos.v1.maintenance.Schedule](#mesos.v1.master..mesos.v1.maintenance.Schedule) | required |  |






<a name="mesos.v1.master.Call.UpdateWeights"/>

### Call.UpdateWeights



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| weight_infos | [.mesos.v1.WeightInfo](#mesos.v1.master..mesos.v1.WeightInfo) | repeated |  |






<a name="mesos.v1.master.Event"/>

### Event
Streaming response to `Call::SUBSCRIBE` made to the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#mesos.v1.master.Event.Type) | optional |  |
| subscribed | [Event.Subscribed](#mesos.v1.master.Event.Subscribed) | optional |  |
| task_added | [Event.TaskAdded](#mesos.v1.master.Event.TaskAdded) | optional |  |
| task_updated | [Event.TaskUpdated](#mesos.v1.master.Event.TaskUpdated) | optional |  |
| agent_added | [Event.AgentAdded](#mesos.v1.master.Event.AgentAdded) | optional |  |
| agent_removed | [Event.AgentRemoved](#mesos.v1.master.Event.AgentRemoved) | optional |  |






<a name="mesos.v1.master.Event.AgentAdded"/>

### Event.AgentAdded
Forwarded by the master when an agent becomes known to it.
This can happen when an agent registered for the first
time, or reregistered after a master failover.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent | [Response.GetAgents.Agent](#mesos.v1.master.Response.GetAgents.Agent) | required |  |






<a name="mesos.v1.master.Event.AgentRemoved"/>

### Event.AgentRemoved
Forwarded by the master when an agent is removed.
This can happen when an agent does not re-register
within `--agent_reregister_timeout` upon a master failover,
or when the agent is scheduled for maintenance.

NOTE: It&#39;s possible that an agent might become
active once it has been removed, i.e. if the master
has gc&#39;ed its list of known &#34;dead&#34; agents.
See MESOS-5965 for context.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |






<a name="mesos.v1.master.Event.Subscribed"/>

### Event.Subscribed
First event received when a client subscribes.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| get_state | [Response.GetState](#mesos.v1.master.Response.GetState) | optional | Snapshot of the entire cluster state. Further updates to the cluster state are sent as separate events on the stream. |






<a name="mesos.v1.master.Event.TaskAdded"/>

### Event.TaskAdded
Forwarded by the master when a task becomes known to it. This can happen
when a new task is launched by the scheduler or when the task becomes
known to the master upon an agent (re-)registration after a failover.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | required |  |






<a name="mesos.v1.master.Event.TaskUpdated"/>

### Event.TaskUpdated
Forwarded by the master when an existing task transitions to a new state.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.master..mesos.v1.FrameworkID) | required |  |
| status | [.mesos.v1.TaskStatus](#mesos.v1.master..mesos.v1.TaskStatus) | required | This is the status of the task corresponding to the last status update acknowledged by the scheduler. |
| state | [.mesos.v1.TaskState](#mesos.v1.master..mesos.v1.TaskState) | required | This is the latest state of the task according to the agent. |






<a name="mesos.v1.master.Response"/>

### Response
Synchronous responses for all calls (except Call::SUBSCRIBE) made to
the v1 master API.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Response.Type](#mesos.v1.master.Response.Type) | optional |  |
| get_health | [Response.GetHealth](#mesos.v1.master.Response.GetHealth) | optional |  |
| get_flags | [Response.GetFlags](#mesos.v1.master.Response.GetFlags) | optional |  |
| get_version | [Response.GetVersion](#mesos.v1.master.Response.GetVersion) | optional |  |
| get_metrics | [Response.GetMetrics](#mesos.v1.master.Response.GetMetrics) | optional |  |
| get_logging_level | [Response.GetLoggingLevel](#mesos.v1.master.Response.GetLoggingLevel) | optional |  |
| list_files | [Response.ListFiles](#mesos.v1.master.Response.ListFiles) | optional |  |
| read_file | [Response.ReadFile](#mesos.v1.master.Response.ReadFile) | optional |  |
| get_state | [Response.GetState](#mesos.v1.master.Response.GetState) | optional |  |
| get_agents | [Response.GetAgents](#mesos.v1.master.Response.GetAgents) | optional |  |
| get_frameworks | [Response.GetFrameworks](#mesos.v1.master.Response.GetFrameworks) | optional |  |
| get_executors | [Response.GetExecutors](#mesos.v1.master.Response.GetExecutors) | optional |  |
| get_tasks | [Response.GetTasks](#mesos.v1.master.Response.GetTasks) | optional |  |
| get_roles | [Response.GetRoles](#mesos.v1.master.Response.GetRoles) | optional |  |
| get_weights | [Response.GetWeights](#mesos.v1.master.Response.GetWeights) | optional |  |
| get_master | [Response.GetMaster](#mesos.v1.master.Response.GetMaster) | optional |  |
| get_maintenance_status | [Response.GetMaintenanceStatus](#mesos.v1.master.Response.GetMaintenanceStatus) | optional |  |
| get_maintenance_schedule | [Response.GetMaintenanceSchedule](#mesos.v1.master.Response.GetMaintenanceSchedule) | optional |  |
| get_quota | [Response.GetQuota](#mesos.v1.master.Response.GetQuota) | optional |  |






<a name="mesos.v1.master.Response.GetAgents"/>

### Response.GetAgents



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agents | [Response.GetAgents.Agent](#mesos.v1.master.Response.GetAgents.Agent) | repeated | Registered agents. |
| recovered_agents | [.mesos.v1.AgentInfo](#mesos.v1.master..mesos.v1.AgentInfo) | repeated | Agents which are recovered from registry but not reregistered yet. |






<a name="mesos.v1.master.Response.GetAgents.Agent"/>

### Response.GetAgents.Agent



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_info | [.mesos.v1.AgentInfo](#mesos.v1.master..mesos.v1.AgentInfo) | required |  |
| active | [bool](#bool) | required |  |
| version | [string](#string) | required |  |
| pid | [string](#string) | optional |  |
| registered_time | [.mesos.v1.TimeInfo](#mesos.v1.master..mesos.v1.TimeInfo) | optional |  |
| reregistered_time | [.mesos.v1.TimeInfo](#mesos.v1.master..mesos.v1.TimeInfo) | optional |  |
| total_resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated | Total resources (including oversubscribed resources) the agent provides. |
| allocated_resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |
| offered_resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |
| capabilities | [.mesos.v1.AgentInfo.Capability](#mesos.v1.master..mesos.v1.AgentInfo.Capability) | repeated |  |






<a name="mesos.v1.master.Response.GetExecutors"/>

### Response.GetExecutors
Lists information about all the executors known to the master at the
current time. Note that there might be executors unknown to the master
running on partitioned or unsubscribed agents.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executors | [Response.GetExecutors.Executor](#mesos.v1.master.Response.GetExecutors.Executor) | repeated |  |
| orphan_executors | [Response.GetExecutors.Executor](#mesos.v1.master.Response.GetExecutors.Executor) | repeated | As of Mesos 1.2, this field will always be empty.

TODO(neilc): Remove this field after a deprecation cycle starting in Mesos 1.2. |






<a name="mesos.v1.master.Response.GetExecutors.Executor"/>

### Response.GetExecutors.Executor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_info | [.mesos.v1.ExecutorInfo](#mesos.v1.master..mesos.v1.ExecutorInfo) | required |  |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.master..mesos.v1.AgentID) | required |  |






<a name="mesos.v1.master.Response.GetFlags"/>

### Response.GetFlags
Contains the flag configuration of the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| flags | [.mesos.v1.Flag](#mesos.v1.master..mesos.v1.Flag) | repeated |  |






<a name="mesos.v1.master.Response.GetFrameworks"/>

### Response.GetFrameworks
Information about all the frameworks known to the master at the current
time. Note that there might be frameworks unknown to the master running
on partitioned or unsubscribed agents.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| frameworks | [Response.GetFrameworks.Framework](#mesos.v1.master.Response.GetFrameworks.Framework) | repeated | Frameworks that have subscribed with the master. Note that this includes frameworks that are disconnected and in the process of re-subscribing. |
| completed_frameworks | [Response.GetFrameworks.Framework](#mesos.v1.master.Response.GetFrameworks.Framework) | repeated | Frameworks that have been teared down. |
| recovered_frameworks | [.mesos.v1.FrameworkInfo](#mesos.v1.master..mesos.v1.FrameworkInfo) | repeated | This field previously contained frameworks that previously subscribed but haven&#39;t yet re-subscribed after a master failover. As of Mesos 1.2, this field will always be empty; recovered frameworks are now reported in the `frameworks` list with the `recovered` field set to true.

TODO(neilc): Remove this field after a deprecation cycle starting in Mesos 1.2. |






<a name="mesos.v1.master.Response.GetFrameworks.Framework"/>

### Response.GetFrameworks.Framework



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_info | [.mesos.v1.FrameworkInfo](#mesos.v1.master..mesos.v1.FrameworkInfo) | required |  |
| active | [bool](#bool) | required |  |
| connected | [bool](#bool) | required |  |
| recovered | [bool](#bool) | required | If true, this framework was previously subscribed but hasn&#39;t yet re-subscribed after a master failover. Recovered frameworks are only reported if one or more agents running a task or executor for the framework have re-registered after master failover. |
| registered_time | [.mesos.v1.TimeInfo](#mesos.v1.master..mesos.v1.TimeInfo) | optional |  |
| reregistered_time | [.mesos.v1.TimeInfo](#mesos.v1.master..mesos.v1.TimeInfo) | optional |  |
| unregistered_time | [.mesos.v1.TimeInfo](#mesos.v1.master..mesos.v1.TimeInfo) | optional |  |
| offers | [.mesos.v1.Offer](#mesos.v1.master..mesos.v1.Offer) | repeated |  |
| inverse_offers | [.mesos.v1.InverseOffer](#mesos.v1.master..mesos.v1.InverseOffer) | repeated |  |
| allocated_resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |
| offered_resources | [.mesos.v1.Resource](#mesos.v1.master..mesos.v1.Resource) | repeated |  |






<a name="mesos.v1.master.Response.GetHealth"/>

### Response.GetHealth
`healthy` would be true if the master is healthy. Delayed responses are
also indicative of the poor health of the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| healthy | [bool](#bool) | required |  |






<a name="mesos.v1.master.Response.GetLoggingLevel"/>

### Response.GetLoggingLevel
Contains the logging level of the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| level | [uint32](#uint32) | required |  |






<a name="mesos.v1.master.Response.GetMaintenanceSchedule"/>

### Response.GetMaintenanceSchedule
Contains the cluster&#39;s maintenance schedule.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| schedule | [.mesos.v1.maintenance.Schedule](#mesos.v1.master..mesos.v1.maintenance.Schedule) | required |  |






<a name="mesos.v1.master.Response.GetMaintenanceStatus"/>

### Response.GetMaintenanceStatus
Contains the cluster&#39;s maintenance status.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [.mesos.v1.maintenance.ClusterStatus](#mesos.v1.master..mesos.v1.maintenance.ClusterStatus) | required |  |






<a name="mesos.v1.master.Response.GetMaster"/>

### Response.GetMaster
Contains the master&#39;s information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| master_info | [.mesos.v1.MasterInfo](#mesos.v1.master..mesos.v1.MasterInfo) | optional |  |






<a name="mesos.v1.master.Response.GetMetrics"/>

### Response.GetMetrics
Contains a snapshot of the current metrics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| metrics | [.mesos.v1.Metric](#mesos.v1.master..mesos.v1.Metric) | repeated |  |






<a name="mesos.v1.master.Response.GetQuota"/>

### Response.GetQuota
Contains the cluster&#39;s configured quotas.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [.mesos.v1.quota.QuotaStatus](#mesos.v1.master..mesos.v1.quota.QuotaStatus) | required |  |






<a name="mesos.v1.master.Response.GetRoles"/>

### Response.GetRoles
Provides information about every role that is on the role whitelist (if
enabled), has one or more registered frameworks or has a non-default weight
or quota.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| roles | [.mesos.v1.Role](#mesos.v1.master..mesos.v1.Role) | repeated |  |






<a name="mesos.v1.master.Response.GetState"/>

### Response.GetState
Contains full state of the master i.e. information about the tasks,
agents, frameworks and executors running in the cluster.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| get_tasks | [Response.GetTasks](#mesos.v1.master.Response.GetTasks) | optional |  |
| get_executors | [Response.GetExecutors](#mesos.v1.master.Response.GetExecutors) | optional |  |
| get_frameworks | [Response.GetFrameworks](#mesos.v1.master.Response.GetFrameworks) | optional |  |
| get_agents | [Response.GetAgents](#mesos.v1.master.Response.GetAgents) | optional |  |






<a name="mesos.v1.master.Response.GetTasks"/>

### Response.GetTasks
Lists information about all the tasks known to the master at the current
time. Note that there might be tasks unknown to the master running on
partitioned or unsubscribed agents.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pending_tasks | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | repeated | Tasks that are enqueued on the master waiting (e.g., authorizing) to be launched. |
| tasks | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | repeated | Tasks that have been forwarded to the agent for launch. This includes tasks that are staging or running; it also includes tasks that have reached a terminal state but the terminal status update has not yet been acknowledged by the scheduler. |
| unreachable_tasks | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | repeated | Tasks that were running on agents that have become partitioned from the master. If/when the agent is no longer partitioned, tasks running on that agent will no longer be unreachable (they will either be running or completed). Note that the master only stores a limited number of unreachable tasks; information about unreachable tasks is also not preserved across master failover. |
| completed_tasks | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | repeated | Tasks that have reached terminal state and have all their updates acknowledged by the scheduler. |
| orphan_tasks | [.mesos.v1.Task](#mesos.v1.master..mesos.v1.Task) | repeated | As of Mesos 1.2, this field will always be empty.

TODO(neilc): Remove this field after a deprecation cycle starting in Mesos 1.2. |






<a name="mesos.v1.master.Response.GetVersion"/>

### Response.GetVersion
Contains the version information of the master.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version_info | [.mesos.v1.VersionInfo](#mesos.v1.master..mesos.v1.VersionInfo) | required |  |






<a name="mesos.v1.master.Response.GetWeights"/>

### Response.GetWeights
Provides the weight information about every role.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| weight_infos | [.mesos.v1.WeightInfo](#mesos.v1.master..mesos.v1.WeightInfo) | repeated |  |






<a name="mesos.v1.master.Response.ListFiles"/>

### Response.ListFiles
Contains the file listing(similar to `ls -l`) for a directory.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| file_infos | [.mesos.v1.FileInfo](#mesos.v1.master..mesos.v1.FileInfo) | repeated |  |






<a name="mesos.v1.master.Response.ReadFile"/>

### Response.ReadFile
Contains the file data.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [uint64](#uint64) | required | The size of file (in bytes). |
| data | [bytes](#bytes) | required |  |





 


<a name="mesos.v1.master.Call.Type"/>

### Call.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | If a call of type `Call::FOO` requires additional parameters they can be included in the corresponding `Call::Foo` message. Similarly, if a call receives a synchronous response it will be returned as a `Response` message of type `Response::FOO`. Currently all calls except `Call::SUBSCRIBE` receive synchronous responses; `Call::SUBSCRIBE` returns a streaming response of `Event`. |
| GET_HEALTH | 1 | Retrieves the master&#39;s health status. |
| GET_FLAGS | 2 | Retrieves the master&#39;s flag configuration. |
| GET_VERSION | 3 | Retrieves the master&#39;s version information. |
| GET_METRICS | 4 | See &#39;GetMetrics&#39; below. |
| GET_LOGGING_LEVEL | 5 | Retrieves the master&#39;s logging level. |
| SET_LOGGING_LEVEL | 6 | See &#39;SetLoggingLevel&#39; below. |
| LIST_FILES | 7 |  |
| READ_FILE | 8 | See &#39;ReadFile&#39; below. |
| GET_STATE | 9 |  |
| GET_AGENTS | 10 |  |
| GET_FRAMEWORKS | 11 |  |
| GET_EXECUTORS | 12 | Retrieves the information about all executors. |
| GET_TASKS | 13 | Retrieves the information about all known tasks. |
| GET_ROLES | 14 | Retrieves the information about roles. |
| GET_WEIGHTS | 15 | Retrieves the information about role weights. |
| UPDATE_WEIGHTS | 16 |  |
| GET_MASTER | 17 | Retrieves the master&#39;s information. |
| SUBSCRIBE | 18 | Subscribes the master to receive events. |
| RESERVE_RESOURCES | 19 |  |
| UNRESERVE_RESOURCES | 20 |  |
| CREATE_VOLUMES | 21 | See &#39;CreateVolumes&#39; below. |
| DESTROY_VOLUMES | 22 | See &#39;DestroyVolumes&#39; below. |
| GET_MAINTENANCE_STATUS | 23 | Retrieves the cluster&#39;s maintenance status. |
| GET_MAINTENANCE_SCHEDULE | 24 | Retrieves the cluster&#39;s maintenance schedule. |
| UPDATE_MAINTENANCE_SCHEDULE | 25 | See &#39;UpdateMaintenanceSchedule&#39; below. |
| START_MAINTENANCE | 26 | See &#39;StartMaintenance&#39; below. |
| STOP_MAINTENANCE | 27 | See &#39;StopMaintenance&#39; below. |
| GET_QUOTA | 28 |  |
| SET_QUOTA | 29 | See &#39;SetQuota&#39; below. |
| REMOVE_QUOTA | 30 | See &#39;RemoveQuota&#39; below. |



<a name="mesos.v1.master.Event.Type"/>

### Event.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| SUBSCRIBED | 1 | See `Subscribed` below. |
| TASK_ADDED | 2 | See `TaskAdded` below. |
| TASK_UPDATED | 3 | See `TaskUpdated` below. |
| AGENT_ADDED | 4 | See `AgentAdded` below. |
| AGENT_REMOVED | 5 | See `AgentRemoved` below. |



<a name="mesos.v1.master.Response.Type"/>

### Response.Type
Each of the responses of type `FOO` corresponds to `Foo` message below.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| GET_HEALTH | 1 | See &#39;GetHealth&#39; below. |
| GET_FLAGS | 2 | See &#39;GetFlags&#39; below. |
| GET_VERSION | 3 | See &#39;GetVersion&#39; below. |
| GET_METRICS | 4 | See &#39;GetMetrics&#39; below. |
| GET_LOGGING_LEVEL | 5 | See &#39;GetLoggingLevel&#39; below. |
| LIST_FILES | 6 |  |
| READ_FILE | 7 | See &#39;ReadFile&#39; below. |
| GET_STATE | 8 |  |
| GET_AGENTS | 9 |  |
| GET_FRAMEWORKS | 10 |  |
| GET_EXECUTORS | 11 | See &#39;GetExecutors&#39; below. |
| GET_TASKS | 12 | See &#39;GetTasks&#39; below. |
| GET_ROLES | 13 | See &#39;GetRoles&#39; below. |
| GET_WEIGHTS | 14 | See &#39;GetWeights&#39; below. |
| GET_MASTER | 15 | See &#39;GetMaster&#39; below. |
| GET_MAINTENANCE_STATUS | 16 | See &#39;GetMaintenanceStatus&#39; below. |
| GET_MAINTENANCE_SCHEDULE | 17 | See &#39;GetMaintenanceSchedule&#39; below. |
| GET_QUOTA | 18 |  |


 

 

 



<a name="scheduler.proto"/>
<p align="right"><a href="#top">Top</a></p>

## scheduler.proto



<a name="mesos.v1.scheduler.Call"/>

### Call
Scheduler call API.

Like Event, a Call is described using the standard protocol buffer
&#34;union&#34; trick (see above).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.scheduler..mesos.v1.FrameworkID) | optional | Identifies who generated this call. Master assigns a framework id when a new scheduler subscribes for the first time. Once assigned, the scheduler must set the &#39;framework_id&#39; here and within its FrameworkInfo (in any further &#39;Subscribe&#39; calls). This allows the master to identify a scheduler correctly across disconnections, failovers, etc. |
| type | [Call.Type](#mesos.v1.scheduler.Call.Type) | optional | Type of the call, indicates which optional field below should be present if that type has a nested message definition. See comments on `Event::Type` above on the reasoning behind this field being optional. |
| subscribe | [Call.Subscribe](#mesos.v1.scheduler.Call.Subscribe) | optional |  |
| accept | [Call.Accept](#mesos.v1.scheduler.Call.Accept) | optional |  |
| decline | [Call.Decline](#mesos.v1.scheduler.Call.Decline) | optional |  |
| accept_inverse_offers | [Call.AcceptInverseOffers](#mesos.v1.scheduler.Call.AcceptInverseOffers) | optional |  |
| decline_inverse_offers | [Call.DeclineInverseOffers](#mesos.v1.scheduler.Call.DeclineInverseOffers) | optional |  |
| revive | [Call.Revive](#mesos.v1.scheduler.Call.Revive) | optional |  |
| kill | [Call.Kill](#mesos.v1.scheduler.Call.Kill) | optional |  |
| shutdown | [Call.Shutdown](#mesos.v1.scheduler.Call.Shutdown) | optional |  |
| acknowledge | [Call.Acknowledge](#mesos.v1.scheduler.Call.Acknowledge) | optional |  |
| reconcile | [Call.Reconcile](#mesos.v1.scheduler.Call.Reconcile) | optional |  |
| message | [Call.Message](#mesos.v1.scheduler.Call.Message) | optional |  |
| request | [Call.Request](#mesos.v1.scheduler.Call.Request) | optional |  |
| suppress | [Call.Suppress](#mesos.v1.scheduler.Call.Suppress) | optional |  |






<a name="mesos.v1.scheduler.Call.Accept"/>

### Call.Accept
Accepts an offer, performing the specified operations
in a sequential manner.

E.g. Launch a task with a newly reserved persistent volume:

Accept {
offer_ids: [ ... ]
operations: [
{ type: RESERVE,
reserve: { resources: [ disk(role):2 ] } }
{ type: CREATE,
create: { volumes: [ disk(role):1&#43;persistence ] } }
{ type: LAUNCH,
launch: { task_infos ... disk(role):1;disk(role):1&#43;persistence } }
]
}

Note that any of the offer’s resources not used in the &#39;Accept&#39;
call (e.g., to launch a task) are considered unused and might be
reoffered to other frameworks. In other words, the same OfferID
cannot be used in more than one &#39;Accept&#39; call.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offer_ids | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | repeated |  |
| operations | [.mesos.v1.Offer.Operation](#mesos.v1.scheduler..mesos.v1.Offer.Operation) | repeated |  |
| filters | [.mesos.v1.Filters](#mesos.v1.scheduler..mesos.v1.Filters) | optional |  |






<a name="mesos.v1.scheduler.Call.AcceptInverseOffers"/>

### Call.AcceptInverseOffers
Accepts an inverse offer. Inverse offers should only be accepted
if the resources in the offer can be safely evacuated before the
provided unavailability.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inverse_offer_ids | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | repeated |  |
| filters | [.mesos.v1.Filters](#mesos.v1.scheduler..mesos.v1.Filters) | optional |  |






<a name="mesos.v1.scheduler.Call.Acknowledge"/>

### Call.Acknowledge
Acknowledges the receipt of status update. Schedulers are
responsible for explicitly acknowledging the receipt of status
updates that have &#39;Update.status().uuid()&#39; field set. Such status
updates are retried by the agent until they are acknowledged by
the scheduler.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | required |  |
| task_id | [.mesos.v1.TaskID](#mesos.v1.scheduler..mesos.v1.TaskID) | required |  |
| uuid | [bytes](#bytes) | required |  |






<a name="mesos.v1.scheduler.Call.Decline"/>

### Call.Decline
Declines an offer, signaling the master to potentially reoffer
the resources to a different framework. Note that this is same
as sending an Accept call with no operations. See comments on
top of &#39;Accept&#39; for semantics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offer_ids | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | repeated |  |
| filters | [.mesos.v1.Filters](#mesos.v1.scheduler..mesos.v1.Filters) | optional |  |






<a name="mesos.v1.scheduler.Call.DeclineInverseOffers"/>

### Call.DeclineInverseOffers
Declines an inverse offer. Inverse offers should be declined if
the resources in the offer might not be safely evacuated before
the provided unavailability.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inverse_offer_ids | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | repeated |  |
| filters | [.mesos.v1.Filters](#mesos.v1.scheduler..mesos.v1.Filters) | optional |  |






<a name="mesos.v1.scheduler.Call.Kill"/>

### Call.Kill
Kills a specific task. If the scheduler has a custom executor,
the kill is forwarded to the executor and it is up to the
executor to kill the task and send a TASK_KILLED (or TASK_FAILED)
update. Note that Mesos releases the resources for a task once it
receives a terminal update (See TaskState in v1/mesos.proto) for
it. If the task is unknown to the master, a TASK_LOST update is
generated.

If a task within a task group is killed before the group is
delivered to the executor, all tasks in the task group are
killed. When a task group has been delivered to the executor,
it is up to the executor to decide how to deal with the kill.
Note The default Mesos executor will currently kill all the
tasks in the task group if it gets a kill for any task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_id | [.mesos.v1.TaskID](#mesos.v1.scheduler..mesos.v1.TaskID) | required |  |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | optional |  |
| kill_policy | [.mesos.v1.KillPolicy](#mesos.v1.scheduler..mesos.v1.KillPolicy) | optional | If set, overrides any previously specified kill policy for this task. This includes &#39;TaskInfo.kill_policy&#39; and &#39;Executor.kill.kill_policy&#39;. Can be used to forcefully kill a task which is already being killed. |






<a name="mesos.v1.scheduler.Call.Message"/>

### Call.Message
Sends arbitrary binary data to the executor. Note that Mesos
neither interprets this data nor makes any guarantees about the
delivery of this message to the executor.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | required |  |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.scheduler..mesos.v1.ExecutorID) | required |  |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.scheduler.Call.Reconcile"/>

### Call.Reconcile
Allows the scheduler to query the status for non-terminal tasks.
This causes the master to send back the latest task status for
each task in &#39;tasks&#39;, if possible. Tasks that are no longer known
will result in a TASK_LOST, TASK_UNKNOWN, or TASK_UNREACHABLE update.
If &#39;tasks&#39; is empty, then the master will send the latest status
for each task currently known.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [Call.Reconcile.Task](#mesos.v1.scheduler.Call.Reconcile.Task) | repeated |  |






<a name="mesos.v1.scheduler.Call.Reconcile.Task"/>

### Call.Reconcile.Task
TODO(vinod): Support arbitrary queries than just state of tasks.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task_id | [.mesos.v1.TaskID](#mesos.v1.scheduler..mesos.v1.TaskID) | required |  |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | optional |  |






<a name="mesos.v1.scheduler.Call.Request"/>

### Call.Request
Requests a specific set of resources from Mesos&#39;s allocator. If
the allocator has support for this, corresponding offers will be
sent asynchronously via the OFFERS event(s).

NOTE: The built-in hierarchical allocator doesn&#39;t have support
for this call and hence simply ignores it.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| requests | [.mesos.v1.Request](#mesos.v1.scheduler..mesos.v1.Request) | repeated |  |






<a name="mesos.v1.scheduler.Call.Revive"/>

### Call.Revive
Revive offers for a specified role. If role is unset, the
`REVIVE` call will revive offers for all of the roles the
framework is subscribed to.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) | optional |  |






<a name="mesos.v1.scheduler.Call.Shutdown"/>

### Call.Shutdown
Shuts down a custom executor. When the executor gets a shutdown
event, it is expected to kill all its tasks (and send TASK_KILLED
updates) and terminate. If the executor doesn’t terminate within
a certain timeout (configurable via
&#39;--executor_shutdown_grace_period&#39; agent flag), the agent will
forcefully destroy the container (executor and its tasks) and
transition its active tasks to TASK_LOST.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.scheduler..mesos.v1.ExecutorID) | required |  |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | required |  |






<a name="mesos.v1.scheduler.Call.Subscribe"/>

### Call.Subscribe
Subscribes the scheduler with the master to receive events. A
scheduler must send other calls only after it has received the
SUBCRIBED event.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_info | [.mesos.v1.FrameworkInfo](#mesos.v1.scheduler..mesos.v1.FrameworkInfo) | required | See the comments below on &#39;framework_id&#39; on the semantics for &#39;framework_info.id&#39;. |






<a name="mesos.v1.scheduler.Call.Suppress"/>

### Call.Suppress
Suppress offers for a specified role. If role is unset, the
`SUPPRESS` call will suppress offers for all of the roles the
framework is subscribed to.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| role | [string](#string) | optional |  |






<a name="mesos.v1.scheduler.Event"/>

### Event
Scheduler event API.

An event is described using the standard protocol buffer &#34;union&#34;
trick, see:
https://developers.google.com/protocol-buffers/docs/techniques#union.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Event.Type](#mesos.v1.scheduler.Event.Type) | optional | Type of the event, indicates which optional field below should be present if that type has a nested message definition. Enum fields should be optional, see: MESOS-4997. |
| subscribed | [Event.Subscribed](#mesos.v1.scheduler.Event.Subscribed) | optional |  |
| offers | [Event.Offers](#mesos.v1.scheduler.Event.Offers) | optional |  |
| inverse_offers | [Event.InverseOffers](#mesos.v1.scheduler.Event.InverseOffers) | optional |  |
| rescind | [Event.Rescind](#mesos.v1.scheduler.Event.Rescind) | optional |  |
| rescind_inverse_offer | [Event.RescindInverseOffer](#mesos.v1.scheduler.Event.RescindInverseOffer) | optional |  |
| update | [Event.Update](#mesos.v1.scheduler.Event.Update) | optional |  |
| message | [Event.Message](#mesos.v1.scheduler.Event.Message) | optional |  |
| failure | [Event.Failure](#mesos.v1.scheduler.Event.Failure) | optional |  |
| error | [Event.Error](#mesos.v1.scheduler.Event.Error) | optional |  |






<a name="mesos.v1.scheduler.Event.Error"/>

### Event.Error
Received when there is an unrecoverable error in the scheduler (e.g.,
scheduler failed over, rate limiting, authorization errors etc.). The
scheduler should abort on receiving this event.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) | required |  |






<a name="mesos.v1.scheduler.Event.Failure"/>

### Event.Failure
Received when an agent is removed from the cluster (e.g., failed
health checks) or when an executor is terminated. Note that, this
event coincides with receipt of terminal UPDATE events for any
active tasks belonging to the agent or executor and receipt of
&#39;Rescind&#39; events for any outstanding offers belonging to the
agent. Note that there is no guaranteed order between the
&#39;Failure&#39;, &#39;Update&#39; and &#39;Rescind&#39; events when an agent or executor
is removed.
TODO(vinod): Consider splitting the lost agent and terminated
executor into separate events and ensure it&#39;s reliably generated.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | optional |  |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.scheduler..mesos.v1.ExecutorID) | optional | If this was just a failure of an executor on an agent then &#39;executor_id&#39; will be set and possibly &#39;status&#39; (if we were able to determine the exit status). |
| status | [int32](#int32) | optional |  |






<a name="mesos.v1.scheduler.Event.InverseOffers"/>

### Event.InverseOffers
Received whenever there are resources requested back from the
scheduler. Each inverse offer specifies the agent, and
optionally specific resources. Accepting or Declining an inverse
offer informs the allocator of the scheduler&#39;s ability to release
the specified resources without violating an SLA. If no resources
are specified then all resources on the agent are requested to be
released.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inverse_offers | [.mesos.v1.InverseOffer](#mesos.v1.scheduler..mesos.v1.InverseOffer) | repeated |  |






<a name="mesos.v1.scheduler.Event.Message"/>

### Event.Message
Received when a custom message generated by the executor is
forwarded by the master. Note that this message is not
interpreted by Mesos and is only forwarded (without reliability
guarantees) to the scheduler. It is up to the executor to retry
if the message is dropped for any reason.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| agent_id | [.mesos.v1.AgentID](#mesos.v1.scheduler..mesos.v1.AgentID) | required |  |
| executor_id | [.mesos.v1.ExecutorID](#mesos.v1.scheduler..mesos.v1.ExecutorID) | required |  |
| data | [bytes](#bytes) | required |  |






<a name="mesos.v1.scheduler.Event.Offers"/>

### Event.Offers
Received whenever there are new resources that are offered to the
scheduler. Each offer corresponds to a set of resources on an
agent. Until the scheduler accepts or declines an offer the
resources are considered allocated to the scheduler.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offers | [.mesos.v1.Offer](#mesos.v1.scheduler..mesos.v1.Offer) | repeated |  |






<a name="mesos.v1.scheduler.Event.Rescind"/>

### Event.Rescind
Received when a particular offer is no longer valid (e.g., the
agent corresponding to the offer has been removed) and hence
needs to be rescinded. Any future calls (&#39;Accept&#39; / &#39;Decline&#39;) made
by the scheduler regarding this offer will be invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offer_id | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | required |  |






<a name="mesos.v1.scheduler.Event.RescindInverseOffer"/>

### Event.RescindInverseOffer
Received when a particular inverse offer is no longer valid
(e.g., the agent corresponding to the offer has been removed)
and hence needs to be rescinded. Any future calls (&#39;Accept&#39;
&#39;Decline&#39;) made by the scheduler regarding this inverse offer
will be invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| inverse_offer_id | [.mesos.v1.OfferID](#mesos.v1.scheduler..mesos.v1.OfferID) | required |  |






<a name="mesos.v1.scheduler.Event.Subscribed"/>

### Event.Subscribed
First event received when the scheduler subscribes.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| framework_id | [.mesos.v1.FrameworkID](#mesos.v1.scheduler..mesos.v1.FrameworkID) | required |  |
| heartbeat_interval_seconds | [double](#double) | optional | This value will be set if the master is sending heartbeats. See the comment above on &#39;HEARTBEAT&#39; for more details. |
| master_info | [.mesos.v1.MasterInfo](#mesos.v1.scheduler..mesos.v1.MasterInfo) | optional | Since Mesos 1.1. |






<a name="mesos.v1.scheduler.Event.Update"/>

### Event.Update
Received whenever there is a status update that is generated by
the executor or agent or master. Status updates should be used by
executors to reliably communicate the status of the tasks that
they manage. It is crucial that a terminal update (see TaskState
in v1/mesos.proto) is sent by the executor as soon as the task
terminates, in order for Mesos to release the resources allocated
to the task. It is also the responsibility of the scheduler to
explicitly acknowledge the receipt of a status update. See
&#39;Acknowledge&#39; in the &#39;Call&#39; section below for the semantics.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [.mesos.v1.TaskStatus](#mesos.v1.scheduler..mesos.v1.TaskStatus) | required |  |





 


<a name="mesos.v1.scheduler.Call.Type"/>

### Call.Type
Possible call types, followed by message definitions if
applicable.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | See comments above on `Event::Type` for more details on this enum value. |
| SUBSCRIBE | 1 | See &#39;Subscribe&#39; below. |
| TEARDOWN | 2 | Shuts down all tasks/executors and removes framework. |
| ACCEPT | 3 | See &#39;Accept&#39; below. |
| DECLINE | 4 | See &#39;Decline&#39; below. |
| ACCEPT_INVERSE_OFFERS | 13 | See &#39;AcceptInverseOffers&#39; below. |
| DECLINE_INVERSE_OFFERS | 14 | See &#39;DeclineInverseOffers&#39; below. |
| REVIVE | 5 | Removes any previous filters set via ACCEPT or DECLINE. |
| KILL | 6 | See &#39;Kill&#39; below. |
| SHUTDOWN | 7 | See &#39;Shutdown&#39; below. |
| ACKNOWLEDGE | 8 | See &#39;Acknowledge&#39; below. |
| RECONCILE | 9 | See &#39;Reconcile&#39; below. |
| MESSAGE | 10 | See &#39;Message&#39; below. |
| REQUEST | 11 | See &#39;Request&#39; below. |
| SUPPRESS | 12 | Inform master to stop sending offers to the framework. |



<a name="mesos.v1.scheduler.Event.Type"/>

### Event.Type
Possible event types, followed by message definitions if
applicable.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This must be the first enum value in this list, to ensure that if &#39;type&#39; is not set, the default value is UNKNOWN. This enables enum values to be added in a backwards-compatible way. See: MESOS-4997. |
| SUBSCRIBED | 1 | See &#39;Subscribed&#39; below. |
| OFFERS | 2 | See &#39;Offers&#39; below. |
| INVERSE_OFFERS | 9 | See &#39;InverseOffers&#39; below. |
| RESCIND | 3 | See &#39;Rescind&#39; below. |
| RESCIND_INVERSE_OFFER | 10 | See &#39;RescindInverseOffer&#39; below. |
| UPDATE | 4 | See &#39;Update&#39; below. |
| MESSAGE | 5 | See &#39;Message&#39; below. |
| FAILURE | 6 | See &#39;Failure&#39; below. |
| ERROR | 7 | See &#39;Error&#39; below. |
| HEARTBEAT | 8 | Periodic message sent by the Mesos master according to &#39;Subscribed.heartbeat_interval_seconds&#39;. If the scheduler does not receive any events (including heartbeats) for an extended period of time (e.g., 5 x heartbeat_interval_seconds), there is likely a network partition. In such a case the scheduler should close the existing subscription connection and resubscribe using a backoff strategy. |


 

 

 



<a name="changelog.proto"/>
<p align="right"><a href="#top">Top</a></p>

## changelog.proto



<a name="peloton.api.changelog.ChangeLog"/>

### ChangeLog
Change log of the entity info


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [int64](#int64) |  | Version number of the entity info which is monotonically increasing. Clients can use this to guide against race conditions using MVCC. |
| createdAt | [int64](#int64) |  | The timestamp when the entity info is created |
| updatedAt | [int64](#int64) |  | The timestamp when the entity info is updated |
| updatedBy | [string](#string) |  | The entity of the user that updated the entity info |





 

 

 

 



<a name="errors.proto"/>
<p align="right"><a href="#top">Top</a></p>

## errors.proto



<a name="peloton.api.errors.InvalidRespool"/>

### InvalidRespool



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respoolID | [.peloton.api.peloton.ResourcePoolID](#peloton.api.errors..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.errors.JobGetRuntimeFail"/>

### JobGetRuntimeFail



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.errors..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.errors.JobNotFound"/>

### JobNotFound



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.errors..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.errors.UnknownError"/>

### UnknownError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |





 

 

 

 



<a name="query.proto"/>
<p align="right"><a href="#top">Top</a></p>

## query.proto



<a name="peloton.api.query.OrderBy"/>

### OrderBy
Order by clause of a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| order | [OrderBy.Order](#peloton.api.query.OrderBy.Order) |  |  |
| property | [PropertyPath](#peloton.api.query.PropertyPath) |  |  |






<a name="peloton.api.query.Pagination"/>

### Pagination
Generic pagination for a list of records to be returned by a query


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the pagination for a query result |
| limit | [uint32](#uint32) |  | Limit of the pagination for a query result |
| total | [uint32](#uint32) |  | Total number of records for a query result |






<a name="peloton.api.query.PaginationSpec"/>

### PaginationSpec
Pagination query spec used as argument to queries that returns a Pagination
result.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint32](#uint32) |  | Offset of the query for pagination |
| limit | [uint32](#uint32) |  | Limit per page of the query for pagination |
| orderBy | [OrderBy](#peloton.api.query.OrderBy) | repeated | List of fields to be order by in sequence |
| maxLimit | [uint32](#uint32) |  | Max limit of the pagination result. |






<a name="peloton.api.query.PropertyPath"/>

### PropertyPath
A dot separated path to a object property such as config.name or
runtime.creationTime for a job object.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |





 


<a name="peloton.api.query.OrderBy.Order"/>

### OrderBy.Order


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| ASC | 1 |  |
| DESC | 2 |  |


 

 

 



<a name="task.proto"/>
<p align="right"><a href="#top">Top</a></p>

## task.proto



<a name="peloton.api.task.AndConstraint"/>

### AndConstraint
AndConstraint represents a logical &#39;and&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.task.Constraint) | repeated |  |






<a name="peloton.api.task.BrowseSandboxFailure"/>

### BrowseSandboxFailure
Failures for browsing sandbox files requests to mesos call.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.task.BrowseSandboxRequest"/>

### BrowseSandboxRequest
DEPRECATED by peloton.api.task.svc.BrowseSandboxRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| instanceId | [uint32](#uint32) |  |  |






<a name="peloton.api.task.BrowseSandboxResponse"/>

### BrowseSandboxResponse
DEPRECATED by peloton.api.task.svc.BrowseSandboxResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [BrowseSandboxResponse.Error](#peloton.api.task.BrowseSandboxResponse.Error) |  |  |
| hostname | [string](#string) |  |  |
| port | [string](#string) |  |  |
| paths | [string](#string) | repeated |  |






<a name="peloton.api.task.BrowseSandboxResponse.Error"/>

### BrowseSandboxResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange) |  |  |
| notRunning | [TaskNotRunning](#peloton.api.task.TaskNotRunning) |  |  |
| failure | [BrowseSandboxFailure](#peloton.api.task.BrowseSandboxFailure) |  |  |






<a name="peloton.api.task.Constraint"/>

### Constraint
Constraint represents a host label constraint or a related tasks label constraint.
This is used to require that a host have certain label constraints or to require
that the tasks already running on the host have certain label constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [Constraint.Type](#peloton.api.task.Constraint.Type) |  |  |
| labelConstraint | [LabelConstraint](#peloton.api.task.LabelConstraint) |  |  |
| andConstraint | [AndConstraint](#peloton.api.task.AndConstraint) |  |  |
| orConstraint | [OrConstraint](#peloton.api.task.OrConstraint) |  |  |






<a name="peloton.api.task.GetEventsRequest"/>

### GetEventsRequest
DEPRECATED by peloton.api.task.svc.GetEventsRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| instanceId | [uint32](#uint32) |  |  |






<a name="peloton.api.task.GetEventsResponse"/>

### GetEventsResponse
DEPRECATED by peloton.api.task.svc.GetEventsResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [GetEventsResponse.Events](#peloton.api.task.GetEventsResponse.Events) | repeated |  |
| error | [GetEventsResponse.Error](#peloton.api.task.GetEventsResponse.Error) |  |  |






<a name="peloton.api.task.GetEventsResponse.Error"/>

### GetEventsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| eventError | [TaskEventsError](#peloton.api.task.TaskEventsError) |  |  |






<a name="peloton.api.task.GetEventsResponse.Events"/>

### GetEventsResponse.Events



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [TaskEvent](#peloton.api.task.TaskEvent) | repeated |  |






<a name="peloton.api.task.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.task.svc.GetTaskRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| instanceId | [uint32](#uint32) |  |  |






<a name="peloton.api.task.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.task.svc.GetTaskResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [TaskInfo](#peloton.api.task.TaskInfo) |  |  |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange) |  |  |






<a name="peloton.api.task.HealthCheckConfig"/>

### HealthCheckConfig
Health check configuration for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| enabled | [bool](#bool) |  | Whether the health check is enabled. |
| initialIntervalSecs | [uint32](#uint32) |  | Start time wait in seconds. Zero or empty value would use default value of 15 from Mesos. |
| intervalSecs | [uint32](#uint32) |  | Interval in seconds between two health checks. Zero or empty value would use default value of 10 from Mesos. |
| maxConsecutiveFailures | [uint32](#uint32) |  | Max number of consecutive failures before failing health check. Zero or empty value would use default value of 3 from Mesos. |
| timeoutSecs | [uint32](#uint32) |  | Health check command timeout in seconds. Zero or empty value would use default value of 20 from Mesos. |
| type | [HealthCheckConfig.Type](#peloton.api.task.HealthCheckConfig.Type) |  |  |
| commandCheck | [HealthCheckConfig.CommandCheck](#peloton.api.task.HealthCheckConfig.CommandCheck) |  | Only applicable when type is `COMMAND`. |






<a name="peloton.api.task.HealthCheckConfig.CommandCheck"/>

### HealthCheckConfig.CommandCheck



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| command | [string](#string) |  | Health check command to be executed. Note that this command by default inherits all environment varibles from the task it&#39;s monitoring, unless `unshare_environments` is set to true. |
| unshareEnvironments | [bool](#bool) |  | If set, this check will not share the environment variables of the task. |






<a name="peloton.api.task.InstanceIdOutOfRange"/>

### InstanceIdOutOfRange
DEPRECATED by google.rpc.OUT_OF_RANGE error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  | Entity ID of the job |
| instanceCount | [uint32](#uint32) |  | Instance count of the job |






<a name="peloton.api.task.InstanceRange"/>

### InstanceRange
Task InstanceID range [from, to)


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from | [uint32](#uint32) |  |  |
| to | [uint32](#uint32) |  |  |






<a name="peloton.api.task.LabelConstraint"/>

### LabelConstraint
LabelConstraint represents a constraint on the number of occources a given
label from the set of host labels or task labels present on the host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [LabelConstraint.Kind](#peloton.api.task.LabelConstraint.Kind) |  | Determines which labels the constraint should apply to. |
| condition | [LabelConstraint.Condition](#peloton.api.task.LabelConstraint.Condition) |  | Determines which constraint there should be on the number of occurrences of the label. |
| label | [.peloton.api.peloton.Label](#peloton.api.task..peloton.api.peloton.Label) |  | The label which this defines a constraint on: For Kind == HOST, each attribute on Mesos agent is transformed to a label, with `hostname` as a special label which is always inferred from agent hostname and set. |
| requirement | [uint32](#uint32) |  | A limit on the number of occurrences of the label. |






<a name="peloton.api.task.ListRequest"/>

### ListRequest
DEPRECATED by peloton.api.task.svc.ListTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| range | [InstanceRange](#peloton.api.task.InstanceRange) |  |  |






<a name="peloton.api.task.ListResponse"/>

### ListResponse
DEPRECATED by peloton.api.task.svc.ListTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [ListResponse.Result](#peloton.api.task.ListResponse.Result) |  |  |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |






<a name="peloton.api.task.ListResponse.Result"/>

### ListResponse.Result



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [ListResponse.Result.ValueEntry](#peloton.api.task.ListResponse.Result.ValueEntry) | repeated |  |






<a name="peloton.api.task.ListResponse.Result.ValueEntry"/>

### ListResponse.Result.ValueEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [TaskInfo](#peloton.api.task.TaskInfo) |  |  |






<a name="peloton.api.task.OrConstraint"/>

### OrConstraint
OrConstraint represents a logical &#39;or&#39; of constraints.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| constraints | [Constraint](#peloton.api.task.Constraint) | repeated |  |






<a name="peloton.api.task.PersistentVolumeConfig"/>

### PersistentVolumeConfig
Persistent volume configuration for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| containerPath | [string](#string) |  | Volume mount path inside container. |
| sizeMB | [uint32](#uint32) |  | Volume size in MB. |






<a name="peloton.api.task.PortConfig"/>

### PortConfig
Network port configuration for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the network port, e.g. http, tchannel. Required field. |
| value | [uint32](#uint32) |  | Static port number if any. If unset, will be dynamically allocated by the scheduler |
| envName | [string](#string) |  | Environment variable name to be exported when running a task for this port. Required field for dynamic port. |






<a name="peloton.api.task.PreemptionPolicy"/>

### PreemptionPolicy
Preemption policy for a task


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| killOnPreempt | [bool](#bool) |  | This polciy defines if the task should be restarted after it is preempted. If set to true the task will not be rescheduled after it is preempted. If set to false the task will be rescheduled. Defaults to false |






<a name="peloton.api.task.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.task.svc.QueryTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| spec | [QuerySpec](#peloton.api.task.QuerySpec) |  |  |






<a name="peloton.api.task.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.task.svc.QueryTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.task.QueryResponse.Error) |  |  |
| records | [TaskInfo](#peloton.api.task.TaskInfo) | repeated |  |
| pagination | [.peloton.api.query.Pagination](#peloton.api.task..peloton.api.query.Pagination) |  |  |






<a name="peloton.api.task.QueryResponse.Error"/>

### QueryResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |






<a name="peloton.api.task.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for tasks. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.query.PaginationSpec](#peloton.api.task..peloton.api.query.PaginationSpec) |  | DEPRECATED: use QueryJobRequest.pagination instead. The spec of how to do pagination for the query results. |
| taskStates | [TaskState](#peloton.api.task.TaskState) | repeated | List of task states to query the tasks. Will match all tasks if the list is empty. |






<a name="peloton.api.task.ResourceConfig"/>

### ResourceConfig
Resource configuration for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cpuLimit | [double](#double) |  | CPU limit in number of CPU cores |
| memLimitMb | [double](#double) |  | Memory limit in MB |
| diskLimitMb | [double](#double) |  | Disk limit in MB |
| fdLimit | [uint32](#uint32) |  | File descriptor limit |
| gpuLimit | [double](#double) |  | GPU limit in number of GPUs |






<a name="peloton.api.task.RestartPolicy"/>

### RestartPolicy
Restart policy for a task.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxFailures | [uint32](#uint32) |  | Max number of task failures can occur before giving up scheduling retry, no backoff for now. Default 0 means no retry on failures. |






<a name="peloton.api.task.RestartRequest"/>

### RestartRequest
DEPRECATED by peloton.api.task.svc.RestartTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.task.InstanceRange) | repeated |  |






<a name="peloton.api.task.RestartResponse"/>

### RestartResponse
DEPRECATED by peloton.api.task.svc.RestartTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange) |  |  |






<a name="peloton.api.task.RuntimeInfo"/>

### RuntimeInfo
Runtime info of an task instance in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [TaskState](#peloton.api.task.TaskState) |  | Runtime status of the task |
| mesosTaskId | [.mesos.v1.TaskID](#peloton.api.task..mesos.v1.TaskID) |  | The mesos task ID for this instance |
| startTime | [string](#string) |  | The time when the instance starts to run. Will be unset if the instance hasn&#39;t started running yet. The time is represented in RFC3339 form with UTC timezone. |
| completionTime | [string](#string) |  | The time when the instance is completed. Will be unset if the instance hasn&#39;t completed yet. The time is represented in RFC3339 form with UTC timezone. |
| host | [string](#string) |  | The name of the host where the instance is running |
| ports | [RuntimeInfo.PortsEntry](#peloton.api.task.RuntimeInfo.PortsEntry) | repeated | Dynamic ports reserved on the host while this instance is running |
| goalState | [TaskState](#peloton.api.task.TaskState) |  | The desired state of the task which should be eventually reached by the system. |
| message | [string](#string) |  | The message that explains the current state of a task such as why the task is failed. Only track the latest one if the task has been retried and failed multiple times. |
| reason | [string](#string) |  | The reason that explains the current state of a task. Only track the latest one if the task has been retried and failed multiple times. See Mesos TaskStatus.Reason for more details. |
| failureCount | [uint32](#uint32) |  | The number of times the task has failed after retries. |
| volumeID | [.peloton.api.peloton.VolumeID](#peloton.api.task..peloton.api.peloton.VolumeID) |  | persistent volume id |
| configVersion | [uint64](#uint64) |  | The config version currently used by the runtime. |
| desiredConfigVersion | [uint64](#uint64) |  | The desired config version that should be used by the runtime. |
| agentID | [.mesos.v1.AgentID](#peloton.api.task..mesos.v1.AgentID) |  | the id of mesos agent on the host to be launched. |
| revision | [.peloton.api.peloton.ChangeLog](#peloton.api.task..peloton.api.peloton.ChangeLog) |  | Revision of the current task info. |






<a name="peloton.api.task.RuntimeInfo.PortsEntry"/>

### RuntimeInfo.PortsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.task.StartRequest"/>

### StartRequest
DEPRECATED by peloton.api.task.svc.StartTasksRequest.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.task.InstanceRange) | repeated |  |






<a name="peloton.api.task.StartResponse"/>

### StartResponse
DEPRECATED by peloton.api.task.svc.StartTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [StartResponse.Error](#peloton.api.task.StartResponse.Error) |  |  |
| startedInstanceIds | [uint32](#uint32) | repeated |  |
| invalidInstanceIds | [uint32](#uint32) | repeated |  |






<a name="peloton.api.task.StartResponse.Error"/>

### StartResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange) |  |  |
| failure | [TaskStartFailure](#peloton.api.task.TaskStartFailure) |  |  |






<a name="peloton.api.task.StopRequest"/>

### StopRequest
DEPRECATED by peloton.api.task.svc.StopTasksRequest.
If no ranges specified, then stop all the tasks in the job.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  |  |
| ranges | [InstanceRange](#peloton.api.task.InstanceRange) | repeated |  |






<a name="peloton.api.task.StopResponse"/>

### StopResponse
DEPRECATED by peloton.api.task.svc.StopTasksResponse.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [StopResponse.Error](#peloton.api.task.StopResponse.Error) |  |  |
| stoppedInstanceIds | [uint32](#uint32) | repeated |  |
| invalidInstanceIds | [uint32](#uint32) | repeated |  |






<a name="peloton.api.task.StopResponse.Error"/>

### StopResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.task..peloton.api.errors.JobNotFound) |  |  |
| outOfRange | [InstanceIdOutOfRange](#peloton.api.task.InstanceIdOutOfRange) |  |  |
| updateError | [TaskUpdateError](#peloton.api.task.TaskUpdateError) |  |  |






<a name="peloton.api.task.TaskConfig"/>

### TaskConfig
Task configuration for a given job instance
Note that only add string/slice/ptr type into TaskConfig directly due to
the limitation of go reflection inside our task specific config logic.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the task |
| labels | [.peloton.api.peloton.Label](#peloton.api.task..peloton.api.peloton.Label) | repeated | List of user-defined labels for the task |
| resource | [ResourceConfig](#peloton.api.task.ResourceConfig) |  | Resource config of the task |
| container | [.mesos.v1.ContainerInfo](#peloton.api.task..mesos.v1.ContainerInfo) |  | Container config of the task. |
| command | [.mesos.v1.CommandInfo](#peloton.api.task..mesos.v1.CommandInfo) |  | Command line config of the task |
| healthCheck | [HealthCheckConfig](#peloton.api.task.HealthCheckConfig) |  | Health check config of the task |
| ports | [PortConfig](#peloton.api.task.PortConfig) | repeated | List of network ports to be allocated for the task |
| constraint | [Constraint](#peloton.api.task.Constraint) |  | Constraint on the attributes of the host or labels on tasks on the host that this task should run on. Use `AndConstraint`/`OrConstraint` to compose multiple constraints if necessary. |
| restartPolicy | [RestartPolicy](#peloton.api.task.RestartPolicy) |  | Task restart policy on failures |
| volume | [PersistentVolumeConfig](#peloton.api.task.PersistentVolumeConfig) |  | Persistent volume config of the task. |
| preemptionPolicy | [PreemptionPolicy](#peloton.api.task.PreemptionPolicy) |  | Preemtion policy of the task |






<a name="peloton.api.task.TaskEvent"/>

### TaskEvent
Task event of a Peloton task instance.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskId | [.peloton.api.peloton.TaskID](#peloton.api.task..peloton.api.peloton.TaskID) |  | The task ID of the task event. |
| state | [TaskState](#peloton.api.task.TaskState) |  | The task state of the task event. |
| message | [string](#string) |  | Short human friendly message explaining state. |
| timestamp | [string](#string) |  | The time when the event was created. The time is represented in RFC3339 form with UTC timezone. |
| source | [TaskEvent.Source](#peloton.api.task.TaskEvent.Source) |  | The source that generated the task event. |
| hostname | [string](#string) |  | The host on which the task is running |
| reason | [string](#string) |  | The short reason for the task event |






<a name="peloton.api.task.TaskEventsError"/>

### TaskEventsError
DEPRECATED by google.rpc.INTERNAL error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.task.TaskInfo"/>

### TaskInfo
Info of a task instance in a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| instanceId | [uint32](#uint32) |  | The numerical ID assigned to this instance. Instance IDs must be unique and contiguous within a job. The ID is in the range of [0, N-1] for a job with instance count of N. |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task..peloton.api.peloton.JobID) |  | Job ID of the task |
| config | [TaskConfig](#peloton.api.task.TaskConfig) |  | Configuration of the task |
| runtime | [RuntimeInfo](#peloton.api.task.RuntimeInfo) |  | Runtime info of the instance |






<a name="peloton.api.task.TaskNotRunning"/>

### TaskNotRunning
Task not running error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.task.TaskStartFailure"/>

### TaskStartFailure
Error when TaskStart failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.api.task.TaskUpdateError"/>

### TaskUpdateError
DEPRECATED by google.rpc.INTERNAL error.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |





 


<a name="peloton.api.task.Constraint.Type"/>

### Constraint.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN_CONSTRAINT | 0 | Reserved for compatibility. |
| LABEL_CONSTRAINT | 1 |  |
| AND_CONSTRAINT | 2 |  |
| OR_CONSTRAINT | 3 |  |



<a name="peloton.api.task.HealthCheckConfig.Type"/>

### HealthCheckConfig.Type


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new types. |
| COMMAND | 1 | Command line based health check |
| HTTP | 2 | HTTP endpoint based health check |



<a name="peloton.api.task.LabelConstraint.Condition"/>

### LabelConstraint.Condition
Condition represents a constraint on the number of occurrences of the label.

| Name | Number | Description |
| ---- | ------ | ----------- |
| CONDITION_UNKNOWN | 0 | Reserved for compatibility. |
| CONDITION_LESS_THAN | 1 |  |
| CONDITION_EQUAL | 2 |  |
| CONDITION_GREATER_THAN | 3 |  |



<a name="peloton.api.task.LabelConstraint.Kind"/>

### LabelConstraint.Kind
Kind represents whatever the constraint applies to the labels on the host
or to the labels of the tasks that are located on the host.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for compatibility. |
| TASK | 1 |  |
| HOST | 2 |  |



<a name="peloton.api.task.TaskEvent.Source"/>

### TaskEvent.Source
Describes the source of the task event

| Name | Number | Description |
| ---- | ------ | ----------- |
| SOURCE_UNKNOWN | 0 |  |
| SOURCE_JOBMGR | 1 |  |
| SOURCE_RESMGR | 2 |  |
| SOURCE_HOSTMGR | 3 |  |



<a name="peloton.api.task.TaskState"/>

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


 

 


<a name="peloton.api.task.TaskManager"/>

### TaskManager
Task manager interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Get | [GetRequest](#peloton.api.task.GetRequest) | [GetResponse](#peloton.api.task.GetRequest) | Get the info of a task in job. |
| List | [ListRequest](#peloton.api.task.ListRequest) | [ListResponse](#peloton.api.task.ListRequest) | List all task info in a job. |
| Start | [StartRequest](#peloton.api.task.StartRequest) | [StartResponse](#peloton.api.task.StartRequest) | Start a set of tasks for a job. Will be no-op for tasks that are currently running. |
| Stop | [StopRequest](#peloton.api.task.StopRequest) | [StopResponse](#peloton.api.task.StopRequest) | Stop a set of tasks for a job. Will be no-op for tasks that are currently stopped. |
| Restart | [RestartRequest](#peloton.api.task.RestartRequest) | [RestartResponse](#peloton.api.task.RestartRequest) | Restart a set of tasks for a job. Will start tasks that are currently stopped. |
| Query | [QueryRequest](#peloton.api.task.QueryRequest) | [QueryResponse](#peloton.api.task.QueryRequest) | Query task info in a job, using a set of filters. |
| BrowseSandbox | [BrowseSandboxRequest](#peloton.api.task.BrowseSandboxRequest) | [BrowseSandboxResponse](#peloton.api.task.BrowseSandboxRequest) | BrowseSandbox returns list of file paths inside sandbox. |
| GetEvents | [GetEventsRequest](#peloton.api.task.GetEventsRequest) | [GetEventsResponse](#peloton.api.task.GetEventsRequest) | Get event changes of a task in job. |

 



<a name="respool.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool.proto



<a name="peloton.api.respool.CreateRequest"/>

### CreateRequest
DEPRECATED by peloton.api.respool.svc.CreateResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| config | [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig) |  |  |






<a name="peloton.api.respool.CreateResponse"/>

### CreateResponse
DEPRECATED by peloton.api.respool.svc.CreateResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [CreateResponse.Error](#peloton.api.respool.CreateResponse.Error) |  |  |
| result | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |






<a name="peloton.api.respool.CreateResponse.Error"/>

### CreateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| alreadyExists | [ResourcePoolAlreadyExists](#peloton.api.respool.ResourcePoolAlreadyExists) |  |  |
| invalidResourcePoolConfig | [InvalidResourcePoolConfig](#peloton.api.respool.InvalidResourcePoolConfig) |  |  |






<a name="peloton.api.respool.DeleteRequest"/>

### DeleteRequest
DEPRECATED by peloton.api.respool.svc.DeleteResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath) |  |  |






<a name="peloton.api.respool.DeleteResponse"/>

### DeleteResponse
DEPRECATED by peloton.api.respool.svc.DeleteResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [DeleteResponse.Error](#peloton.api.respool.DeleteResponse.Error) |  |  |






<a name="peloton.api.respool.DeleteResponse.Error"/>

### DeleteResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolPathNotFound](#peloton.api.respool.ResourcePoolPathNotFound) |  |  |
| isBusy | [ResourcePoolIsBusy](#peloton.api.respool.ResourcePoolIsBusy) |  |  |
| isNotLeaf | [ResourcePoolIsNotLeaf](#peloton.api.respool.ResourcePoolIsNotLeaf) |  |  |
| notDeleted | [ResourcePoolNotDeleted](#peloton.api.respool.ResourcePoolNotDeleted) |  |  |






<a name="peloton.api.respool.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.respool.svc.GetResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The ID of the resource pool to get |
| includeChildPools | [bool](#bool) |  | Whether or not to include the resource pool info of the direct children |






<a name="peloton.api.respool.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.respool.svc.GetResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetResponse.Error](#peloton.api.respool.GetResponse.Error) |  |  |
| poolinfo | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) |  |  |
| childPools | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.respool.GetResponse.Error"/>

### GetResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolNotFound](#peloton.api.respool.ResourcePoolNotFound) |  |  |






<a name="peloton.api.respool.InvalidResourcePoolConfig"/>

### InvalidResourcePoolConfig
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.InvalidResourcePoolPath"/>

### InvalidResourcePoolPath
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.LookupRequest"/>

### LookupRequest
DEPRECATED by peloton.api.respool.svc.LookupResourcePoolIDRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath) |  |  |






<a name="peloton.api.respool.LookupResponse"/>

### LookupResponse
DEPRECATED by peloton.api.respool.svc.LookupResourcePoolIDResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [LookupResponse.Error](#peloton.api.respool.LookupResponse.Error) |  |  |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |






<a name="peloton.api.respool.LookupResponse.Error"/>

### LookupResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolPathNotFound](#peloton.api.respool.ResourcePoolPathNotFound) |  |  |
| invalidPath | [InvalidResourcePoolPath](#peloton.api.respool.InvalidResourcePoolPath) |  |  |






<a name="peloton.api.respool.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.respool.svc.QueryResourcePoolRequest


TODO Filters






<a name="peloton.api.respool.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.respool.svc.QueryResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.respool.QueryResponse.Error) |  |  |
| resourcePools | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.respool.QueryResponse.Error"/>

### QueryResponse.Error
TODO add error types






<a name="peloton.api.respool.ResourceConfig"/>

### ResourceConfig
Resource configuration for a resource


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| reservation | [double](#double) |  | Reservation/min of the resource |
| limit | [double](#double) |  | Limit of the resource |
| share | [double](#double) |  | Share on the resource pool |






<a name="peloton.api.respool.ResourcePoolAlreadyExists"/>

### ResourcePoolAlreadyExists
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolConfig"/>

### ResourcePoolConfig
Resource Pool configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| changeLog | [.peloton.api.changelog.ChangeLog](#peloton.api.respool..peloton.api.changelog.ChangeLog) |  | Change log entry of the Resource Pool config |
| name | [string](#string) |  | Name of the resource pool |
| owningTeam | [string](#string) |  | Owning team of the pool |
| ldapGroups | [string](#string) | repeated | LDAP groups of the pool |
| description | [string](#string) |  | Description of the resource pool |
| resources | [ResourceConfig](#peloton.api.respool.ResourceConfig) | repeated | Resource config of the Resource Pool |
| parent | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent |
| policy | [SchedulingPolicy](#peloton.api.respool.SchedulingPolicy) |  | Task Scheduling policy |






<a name="peloton.api.respool.ResourcePoolInfo"/>

### ResourcePoolInfo



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | Resource Pool Id |
| config | [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig) |  | ResourcePool config |
| parent | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | Resource Pool&#39;s parent TODO: parent duplicated from ResourcePoolConfig |
| children | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) | repeated | Resource Pool&#39;s children |
| usage | [ResourceUsage](#peloton.api.respool.ResourceUsage) | repeated | Resource usage for each resource kind |






<a name="peloton.api.respool.ResourcePoolIsBusy"/>

### ResourcePoolIsBusy
DEPRECATED by google.rpc.FAILED_PRECONDITION error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolIsNotLeaf"/>

### ResourcePoolIsNotLeaf
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolNotDeleted"/>

### ResourcePoolNotDeleted
DEPRECATED by google.rpc.INTERNAL error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolNotFound"/>

### ResourcePoolNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolPath"/>

### ResourcePoolPath
A fully qualified path to a resource pool in a resource pool hierrarchy.
The path to a resource pool can be defined as an absolute path,
starting from the root node and separated by a slash.

The resource hierarchy is anchored at a node called the root,
designated by a slash &#34;/&#34;.

For thhe below resource hierarchy ; the &#34;compute&#34; resource pool would be
desgignated by path: /infrastructure/compute
root
├─ infrastructure
│  └─ compute
└─ marketplace


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.respool.ResourcePoolPathNotFound"/>

### ResourcePoolPathNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.respool.ResourceUsage"/>

### ResourceUsage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| allocation | [double](#double) |  | Allocation of the resource |
| slack | [double](#double) |  | slack is the resource which is allocated but not used and mesos will give those resources as revocable offers |






<a name="peloton.api.respool.UpdateRequest"/>

### UpdateRequest
DEPRECATED by peloton.api.respool.svc.UpdateResourcePoolRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  |  |
| config | [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig) |  |  |






<a name="peloton.api.respool.UpdateResponse"/>

### UpdateResponse
DEPRECATED by peloton.api.respool.svc.UpdateResourcePoolResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [UpdateResponse.Error](#peloton.api.respool.UpdateResponse.Error) |  |  |






<a name="peloton.api.respool.UpdateResponse.Error"/>

### UpdateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolNotFound](#peloton.api.respool.ResourcePoolNotFound) |  |  |
| invalidResourcePoolConfig | [InvalidResourcePoolConfig](#peloton.api.respool.InvalidResourcePoolConfig) |  |  |





 


<a name="peloton.api.respool.SchedulingPolicy"/>

### SchedulingPolicy
Scheduling policy for Resource Pool.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | TODO: We need to investigate why yarpc is ommiting value 0 |
| PriorityFIFO | 1 | This scheduling policy will return item for highest priority in FIFO order |


 

 


<a name="peloton.api.respool.ResourceManager"/>

### ResourceManager
DEPRECATED by peloton.api.respool.svc.ResourcePoolService
Resource Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateResourcePool | [CreateRequest](#peloton.api.respool.CreateRequest) | [CreateResponse](#peloton.api.respool.CreateRequest) | Create a resource pool entity for a given config |
| GetResourcePool | [GetRequest](#peloton.api.respool.GetRequest) | [GetResponse](#peloton.api.respool.GetRequest) | Get the resource pool entity |
| DeleteResourcePool | [DeleteRequest](#peloton.api.respool.DeleteRequest) | [DeleteResponse](#peloton.api.respool.DeleteRequest) | Delete a resource pool entity |
| UpdateResourcePool | [UpdateRequest](#peloton.api.respool.UpdateRequest) | [UpdateResponse](#peloton.api.respool.UpdateRequest) | modify a resource pool entity |
| LookupResourcePoolID | [LookupRequest](#peloton.api.respool.LookupRequest) | [LookupResponse](#peloton.api.respool.LookupRequest) | Lookup the resource pool ID for a given resource pool path |
| Query | [QueryRequest](#peloton.api.respool.QueryRequest) | [QueryResponse](#peloton.api.respool.QueryRequest) | Query the resource pool. |

 



<a name="job.proto"/>
<p align="right"><a href="#top">Top</a></p>

## job.proto



<a name="peloton.api.job.CreateRequest"/>

### CreateRequest
DEPRECATED by peloton.api.job.svc.CreateJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| config | [JobConfig](#peloton.api.job.JobConfig) |  |  |






<a name="peloton.api.job.CreateResponse"/>

### CreateResponse
DEPRECATED by peloton.api.job.svc.CreateJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [CreateResponse.Error](#peloton.api.job.CreateResponse.Error) |  |  |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |






<a name="peloton.api.job.CreateResponse.Error"/>

### CreateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| alreadyExists | [JobAlreadyExists](#peloton.api.job.JobAlreadyExists) |  |  |
| invalidConfig | [InvalidJobConfig](#peloton.api.job.InvalidJobConfig) |  |  |
| invalidJobId | [InvalidJobId](#peloton.api.job.InvalidJobId) |  |  |






<a name="peloton.api.job.DeleteRequest"/>

### DeleteRequest
DEPRECATED by peloton.api.job.svc.DeleteRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |






<a name="peloton.api.job.DeleteResponse"/>

### DeleteResponse
DEPRECATED by peloton.api.job.svc.DeleteResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [DeleteResponse.Error](#peloton.api.job.DeleteResponse.Error) |  |  |






<a name="peloton.api.job.DeleteResponse.Error"/>

### DeleteResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.job..peloton.api.errors.JobNotFound) |  |  |






<a name="peloton.api.job.GetRequest"/>

### GetRequest
DEPRECATED by peloton.api.job.svc.GetJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |






<a name="peloton.api.job.GetResponse"/>

### GetResponse
DEPRECATED by peloton.api.job.svc.GetJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetResponse.Error](#peloton.api.job.GetResponse.Error) |  |  |
| jobInfo | [JobInfo](#peloton.api.job.JobInfo) |  |  |






<a name="peloton.api.job.GetResponse.Error"/>

### GetResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [.peloton.api.errors.JobNotFound](#peloton.api.job..peloton.api.errors.JobNotFound) |  |  |
| getRuntimeFail | [.peloton.api.errors.JobGetRuntimeFail](#peloton.api.job..peloton.api.errors.JobGetRuntimeFail) |  |  |






<a name="peloton.api.job.InvalidJobConfig"/>

### InvalidJobConfig
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.job.InvalidJobId"/>

### InvalidJobId
DEPRECATED by google.rpc.INVALID_ARGUMENT error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.job.JobAlreadyExists"/>

### JobAlreadyExists
DEPRECATED by google.rpc.ALREADY_EXISTS error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.job.JobConfig"/>

### JobConfig
Job configuration


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| changeLog | [.peloton.api.peloton.ChangeLog](#peloton.api.job..peloton.api.peloton.ChangeLog) |  | Change log entry of the job config |
| name | [string](#string) |  | Name of the job |
| type | [JobType](#peloton.api.job.JobType) |  | Type of the job |
| owningTeam | [string](#string) |  | Owning team of the job |
| ldapGroups | [string](#string) | repeated | LDAP groups of the job |
| description | [string](#string) |  | Description of the job |
| labels | [.peloton.api.peloton.Label](#peloton.api.job..peloton.api.peloton.Label) | repeated | List of user-defined labels for the job |
| instanceCount | [uint32](#uint32) |  | Number of instances of the job |
| sla | [SlaConfig](#peloton.api.job.SlaConfig) |  | SLA config of the job |
| defaultConfig | [.peloton.api.task.TaskConfig](#peloton.api.job..peloton.api.task.TaskConfig) |  | Default task configuration of the job |
| instanceConfig | [JobConfig.InstanceConfigEntry](#peloton.api.job.JobConfig.InstanceConfigEntry) | repeated | Instance specific task config which overwrites the default one |
| respoolID | [.peloton.api.peloton.ResourcePoolID](#peloton.api.job..peloton.api.peloton.ResourcePoolID) |  | Resource Pool ID where this job belongs to |






<a name="peloton.api.job.JobConfig.InstanceConfigEntry"/>

### JobConfig.InstanceConfigEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [.peloton.api.task.TaskConfig](#peloton.api.job..peloton.api.task.TaskConfig) |  |  |






<a name="peloton.api.job.JobInfo"/>

### JobInfo
Information of a job, such as job config and runtime


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  | Job ID |
| config | [JobConfig](#peloton.api.job.JobConfig) |  | Job configuration |
| runtime | [RuntimeInfo](#peloton.api.job.RuntimeInfo) |  | Job runtime information |






<a name="peloton.api.job.JobNotFound"/>

### JobNotFound
DEPRECATED by google.rpc.NOT_FOUND error


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.job.QueryRequest"/>

### QueryRequest
DEPRECATED by peloton.api.job.svc.QueryJobsRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| respoolID | [.peloton.api.peloton.ResourcePoolID](#peloton.api.job..peloton.api.peloton.ResourcePoolID) |  |  |
| spec | [QuerySpec](#peloton.api.job.QuerySpec) |  |  |






<a name="peloton.api.job.QueryResponse"/>

### QueryResponse
DEPRECATED by peloton.api.job.svc.QueryJobsResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [QueryResponse.Error](#peloton.api.job.QueryResponse.Error) |  |  |
| records | [JobInfo](#peloton.api.job.JobInfo) | repeated | NOTE: instanceConfig is not returned as part of jobConfig inside jobInfo as it can be so huge that exceeds grpc limit size. Use Job.Get() API to get instanceConfig for job. |
| pagination | [.peloton.api.query.Pagination](#peloton.api.job..peloton.api.query.Pagination) |  |  |






<a name="peloton.api.job.QueryResponse.Error"/>

### QueryResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| err | [.peloton.api.errors.UnknownError](#peloton.api.job..peloton.api.errors.UnknownError) |  |  |
| invalidRespool | [.peloton.api.errors.InvalidRespool](#peloton.api.job..peloton.api.errors.InvalidRespool) |  |  |






<a name="peloton.api.job.QuerySpec"/>

### QuerySpec
QuerySpec specifies the list of query criteria for jobs. All
indexed fields should be part of this message. And all fields
in this message have to be indexed too.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pagination | [.peloton.api.query.PaginationSpec](#peloton.api.job..peloton.api.query.PaginationSpec) |  | DEPRECATED: use QueryJobRequest.pagination instead. The spec of how to do pagination for the query results. |
| labels | [.peloton.api.peloton.Label](#peloton.api.job..peloton.api.peloton.Label) | repeated | List of labels to query the jobs. Will match all jobs if the list is empty. |
| keywords | [string](#string) | repeated | List of keywords to query the jobs. Will match all jobs if the list is empty. |
| jobStates | [JobState](#peloton.api.job.JobState) | repeated | List of job states to query the jobs. Will match all jobs if the list is empty. |
| respool | [.peloton.api.respool.ResourcePoolPath](#peloton.api.job..peloton.api.respool.ResourcePoolPath) |  | The resource pool to query the jobs. Will match jobs from all resource pools if unset. |






<a name="peloton.api.job.RuntimeInfo"/>

### RuntimeInfo
Job RuntimeInfo provides the current runtime status of a Job


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| state | [JobState](#peloton.api.job.JobState) |  | State of the job |
| creationTime | [string](#string) |  | The time when the job was created. The time is represented in RFC3339 form with UTC timezone. |
| startTime | [string](#string) |  | The time when the first task of the job starts to run. The time is represented in RFC3339 form with UTC timezone. |
| completionTime | [string](#string) |  | The time when the last task of the job is completed. The time is represented in RFC3339 form with UTC timezone. |
| taskStats | [RuntimeInfo.TaskStatsEntry](#peloton.api.job.RuntimeInfo.TaskStatsEntry) | repeated | The number of tasks grouped by each task state. The map key is the task.TaskState in string format and the map value is the number of tasks in the particular state. |
| configVersion | [int64](#int64) |  | The version reflects the version of the JobConfig, that&#39;s currently used by the job. |
| goalState | [JobState](#peloton.api.job.JobState) |  | Goal state of the job. |






<a name="peloton.api.job.RuntimeInfo.TaskStatsEntry"/>

### RuntimeInfo.TaskStatsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.api.job.SlaConfig"/>

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






<a name="peloton.api.job.UpdateRequest"/>

### UpdateRequest
DEPRECATED by peloton.api.job.svc.UpdateJobRequest


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| config | [JobConfig](#peloton.api.job.JobConfig) |  |  |






<a name="peloton.api.job.UpdateResponse"/>

### UpdateResponse
DEPRECATED by peloton.api.job.svc.UpdateJobResponse


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [UpdateResponse.Error](#peloton.api.job.UpdateResponse.Error) |  |  |
| id | [.peloton.api.peloton.JobID](#peloton.api.job..peloton.api.peloton.JobID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.api.job.UpdateResponse.Error"/>

### UpdateResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobNotFound | [JobNotFound](#peloton.api.job.JobNotFound) |  |  |
| invalidConfig | [InvalidJobConfig](#peloton.api.job.InvalidJobConfig) |  |  |
| invalidJobId | [InvalidJobId](#peloton.api.job.InvalidJobId) |  |  |





 


<a name="peloton.api.job.JobState"/>

### JobState
Runtime states of a Job

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new states. |
| INITIALIZED | 1 | The job has been initialized and persisted in DB. |
| PENDING | 2 | All tasks in the job are created and enqueued to resource manager |
| RUNNING | 3 | Any of the tasks in the job is in RUNNING state. |
| SUCCEEDED | 4 | All tasks in the job are in SUCCEEDED state. |
| FAILED | 5 | All tasks in the job are in terminated state and one or more tasks is in FAILED state. |
| KILLED | 6 | All tasks in the job are in terminated state and one or more tasks in the job is killed by the user. |



<a name="peloton.api.job.JobType"/>

### JobType
Job type definition such as batch, service and infra agent.

| Name | Number | Description |
| ---- | ------ | ----------- |
| BATCH | 0 | Normal batch job which will run to completion after all instances finishes. |
| SERVICE | 1 | Service job which is long running and will be restarted upon failures. |
| DAEMON | 2 | Daemon job which has one instance running on each host for infra agents like muttley, m3collector etc. |


 

 


<a name="peloton.api.job.JobManager"/>

### JobManager
DEPRECATED by peloton.api.job.svc.JobService
Job Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Create | [CreateRequest](#peloton.api.job.CreateRequest) | [CreateResponse](#peloton.api.job.CreateRequest) | Create a Job entity for a given config |
| Get | [GetRequest](#peloton.api.job.GetRequest) | [GetResponse](#peloton.api.job.GetRequest) | Get the config of a job entity |
| Query | [QueryRequest](#peloton.api.job.QueryRequest) | [QueryResponse](#peloton.api.job.QueryRequest) | Query the jobs that match a list of labels. |
| Delete | [DeleteRequest](#peloton.api.job.DeleteRequest) | [DeleteResponse](#peloton.api.job.DeleteRequest) | Delete a job entity and stop all related tasks |
| Update | [UpdateRequest](#peloton.api.job.UpdateRequest) | [UpdateResponse](#peloton.api.job.UpdateRequest) | Update a Job entity with a new config |

 



<a name="update.proto"/>
<p align="right"><a href="#top">Top</a></p>

## update.proto



<a name="peloton.api.update.UpdateConfig"/>

### UpdateConfig
Update options for a job update


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| batchSize | [int32](#int32) |  | Update batch size of the deployment |
| batchPercentage | [double](#double) |  | Update batch percentage of the deployment. If present, will take precedence over batchSize |
| stopBeforeUpdate | [bool](#bool) |  | Whether or not to stop all instance before update |
| startPaused | [bool](#bool) |  | startPaused indicates if the update should start in the paused state, requiring an explicit resume to initiate. |






<a name="peloton.api.update.UpdateID"/>

### UpdateID
A unique ID assigned to a update.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [string](#string) |  |  |






<a name="peloton.api.update.UpdateInfo"/>

### UpdateInfo
Information of an update, such as update config and runtime status


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [UpdateID](#peloton.api.update.UpdateID) |  | Update ID of the job update |
| config | [UpdateConfig](#peloton.api.update.UpdateConfig) |  | Update configuration |
| status | [UpdateStatus](#peloton.api.update.UpdateStatus) |  | Update runtime status |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.update..peloton.api.peloton.JobID) |  | Job ID of the job update |






<a name="peloton.api.update.UpdateStatus"/>

### UpdateStatus
UpdateStatus provides current runtime status of an update


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| numTasksDone | [int32](#int32) |  | Number of tasks that have been updated |
| numTasksRemaining | [int32](#int32) |  | Number of tasks to be updated |
| state | [State](#peloton.api.update.State) |  | Runtime state of the update |





 


<a name="peloton.api.update.State"/>

### State
Runtime state of a job update

| Name | Number | Description |
| ---- | ------ | ----------- |
| ROLLING_FORWARD | 0 | The update is rolling forward |
| ROLLING_BACK | 1 | The update is rolling back |
| PAUSED | 2 | The update is paused |
| SUCCEEDED | 3 | The update completed successfully |
| ROLLED_BACK | 4 | The update is rolled back |
| ABORTED | 5 | The update is aborted |


 

 

 



<a name="volume.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume.proto



<a name="peloton.api.volume.PersistentVolumeInfo"/>

### PersistentVolumeInfo
Persistent volume information.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.VolumeID](#peloton.api.volume..peloton.api.peloton.VolumeID) |  | ID of the persistent volume. |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.volume..peloton.api.peloton.JobID) |  | ID of the job that owns the volume. |
| instanceId | [uint32](#uint32) |  | ID of the instance that owns the volume. |
| hostname | [string](#string) |  | Hostname of the persisted volume. |
| state | [VolumeState](#peloton.api.volume.VolumeState) |  | Current state of the volume. |
| goalState | [VolumeState](#peloton.api.volume.VolumeState) |  | Goal state of the volume. |
| sizeMB | [uint32](#uint32) |  | Volume size in MB. |
| containerPath | [string](#string) |  | Volume mount path inside container. |
| createTime | [string](#string) |  | Volume creation time. |
| updateTime | [string](#string) |  | Volume info last update time. |





 


<a name="peloton.api.volume.VolumeState"/>

### VolumeState
States of a persistent volume

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved for future compatibility of new states. |
| INITIALIZED | 1 | The persistent volume is being initialized. |
| CREATED | 2 | The persistent volume is created successfully. |
| DELETED | 3 | The persistent volume is deleted. |


 

 

 



<a name="eventstream.proto"/>
<p align="right"><a href="#top">Top</a></p>

## eventstream.proto



<a name="peloton.private.eventstream.ClientUnsupported"/>

### ClientUnsupported
Error message for clients that are not expected by the server
For now, the server only expects a list of pre-defined clients
For example. Hostmgr would expect only Job manager / resource manager
to consume the task update event stream.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.eventstream.Event"/>

### Event



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| offset | [uint64](#uint64) |  | offset is the sequence id of the event |
| type | [Event.Type](#peloton.private.eventstream.Event.Type) |  |  |
| mesosTaskStatus | [.mesos.v1.TaskStatus](#peloton.private.eventstream..mesos.v1.TaskStatus) |  |  |
| pelotonTaskEvent | [.peloton.api.task.TaskEvent](#peloton.private.eventstream..peloton.api.task.TaskEvent) |  |  |






<a name="peloton.private.eventstream.InitStreamRequest"/>

### InitStreamRequest
Client need to call this to init a stream on server side


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| clientName | [string](#string) |  |  |






<a name="peloton.private.eventstream.InitStreamResponse"/>

### InitStreamResponse
InitStreamResponse pass back the streamID and the minOffset of the events
on server side


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [InitStreamResponse.Error](#peloton.private.eventstream.InitStreamResponse.Error) |  |  |
| streamID | [string](#string) |  | streamID is created by the server and will change when server restarts |
| minOffset | [uint64](#uint64) |  | min Offset of the event in the server side circular buffer |
| previousPurgeOffset | [uint64](#uint64) |  | previous purgeOffset for the client, if there is any stored on the server the client can use previousPurgeOffset as the begin offset for the next WaitForEventsRequest |






<a name="peloton.private.eventstream.InitStreamResponse.Error"/>

### InitStreamResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| clientUnsupported | [ClientUnsupported](#peloton.private.eventstream.ClientUnsupported) |  |  |






<a name="peloton.private.eventstream.InvalidPurgeOffset"/>

### InvalidPurgeOffset
Error message for incorrect purge offset


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| purgeOffset | [uint64](#uint64) |  |  |
| beginOffset | [uint64](#uint64) |  |  |






<a name="peloton.private.eventstream.InvalidStreamID"/>

### InvalidStreamID
Error message for clients that are not expected by the server


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| currentStreamID | [string](#string) |  |  |






<a name="peloton.private.eventstream.OffsetOutOfRange"/>

### OffsetOutOfRange
The intended event offset is out of the event range on the server side


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| streamID | [string](#string) |  |  |
| minOffset | [uint64](#uint64) |  |  |
| maxOffset | [uint64](#uint64) |  |  |
| offsetRequested | [uint64](#uint64) |  |  |






<a name="peloton.private.eventstream.WaitForEventsRequest"/>

### WaitForEventsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| streamID | [string](#string) |  | Current streamID |
| purgeOffset | [uint64](#uint64) |  | The offeSet that the client has processed, which can be purged on the server |
| beginOffset | [uint64](#uint64) |  | The begin offset of the intended data |
| limit | [int32](#int32) |  | The max number of events limit for current request |
| timeoutMs | [int32](#int32) |  | Timeout value |
| clientName | [string](#string) |  | Name of the client |






<a name="peloton.private.eventstream.WaitForEventsResponse"/>

### WaitForEventsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [WaitForEventsResponse.Error](#peloton.private.eventstream.WaitForEventsResponse.Error) |  |  |
| events | [Event](#peloton.private.eventstream.Event) | repeated |  |






<a name="peloton.private.eventstream.WaitForEventsResponse.Error"/>

### WaitForEventsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| outOfRange | [OffsetOutOfRange](#peloton.private.eventstream.OffsetOutOfRange) |  |  |
| clientUnsupported | [ClientUnsupported](#peloton.private.eventstream.ClientUnsupported) |  |  |
| invalidStreamID | [InvalidStreamID](#peloton.private.eventstream.InvalidStreamID) |  |  |
| invalidPurgeOffset | [InvalidPurgeOffset](#peloton.private.eventstream.InvalidPurgeOffset) |  |  |





 


<a name="peloton.private.eventstream.Event.Type"/>

### Event.Type
Describes the type of event

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN_EVENT_TYPE | 0 |  |
| MESOS_TASK_STATUS | 1 |  |
| PELOTON_TASK_EVENT | 2 |  |


 

 


<a name="peloton.private.eventstream.EventStreamService"/>

### EventStreamService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| InitStream | [InitStreamRequest](#peloton.private.eventstream.InitStreamRequest) | [InitStreamResponse](#peloton.private.eventstream.InitStreamRequest) | Client calls CreateStream to learn about information to consume the stream |
| WaitForEvents | [WaitForEventsRequest](#peloton.private.eventstream.WaitForEventsRequest) | [WaitForEventsResponse](#peloton.private.eventstream.WaitForEventsRequest) | Wait for some task events |

 



<a name="resmgr.proto"/>
<p align="right"><a href="#top">Top</a></p>

## resmgr.proto



<a name="peloton.private.resmgr.Placement"/>

### Placement
Placement describes the mapping of a list of tasks to a host
so that Job Manager can launch the tasks on the host.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [.peloton.api.peloton.TaskID](#peloton.private.resmgr..peloton.api.peloton.TaskID) | repeated | The list of tasks to be placed |
| hostname | [string](#string) |  | The name of the host where the tasks are placed |
| agentId | [.mesos.v1.AgentID](#peloton.private.resmgr..mesos.v1.AgentID) |  | The Mesos agent ID of the host where the tasks are placed |
| offerIds | [.mesos.v1.OfferID](#peloton.private.resmgr..mesos.v1.OfferID) | repeated | The list of Mesos offers of the placed tasks |
| ports | [uint32](#uint32) | repeated | The list of allocated ports which should be sufficient for all placed tasks |
| type | [TaskType](#peloton.private.resmgr.TaskType) |  | Type of the tasks in the placement. Note all tasks must belong to same type. By default the type is batch task. |






<a name="peloton.private.resmgr.Task"/>

### Task
Task describes a task instance at Resource Manager layer. Only
includes the minimal set of fields required for Resource Manager
and Placement Engine, such as resource config, constraint etc.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the task |
| id | [.peloton.api.peloton.TaskID](#peloton.private.resmgr..peloton.api.peloton.TaskID) |  | The unique ID of the task |
| jobId | [.peloton.api.peloton.JobID](#peloton.private.resmgr..peloton.api.peloton.JobID) |  | The Job ID of the task for use cases like gang scheduling |
| taskId | [.mesos.v1.TaskID](#peloton.private.resmgr..mesos.v1.TaskID) |  | The mesos task ID of the task |
| resource | [.peloton.api.task.ResourceConfig](#peloton.private.resmgr..peloton.api.task.ResourceConfig) |  | Resource config of the task |
| priority | [uint32](#uint32) |  | Priority of a task. Higher value takes priority over lower value when making scheduling decisions as well as preemption decisions |
| preemptible | [bool](#bool) |  | Whether the task is preemptible. If a task is not preemptible, then it will have to be launched using reserved resources. |
| labels | [.mesos.v1.Labels](#peloton.private.resmgr..mesos.v1.Labels) |  | List of user-defined labels for the task, these are used to enforce the constraint. These are copied from the TaskConfig. |
| constraint | [.peloton.api.task.Constraint](#peloton.private.resmgr..peloton.api.task.Constraint) |  | Constraint on the labels of the host or tasks on the host that this task should run on. This is copied from the TaskConfig. |
| type | [TaskType](#peloton.private.resmgr.TaskType) |  | Type of the Task |
| numPorts | [uint32](#uint32) |  | Number of dynamic ports |
| minInstances | [uint32](#uint32) |  | Minimum number of running instances. Value &gt; 1 indicates task is in scheduling gang of that size; task instanceID is in [0..minInstances-1]. If value &lt;= 1, task is not in scheduling gang and is scheduled singly. |
| hostname | [string](#string) |  | Hostname of the host on which the task is running on. |





 


<a name="peloton.private.resmgr.TaskType"/>

### TaskType
TaskType task type definition such as batch, service and infra agent.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | This is unknown type, this is also used in DequeueGangsRequest to indicate that we want tasks of any task type back. |
| BATCH | 1 | Normal batch task |
| STATELESS | 2 | STATELESS task which is long running and will be restarted upon failures. |
| STATEFUL | 3 | STATEFUL task which is using persistent volume and is long running |
| DAEMON | 4 | Daemon task which has one instance running on each host for infra agents like muttley, m3collector etc. |


 

 

 



<a name="resmgrsvc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## resmgrsvc.proto



<a name="peloton.private.resmgr.DequeueGangsFailure"/>

### DequeueGangsFailure



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.DequeueGangsRequest"/>

### DequeueGangsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [uint32](#uint32) |  | Max number of ready gangs to dequeue |
| timeout | [uint32](#uint32) |  | Timeout in milliseconds if no gangs are ready |
| type | [TaskType](#peloton.private.resmgr.TaskType) |  | Task Type to identify which kind of tasks need to be dequeued |






<a name="peloton.private.resmgr.DequeueGangsResponse"/>

### DequeueGangsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [DequeueGangsResponse.Error](#peloton.private.resmgr.DequeueGangsResponse.Error) |  |  |
| gangs | [Gang](#peloton.private.resmgr.Gang) | repeated | The list of gangs that have been dequeued |






<a name="peloton.private.resmgr.DequeueGangsResponse.Error"/>

### DequeueGangsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timedout | [RequestTimedout](#peloton.private.resmgr.RequestTimedout) |  |  |
| failure | [DequeueGangsFailure](#peloton.private.resmgr.DequeueGangsFailure) |  |  |






<a name="peloton.private.resmgr.EnqueueGangsFailure"/>

### EnqueueGangsFailure



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| failed | [EnqueueGangsFailure.FailedTask](#peloton.private.resmgr.EnqueueGangsFailure.FailedTask) | repeated |  |






<a name="peloton.private.resmgr.EnqueueGangsFailure.FailedTask"/>

### EnqueueGangsFailure.FailedTask



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [Task](#peloton.private.resmgr.Task) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.EnqueueGangsRequest"/>

### EnqueueGangsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resPool | [.peloton.api.peloton.ResourcePoolID](#peloton.private.resmgr..peloton.api.peloton.ResourcePoolID) |  | ResourcePool |
| gangs | [Gang](#peloton.private.resmgr.Gang) | repeated | The list of gangs to enqueue |






<a name="peloton.private.resmgr.EnqueueGangsResponse"/>

### EnqueueGangsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [EnqueueGangsResponse.Error](#peloton.private.resmgr.EnqueueGangsResponse.Error) |  |  |






<a name="peloton.private.resmgr.EnqueueGangsResponse.Error"/>

### EnqueueGangsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [ResourcePoolNotFound](#peloton.private.resmgr.ResourcePoolNotFound) |  |  |
| noPermission | [ResourcePoolNoPermission](#peloton.private.resmgr.ResourcePoolNoPermission) |  |  |
| failure | [EnqueueGangsFailure](#peloton.private.resmgr.EnqueueGangsFailure) |  |  |






<a name="peloton.private.resmgr.Gang"/>

### Gang



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [Task](#peloton.private.resmgr.Task) | repeated | List of tasks to be scheduled together |






<a name="peloton.private.resmgr.GetActiveTasksRequest"/>

### GetActiveTasksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobID | [string](#string) |  | optional jobID to filter out tasks |
| respoolID | [string](#string) |  | optional respoolID to filter out tasks |






<a name="peloton.private.resmgr.GetActiveTasksResponse"/>

### GetActiveTasksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetActiveTasksResponse.Error](#peloton.private.resmgr.GetActiveTasksResponse.Error) |  |  |
| taskStatesMap | [GetActiveTasksResponse.TaskStatesMapEntry](#peloton.private.resmgr.GetActiveTasksResponse.TaskStatesMapEntry) | repeated | This will return a map from task id to state. |






<a name="peloton.private.resmgr.GetActiveTasksResponse.Error"/>

### GetActiveTasksResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.GetActiveTasksResponse.TaskStatesMapEntry"/>

### GetActiveTasksResponse.TaskStatesMapEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [string](#string) |  |  |






<a name="peloton.private.resmgr.GetPlacementsFailure"/>

### GetPlacementsFailure



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.GetPlacementsRequest"/>

### GetPlacementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [uint32](#uint32) |  | Max number of placements to retrieve |
| timeout | [uint32](#uint32) |  | Timeout in milliseconds if no placements |






<a name="peloton.private.resmgr.GetPlacementsResponse"/>

### GetPlacementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetPlacementsResponse.Error](#peloton.private.resmgr.GetPlacementsResponse.Error) |  |  |
| placements | [Placement](#peloton.private.resmgr.Placement) | repeated | List of task placements to return |






<a name="peloton.private.resmgr.GetPlacementsResponse.Error"/>

### GetPlacementsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| failure | [GetPlacementsFailure](#peloton.private.resmgr.GetPlacementsFailure) |  |  |






<a name="peloton.private.resmgr.GetPreemptibleTasksFailure"/>

### GetPreemptibleTasksFailure



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.GetPreemptibleTasksRequest"/>

### GetPreemptibleTasksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [uint32](#uint32) |  | Max number of running tasks to dequeue |
| timeout | [uint32](#uint32) |  | Timeout in milliseconds if no tasks are ready |






<a name="peloton.private.resmgr.GetPreemptibleTasksResponse"/>

### GetPreemptibleTasksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetPreemptibleTasksResponse.Error](#peloton.private.resmgr.GetPreemptibleTasksResponse.Error) |  |  |
| tasks | [Task](#peloton.private.resmgr.Task) | repeated | The list of tasks that have been dequeued |






<a name="peloton.private.resmgr.GetPreemptibleTasksResponse.Error"/>

### GetPreemptibleTasksResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| timedout | [RequestTimedout](#peloton.private.resmgr.RequestTimedout) |  |  |
| failure | [GetPreemptibleTasksFailure](#peloton.private.resmgr.GetPreemptibleTasksFailure) |  |  |






<a name="peloton.private.resmgr.GetTasksByHostsRequest"/>

### GetTasksByHostsRequest
GetTasksByHostsRequest will always returns the currently running tasks


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostnames | [string](#string) | repeated |  |
| type | [TaskType](#peloton.private.resmgr.TaskType) |  | Task Type to identify which kind of tasks need to be dequeued, if this is left out all tasks wil be returned. |






<a name="peloton.private.resmgr.GetTasksByHostsResponse"/>

### GetTasksByHostsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [GetTasksByHostsResponse.Error](#peloton.private.resmgr.GetTasksByHostsResponse.Error) |  |  |
| hostTasksMap | [GetTasksByHostsResponse.HostTasksMapEntry](#peloton.private.resmgr.GetTasksByHostsResponse.HostTasksMapEntry) | repeated | This will return a map from hostname to a list of tasks running on the host. |






<a name="peloton.private.resmgr.GetTasksByHostsResponse.Error"/>

### GetTasksByHostsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.GetTasksByHostsResponse.HostTasksMapEntry"/>

### GetTasksByHostsResponse.HostTasksMapEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [TaskList](#peloton.private.resmgr.TaskList) |  |  |






<a name="peloton.private.resmgr.KillTasksError"/>

### KillTasksError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [.peloton.api.peloton.TaskID](#peloton.private.resmgr..peloton.api.peloton.TaskID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.KillTasksRequest"/>

### KillTasksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [.peloton.api.peloton.TaskID](#peloton.private.resmgr..peloton.api.peloton.TaskID) | repeated | Peloton Task Ids for |






<a name="peloton.private.resmgr.KillTasksResponse"/>

### KillTasksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [KillTasksResponse.Error](#peloton.private.resmgr.KillTasksResponse.Error) | repeated |  |






<a name="peloton.private.resmgr.KillTasksResponse.Error"/>

### KillTasksResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| notFound | [TasksNotFound](#peloton.private.resmgr.TasksNotFound) |  |  |
| killError | [KillTasksError](#peloton.private.resmgr.KillTasksError) |  |  |






<a name="peloton.private.resmgr.NotifyTaskUpdatesError"/>

### NotifyTaskUpdatesError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.NotifyTaskUpdatesRequest"/>

### NotifyTaskUpdatesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| events | [.peloton.private.eventstream.Event](#peloton.private.resmgr..peloton.private.eventstream.Event) | repeated |  |






<a name="peloton.private.resmgr.NotifyTaskUpdatesResponse"/>

### NotifyTaskUpdatesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [NotifyTaskUpdatesResponse.Error](#peloton.private.resmgr.NotifyTaskUpdatesResponse.Error) |  |  |
| purgeOffset | [uint64](#uint64) |  |  |






<a name="peloton.private.resmgr.NotifyTaskUpdatesResponse.Error"/>

### NotifyTaskUpdatesResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [NotifyTaskUpdatesError](#peloton.private.resmgr.NotifyTaskUpdatesError) |  |  |






<a name="peloton.private.resmgr.RequestTimedout"/>

### RequestTimedout



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.ResourcePoolNoPermission"/>

### ResourcePoolNoPermission



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.private.resmgr..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.ResourcePoolNotFound"/>

### ResourcePoolNotFound



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.private.resmgr..peloton.api.peloton.ResourcePoolID) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.SetPlacementsFailure"/>

### SetPlacementsFailure



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| failed | [SetPlacementsFailure.FailedPlacement](#peloton.private.resmgr.SetPlacementsFailure.FailedPlacement) | repeated |  |






<a name="peloton.private.resmgr.SetPlacementsFailure.FailedPlacement"/>

### SetPlacementsFailure.FailedPlacement



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| placement | [Placement](#peloton.private.resmgr.Placement) |  |  |
| message | [string](#string) |  |  |






<a name="peloton.private.resmgr.SetPlacementsRequest"/>

### SetPlacementsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| placements | [Placement](#peloton.private.resmgr.Placement) | repeated | List of task placements to set |






<a name="peloton.private.resmgr.SetPlacementsResponse"/>

### SetPlacementsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [SetPlacementsResponse.Error](#peloton.private.resmgr.SetPlacementsResponse.Error) |  |  |






<a name="peloton.private.resmgr.SetPlacementsResponse.Error"/>

### SetPlacementsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| failure | [SetPlacementsFailure](#peloton.private.resmgr.SetPlacementsFailure) |  |  |






<a name="peloton.private.resmgr.TaskList"/>

### TaskList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [Task](#peloton.private.resmgr.Task) | repeated |  |






<a name="peloton.private.resmgr.TasksNotFound"/>

### TasksNotFound



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| task | [.peloton.api.peloton.TaskID](#peloton.private.resmgr..peloton.api.peloton.TaskID) |  |  |
| message | [string](#string) |  |  |





 

 

 


<a name="peloton.private.resmgr.ResourceManagerService"/>

### ResourceManagerService
ResourceManagerService describes the internal interface of
Resource Manager to other Peloton applications such as Job Manager
and Placement Engine. This includes the EnqueueGangs and GetPlacements
APIs called by Job Manager, and DequeueGangs and SetPlacements APIs
called by Placement Engine.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| EnqueueGangs | [EnqueueGangsRequest](#peloton.private.resmgr.EnqueueGangsRequest) | [EnqueueGangsResponse](#peloton.private.resmgr.EnqueueGangsRequest) | Enqueue a list of Gangs, each of which is a list of one or more tasks, to a given leaf resource pool for scheduling. The Gangs will be in PENDING state first and then transit to READY state when the resource pool has available resources. This method will be called by Job Manager when a new job is created or new Gangs are added. If any Gangs fail to enqueue, Job Manager should retry those failed Gangs. |
| DequeueGangs | [DequeueGangsRequest](#peloton.private.resmgr.DequeueGangsRequest) | [DequeueGangsResponse](#peloton.private.resmgr.DequeueGangsRequest) | Dequeue a list of Gangs, each comprised of tasks that are in READY state for placement. The tasks will transit from READY to PLACING state after the return of this method. This method will be called by Placement Engine to retrieve a list of gangs for computing placement. If tasks are in PLACING state for too long in case of Placement Engine failures, the tasks will be timed out and transit back to READY state. |
| SetPlacements | [SetPlacementsRequest](#peloton.private.resmgr.SetPlacementsRequest) | [SetPlacementsResponse](#peloton.private.resmgr.SetPlacementsRequest) | Set the placement information for a list of tasks. The tasks will transit from PLACING to PLACED state after this call. This method will be called by Placement Engine after it computes the placement decision for those tasks. |
| GetPlacements | [GetPlacementsRequest](#peloton.private.resmgr.GetPlacementsRequest) | [GetPlacementsResponse](#peloton.private.resmgr.GetPlacementsRequest) | Get the placement information for a list of tasks. The tasks will transit from PLACED to LAUNCHING state after this call. This method is called by Job Manager to launch the tasks on Mesos. If the tasks are in LAUNCHING state for too long without transiting to RUNNING state, the tasks will be timedout and transit back to PLACED state. |
| NotifyTaskUpdates | [NotifyTaskUpdatesRequest](#peloton.private.resmgr.NotifyTaskUpdatesRequest) | [NotifyTaskUpdatesResponse](#peloton.private.resmgr.NotifyTaskUpdatesRequest) | Notifies task status updates to resource manager. This will be called by Host manager to notify resource manager on task status updates. |
| GetTasksByHosts | [GetTasksByHostsRequest](#peloton.private.resmgr.GetTasksByHostsRequest) | [GetTasksByHostsResponse](#peloton.private.resmgr.GetTasksByHostsRequest) | Get the list of Tasks running on the the list of host provided. This information is needed from the placement engines to find out which tasks are running on which hosts so the placement engine can place tasks taking this information into account. |
| GetActiveTasks | [GetActiveTasksRequest](#peloton.private.resmgr.GetActiveTasksRequest) | [GetActiveTasksResponse](#peloton.private.resmgr.GetActiveTasksRequest) | Get task to state map. This information is helpful for debug purpose. |
| KillTasks | [KillTasksRequest](#peloton.private.resmgr.KillTasksRequest) | [KillTasksResponse](#peloton.private.resmgr.KillTasksRequest) | Kill Tasks kills/Delete the tasks in Resource Manager |
| GetPreemptibleTasks | [GetPreemptibleTasksRequest](#peloton.private.resmgr.GetPreemptibleTasksRequest) | [GetPreemptibleTasksResponse](#peloton.private.resmgr.GetPreemptibleTasksRequest) | Get the list of tasks to preempt. The tasks will transition from RUNNING to PREEMPTING state after the return of this method. This method will be called by the job manager to kill the tasks and re-enqueue them. |

 



<a name="job_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## job_svc.proto



<a name="peloton.api.job.svc.CreateJobRequest"/>

### CreateJobRequest
Request message for JobService.CreateJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job.svc..peloton.api.peloton.JobID) |  | The unique job UUID specified by the client. This can be used by the client to re-create a failed job without the side-effect of creating duplicated jobs. If unset, the server will create a new UUID for the job for each invocation. |
| pool | [.peloton.api.respool.ResourcePoolPath](#peloton.api.job.svc..peloton.api.respool.ResourcePoolPath) |  | The resource pool under which the job should be created. The scheduling of all tasks in the job will be subject to the resource availablity of the resource pool. |
| config | [.peloton.api.job.JobConfig](#peloton.api.job.svc..peloton.api.job.JobConfig) |  | The detailed configuration of the job to be created. |






<a name="peloton.api.job.svc.CreateJobResponse"/>

### CreateJobResponse
Response message for JobService.CreateJob method.

Return errors:
ALREADY_EXISTS:    if the job ID already exists.o
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.job.svc..peloton.api.peloton.JobID) |  | The job ID of the newly created job. Will be the same as the one in CreateJobRequest if provided. Otherwise, a new job ID will be generated by the server. |






<a name="peloton.api.job.svc.DeleteJobRequest"/>

### DeleteJobRequest
Request message for JobService.DeleteJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job.svc..peloton.api.peloton.JobID) |  | The job ID to be deleted. |






<a name="peloton.api.job.svc.DeleteJobResponse"/>

### DeleteJobResponse
Response message for JobService.DeleteJob method.

Return errors:
NOT_FOUND:  if the job is not found in Peloton.






<a name="peloton.api.job.svc.GetJobRequest"/>

### GetJobRequest
Request message for JobService.GetJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job.svc..peloton.api.peloton.JobID) |  | The job ID to look up the job. |






<a name="peloton.api.job.svc.GetJobResponse"/>

### GetJobResponse
Response message for JobService.GetJob method.

Return errors:
NOT_FOUND:  if the job is not found in Peloton.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.job.JobConfig](#peloton.api.job.svc..peloton.api.job.JobConfig) |  | The job configuration of the matching job. |






<a name="peloton.api.job.svc.QueryJobsRequest"/>

### QueryJobsRequest
Request message for JobService.QueryJobs method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| spec | [.peloton.api.job.QuerySpec](#peloton.api.job.svc..peloton.api.job.QuerySpec) |  | The spec of query criteria for the jobs. |
| pagination | [.peloton.api.query.PaginationSpec](#peloton.api.job.svc..peloton.api.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |






<a name="peloton.api.job.svc.QueryJobsResponse"/>

### QueryJobsResponse
Response message for JobService.QueryJobs method.

Return errors:
INVALID_ARGUMENT:  if the resource pool path or job states are invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [.peloton.api.job.JobInfo](#peloton.api.job.svc..peloton.api.job.JobInfo) | repeated | List of jobs that match the job query criteria. |
| pagination | [.peloton.api.query.Pagination](#peloton.api.job.svc..peloton.api.query.Pagination) |  | Pagination result of the job query. |






<a name="peloton.api.job.svc.UpdateJobRequest"/>

### UpdateJobRequest
Request message for JobService.UpdateJob method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.JobID](#peloton.api.job.svc..peloton.api.peloton.JobID) |  | The job ID to be updated. |
| config | [.peloton.api.job.JobConfig](#peloton.api.job.svc..peloton.api.job.JobConfig) |  | The new job config to be applied to the job. |






<a name="peloton.api.job.svc.UpdateJobResponse"/>

### UpdateJobResponse
Response message for JobService.UpdateJob method.

Return errors:
INVALID_ARGUMENT:  if the job ID or job config is invalid.
NOT_FOUND:         if the job ID is not found.





 

 

 


<a name="peloton.api.job.svc.JobService"/>

### JobService
Job service defines the job related methods such as create, get,
query and kill jobs.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateJob | [CreateJobRequest](#peloton.api.job.svc.CreateJobRequest) | [CreateJobResponse](#peloton.api.job.svc.CreateJobRequest) | Create a job entity for a given config. |
| GetJob | [GetJobRequest](#peloton.api.job.svc.GetJobRequest) | [GetJobResponse](#peloton.api.job.svc.GetJobRequest) | Get the config of a job entity. |
| QueryJobs | [QueryJobsRequest](#peloton.api.job.svc.QueryJobsRequest) | [QueryJobsResponse](#peloton.api.job.svc.QueryJobsRequest) | Query the jobs that match a list of labels. |
| DeleteJob | [DeleteJobRequest](#peloton.api.job.svc.DeleteJobRequest) | [DeleteJobResponse](#peloton.api.job.svc.DeleteJobRequest) | Delete a job and stop all related tasks. |
| UpdateJob | [UpdateJobRequest](#peloton.api.job.svc.UpdateJobRequest) | [UpdateJobResponse](#peloton.api.job.svc.UpdateJobRequest) | Update a job entity with a new config. This is a temporary API for updating batch jobs. It only supports adding new instances to an existing job. It will be deprecated when the UpgradeService API is implemented. |

 



<a name="respool_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## respool_svc.proto



<a name="peloton.api.respool.CreateResourcePoolRequest"/>

### CreateResourcePoolRequest
Request message for ResourcePoolService.CreateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The unique resource pool UUID specified by the client. This can be used by the client to re-create a failed resource pool without the side-effect of creating duplicated resource pool. If unset, the server will create a new UUID for the resource pool. |
| config | [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig) |  | The detailed configuration of the resource pool be to created. |






<a name="peloton.api.respool.CreateResourcePoolResponse"/>

### CreateResourcePoolResponse
Response message for ResourcePoolService.CreateResourcePool method.

Return errors:
ALREADY_EXISTS:   if the resource pool already exists.
INVALID_ARGUMENT: if the resource pool config is invalid.o


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The ID of the newly created resource pool. |






<a name="peloton.api.respool.DeleteResourcePoolRequest"/>

### DeleteResourcePoolRequest
Request message for ResourcePoolService.DeleteResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The ID of the resource pool to be deleted. |






<a name="peloton.api.respool.DeleteResourcePoolResponse"/>

### DeleteResourcePoolResponse
Response message for ResourcePoolService.DeleteResourcePool method.

Return errors:
NOT_FOUND:        if the resource pool is not found.
INVALID_ARGUMENT: if the resource pool is not leaf node.
FAILED_PRECONDITION:  if the resource pool is busy.
INTERNAL:         if the resource pool fail to delete for internal errors.






<a name="peloton.api.respool.GetResourcePoolRequest"/>

### GetResourcePoolRequest
Request message for ResourcePoolService.GetResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The ID of the resource pool to get the detailed information. |
| includeChildPools | [bool](#bool) |  | Whether or not to include the resource pool info of the direct children |






<a name="peloton.api.respool.GetResourcePoolResponse"/>

### GetResourcePoolResponse
Response message for ResourcePoolService.GetResourcePool method.

Return errors:
NOT_FOUND:   if the resource pool is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resPool | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) |  | The detailed information of the resource pool. |
| childResPools | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) | repeated | The list of child resource pools. |






<a name="peloton.api.respool.LookupResourcePoolIDRequest"/>

### LookupResourcePoolIDRequest
Request message for ResourcePoolService.LookupResourcePoolID method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [ResourcePoolPath](#peloton.api.respool.ResourcePoolPath) |  | The resource pool path to look up the resource pool ID. |






<a name="peloton.api.respool.LookupResourcePoolIDResponse"/>

### LookupResourcePoolIDResponse
Response message for ResourcePoolService.LookupResourcePoolID method.

Return errors:
NOT_FOUND:        if the resource pool is not found.
INVALID_ARGUMENT: if the resource pool path is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The resource pool ID for the given resource pool path. |






<a name="peloton.api.respool.QueryResourcePoolsRequest"/>

### QueryResourcePoolsRequest
Request message for ResourcePoolService.QueryResourcePools method.


TODO Filters






<a name="peloton.api.respool.QueryResourcePoolsResponse"/>

### QueryResourcePoolsResponse
Response message for ResourcePoolService.QueryResourcePools method.

Return errors:


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resPools | [ResourcePoolInfo](#peloton.api.respool.ResourcePoolInfo) | repeated |  |






<a name="peloton.api.respool.UpdateResourcePoolRequest"/>

### UpdateResourcePoolRequest
Request message for ResourcePoolService.UpdateResourcePool method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.ResourcePoolID](#peloton.api.respool..peloton.api.peloton.ResourcePoolID) |  | The ID of the resource pool to update the configuration. |
| config | [ResourcePoolConfig](#peloton.api.respool.ResourcePoolConfig) |  | The configuration of the resource pool to be updated. |






<a name="peloton.api.respool.UpdateResourcePoolResponse"/>

### UpdateResourcePoolResponse
Response message for ResourcePoolService.UpdateResourcePool method.

Return errors:
NOT_FOUND:   if the resource pool is not found.





 

 

 


<a name="peloton.api.respool.ResourcePoolService"/>

### ResourcePoolService
ResourcePoolService defines the resource pool related methods
such as create, get, delete and upgrade resource pools.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateResourcePool | [CreateResourcePoolRequest](#peloton.api.respool.CreateResourcePoolRequest) | [CreateResourcePoolResponse](#peloton.api.respool.CreateResourcePoolRequest) | Create a resource pool entity for a given config |
| GetResourcePool | [GetResourcePoolRequest](#peloton.api.respool.GetResourcePoolRequest) | [GetResourcePoolResponse](#peloton.api.respool.GetResourcePoolRequest) | Get the resource pool entity |
| DeleteResourcePool | [DeleteResourcePoolRequest](#peloton.api.respool.DeleteResourcePoolRequest) | [DeleteResourcePoolResponse](#peloton.api.respool.DeleteResourcePoolRequest) | Delete a resource pool entity |
| UpdateResourcePool | [UpdateResourcePoolRequest](#peloton.api.respool.UpdateResourcePoolRequest) | [UpdateResourcePoolResponse](#peloton.api.respool.UpdateResourcePoolRequest) | Modify a resource pool entity |
| LookupResourcePoolID | [LookupResourcePoolIDRequest](#peloton.api.respool.LookupResourcePoolIDRequest) | [LookupResourcePoolIDResponse](#peloton.api.respool.LookupResourcePoolIDRequest) | Lookup the resource pool ID for a given resource pool path |
| QueryResourcePools | [QueryResourcePoolsRequest](#peloton.api.respool.QueryResourcePoolsRequest) | [QueryResourcePoolsResponse](#peloton.api.respool.QueryResourcePoolsRequest) | Query the resource pools. |

 



<a name="task_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## task_svc.proto



<a name="peloton.api.task.svc.BrowseSandboxRequest"/>

### BrowseSandboxRequest
Request message for TaskService.BrowseSandbox method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the task to browse the sandbox. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to browse the sandbox. |






<a name="peloton.api.task.svc.BrowseSandboxResponse"/>

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






<a name="peloton.api.task.svc.GetEventsRequest"/>

### GetEventsRequest
Request message for TaskService.GetEvents method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the task |
| instanceId | [uint32](#uint32) |  | The instance ID of the task |






<a name="peloton.api.task.svc.GetEventsResponse"/>

### GetEventsResponse
Response message for TaskService.GetEvents method.

Return errors:
INTERNAL:      if failed to get task events for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [GetEventsResponse.Events](#peloton.api.task.svc.GetEventsResponse.Events) | repeated |  |
| error | [GetEventsResponse.Error](#peloton.api.task.svc.GetEventsResponse.Error) |  |  |






<a name="peloton.api.task.svc.GetEventsResponse.Error"/>

### GetEventsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| eventError | [TaskEventsError](#peloton.api.task.svc.TaskEventsError) |  |  |






<a name="peloton.api.task.svc.GetEventsResponse.Events"/>

### GetEventsResponse.Events



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| event | [.peloton.api.task.TaskEvent](#peloton.api.task.svc..peloton.api.task.TaskEvent) | repeated |  |






<a name="peloton.api.task.svc.GetTaskRequest"/>

### GetTaskRequest
Request message for TaskService.GetTask method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the task to get. |
| instanceId | [uint32](#uint32) |  | The instance ID of the task to get. |






<a name="peloton.api.task.svc.GetTaskResponse"/>

### GetTaskResponse
Response message for TaskService.GetTask method.

Return errors:
NOT_FOUND:     if the job or task not found.
OUT_OF_RANGE:  if the instance ID is out of range.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.task.TaskInfo](#peloton.api.task.svc..peloton.api.task.TaskInfo) |  | The task info of the task. |






<a name="peloton.api.task.svc.ListTasksRequest"/>

### ListTasksRequest
Request message for TaskService.ListTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the tasks to list. |
| range | [.peloton.api.task.InstanceRange](#peloton.api.task.svc..peloton.api.task.InstanceRange) |  | The instance ID range of the tasks to list. |






<a name="peloton.api.task.svc.ListTasksResponse"/>

### ListTasksResponse
Response message for TaskService.GetTask method.

Return errors:
NOT_FOUND:  if the job ID is not found.
OUT_OF_RANGE:  if the instance IDs are out of range.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [ListTasksResponse.TasksEntry](#peloton.api.task.svc.ListTasksResponse.TasksEntry) | repeated | The map of instance ID to task info for all matching tasks. |






<a name="peloton.api.task.svc.ListTasksResponse.TasksEntry"/>

### ListTasksResponse.TasksEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [uint32](#uint32) |  |  |
| value | [.peloton.api.task.TaskInfo](#peloton.api.task.svc..peloton.api.task.TaskInfo) |  |  |






<a name="peloton.api.task.svc.QueryTasksRequest"/>

### QueryTasksRequest
Request message for TaskService.QueryTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the tasks to query. |
| spec | [.peloton.api.task.QuerySpec](#peloton.api.task.svc..peloton.api.task.QuerySpec) |  | The spec of query criteria for the tasks. |
| pagination | [.peloton.api.query.PaginationSpec](#peloton.api.task.svc..peloton.api.query.PaginationSpec) |  | The spec of how to do pagination for the query results. |






<a name="peloton.api.task.svc.QueryTasksResponse"/>

### QueryTasksResponse
Response message for TaskService.QueryTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found.
INTERNAL:      if fail to query the tasks for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| records | [.peloton.api.task.TaskInfo](#peloton.api.task.svc..peloton.api.task.TaskInfo) | repeated | List of tasks that match the task query criteria. |
| pagination | [.peloton.api.query.Pagination](#peloton.api.task.svc..peloton.api.query.Pagination) |  | Pagination result of the task query. |






<a name="peloton.api.task.svc.RestartTasksRequest"/>

### RestartTasksRequest
Request message for TaskService.RestartTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the tasks to restart. |
| ranges | [.peloton.api.task.InstanceRange](#peloton.api.task.svc..peloton.api.task.InstanceRange) | repeated | The instance ID ranges of the tasks to restart. |






<a name="peloton.api.task.svc.RestartTasksResponse"/>

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






<a name="peloton.api.task.svc.StartTasksRequest"/>

### StartTasksRequest
Request message for TaskService.StartTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the tasks to start. |
| ranges | [.peloton.api.task.InstanceRange](#peloton.api.task.svc..peloton.api.task.InstanceRange) | repeated | The instance ID ranges of the tasks to start. |






<a name="peloton.api.task.svc.StartTasksResponse"/>

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






<a name="peloton.api.task.svc.StopTasksRequest"/>

### StopTasksRequest
Request message for TaskService.StopTasks method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.task.svc..peloton.api.peloton.JobID) |  | The job ID of the tasks to stop. |
| ranges | [.peloton.api.task.InstanceRange](#peloton.api.task.svc..peloton.api.task.InstanceRange) | repeated | The instance ID ranges of the tasks to stop. |






<a name="peloton.api.task.svc.StopTasksResponse"/>

### StopTasksResponse
Response message for TaskService.StopTasks method.

Return errors:
NOT_FOUND:     if the job ID is not found in Peloton.
OUT_OF_RANGE:  if the instance IDs are out of range.
INTERNAL:      if the tasks fail to stop for internal errors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stopped | [uint32](#uint32) | repeated | The set of instance IDs that have been stopped. |
| failed | [uint32](#uint32) | repeated | The set of instance IDs that are failed to stop. |






<a name="peloton.api.task.svc.TaskEventsError"/>

### TaskEventsError



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |





 

 

 


<a name="peloton.api.task.svc.TaskService"/>

### TaskService
Task service defines the task related methods such as get, list,
start, stop and restart tasks.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetTask | [GetTaskRequest](#peloton.api.task.svc.GetTaskRequest) | [GetTaskResponse](#peloton.api.task.svc.GetTaskRequest) | Get the info of a task in job. |
| ListTasks | [ListTasksRequest](#peloton.api.task.svc.ListTasksRequest) | [ListTasksResponse](#peloton.api.task.svc.ListTasksRequest) | List a set of tasks in a job for a given range of instance IDs. |
| StartTasks | [StartTasksRequest](#peloton.api.task.svc.StartTasksRequest) | [StartTasksResponse](#peloton.api.task.svc.StartTasksRequest) | Start a set of tasks for a job. Will be no-op for tasks that are currently running. |
| StopTasks | [StopTasksRequest](#peloton.api.task.svc.StopTasksRequest) | [StopTasksResponse](#peloton.api.task.svc.StopTasksRequest) | Stop a set of tasks for a job. Will be no-op for tasks that are currently stopped. |
| RestartTasks | [RestartTasksRequest](#peloton.api.task.svc.RestartTasksRequest) | [RestartTasksResponse](#peloton.api.task.svc.RestartTasksRequest) | Restart a set of tasks for a job. Will start tasks that are currently stopped. |
| QueryTasks | [QueryTasksRequest](#peloton.api.task.svc.QueryTasksRequest) | [QueryTasksResponse](#peloton.api.task.svc.QueryTasksRequest) | Query task info in a job, using a set of filters. |
| BrowseSandbox | [BrowseSandboxRequest](#peloton.api.task.svc.BrowseSandboxRequest) | [BrowseSandboxResponse](#peloton.api.task.svc.BrowseSandboxRequest) | BrowseSandbox returns list of file paths inside sandbox. The client can use the Mesos Agent HTTP endpoints to read and download the files. http://mesos.apache.org/documentation/latest/endpoints |
| GetEvents | [GetEventsRequest](#peloton.api.task.svc.GetEventsRequest) | [GetEventsResponse](#peloton.api.task.svc.GetEventsRequest) | GetEvents returns task events in for a instance in a job. An instance may have multiple Mesos tasks and GetEventsResponse will contain a map for each mesos task and its corresponding list of events. This is an experimental API and is subject to change |

 



<a name="update_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## update_svc.proto



<a name="peloton.api.update.svc.AbortUpdateRequest"/>

### AbortUpdateRequest
Request message for UpdateService.AbortUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  | Identifier of the update to be aborted. |
| softAbort | [bool](#bool) |  |  |






<a name="peloton.api.update.svc.AbortUpdateResponse"/>

### AbortUpdateResponse
Response message for UpdateService.AbortUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.






<a name="peloton.api.update.svc.CreateUpdateRequest"/>

### CreateUpdateRequest
Request message for UpdateService.CreateUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.update.svc..peloton.api.peloton.JobID) |  | Entity id of the job to be updated. |
| jobConfig | [.peloton.api.job.JobConfig](#peloton.api.update.svc..peloton.api.job.JobConfig) |  | New configuration of the job to be updated. The new job config will be applied to all instances without violating the job SLA. |
| updateConfig | [.peloton.api.update.UpdateConfig](#peloton.api.update.svc..peloton.api.update.UpdateConfig) |  | The options of the update. |






<a name="peloton.api.update.svc.CreateUpdateResponse"/>

### CreateUpdateResponse
Response message for UpdateService.CreateUpdate method.
Returns errors:
NOT_FOUND:      if the job with the provided identifier is not found.
ALREADY_EXISTS: if another update for the same job is running.
INVALID_ARGUMENTS: if the provided job config or update config is invalid.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  | Identifier for the newly created update. |






<a name="peloton.api.update.svc.GetUpdateRequest"/>

### GetUpdateRequest
Request message for UpdateService.GetUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  |  |






<a name="peloton.api.update.svc.GetUpdateResponse"/>

### GetUpdateResponse
Response message for UpdateService.GetUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.update.UpdateInfo](#peloton.api.update.svc..peloton.api.update.UpdateInfo) |  | Update update information. |






<a name="peloton.api.update.svc.ListUpdatesRequest"/>

### ListUpdatesRequest
Request message for UpdateService.ListUpdates method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [int32](#int32) |  | Number of updates to return. |






<a name="peloton.api.update.svc.ListUpdatesResponse"/>

### ListUpdatesResponse
Response message for UpdateService.ListUpdates method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateInfo | [.peloton.api.update.UpdateInfo](#peloton.api.update.svc..peloton.api.update.UpdateInfo) | repeated |  |






<a name="peloton.api.update.svc.PauseUpdateRequest"/>

### PauseUpdateRequest
Request message for UpdateService.PauseUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  | Identifier of the update to be paused. |






<a name="peloton.api.update.svc.PauseUpdateResponse"/>

### PauseUpdateResponse
Response message for UpdateService.PauseUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be paused.






<a name="peloton.api.update.svc.ResumeUpdateRequest"/>

### ResumeUpdateRequest
Request message for UpdateService.ResumeUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  | Identifier of the update to be resumed. |






<a name="peloton.api.update.svc.ResumeUpdateResponse"/>

### ResumeUpdateResponse
Response message for UpdateService.ResumeUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.






<a name="peloton.api.update.svc.RollbackUpdateRequest"/>

### RollbackUpdateRequest
Request message for UpdateService.RollbackUpdate method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| updateId | [.peloton.api.update.UpdateID](#peloton.api.update.svc..peloton.api.update.UpdateID) |  | Identifier of the update to be rolled back. |






<a name="peloton.api.update.svc.RollbackUpdateResponse"/>

### RollbackUpdateResponse
Response message for UpdateService.RollbackUpdate method.
Returns errors:
NOT_FOUND: if the update with the provided identifier is not found.
UNAVAILABLE: if the update is in a state which cannot be resumed.





 

 

 


<a name="peloton.api.update.svc.UpdateService"/>

### UpdateService
Update service interface
EXPERIMENTAL: This API is not yet stable.

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateUpdate | [CreateUpdateRequest](#peloton.api.update.svc.CreateUpdateRequest) | [CreateUpdateResponse](#peloton.api.update.svc.CreateUpdateRequest) | Create a new update for a job. Only one update can exist for a job at a given time. |
| GetUpdate | [GetUpdateRequest](#peloton.api.update.svc.GetUpdateRequest) | [GetUpdateResponse](#peloton.api.update.svc.GetUpdateRequest) | Get the status of an update. |
| ListUpdates | [ListUpdatesRequest](#peloton.api.update.svc.ListUpdatesRequest) | [ListUpdatesResponse](#peloton.api.update.svc.ListUpdatesRequest) | List all updates that are currently running. |
| PauseUpdate | [PauseUpdateRequest](#peloton.api.update.svc.PauseUpdateRequest) | [PauseUpdateResponse](#peloton.api.update.svc.PauseUpdateRequest) | Pause an update. |
| ResumeUpdate | [ResumeUpdateRequest](#peloton.api.update.svc.ResumeUpdateRequest) | [ResumeUpdateResponse](#peloton.api.update.svc.ResumeUpdateRequest) | Resume a paused update. |
| RollbackUpdate | [RollbackUpdateRequest](#peloton.api.update.svc.RollbackUpdateRequest) | [RollbackUpdateResponse](#peloton.api.update.svc.RollbackUpdateRequest) | Rollback an update. |
| AbortUpdate | [AbortUpdateRequest](#peloton.api.update.svc.AbortUpdateRequest) | [AbortUpdateResponse](#peloton.api.update.svc.AbortUpdateRequest) | Abort an update. |

 



<a name="volume_svc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## volume_svc.proto



<a name="peloton.api.volume.svc.DeleteVolumeRequest"/>

### DeleteVolumeRequest
Request message for VolumeService.Delete method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.VolumeID](#peloton.api.volume.svc..peloton.api.peloton.VolumeID) |  | volume id for the delete request. |






<a name="peloton.api.volume.svc.DeleteVolumeResponse"/>

### DeleteVolumeResponse
Response message for VolumeService.Delete method.

Return errors:
NOT_FOUND:         if the volume is not found.






<a name="peloton.api.volume.svc.GetVolumeRequest"/>

### GetVolumeRequest
Request message for VolumeService.Get method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.VolumeID](#peloton.api.volume.svc..peloton.api.peloton.VolumeID) |  | the volume id. |






<a name="peloton.api.volume.svc.GetVolumeResponse"/>

### GetVolumeResponse
Response message for VolumeService.Get method.

Return errors:
NOT_FOUND:         if the volume is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [.peloton.api.volume.PersistentVolumeInfo](#peloton.api.volume.svc..peloton.api.volume.PersistentVolumeInfo) |  | volume info result. |






<a name="peloton.api.volume.svc.ListVolumesRequest"/>

### ListVolumesRequest
Request message for VolumeService.List method.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| jobId | [.peloton.api.peloton.JobID](#peloton.api.volume.svc..peloton.api.peloton.JobID) |  | job ID for the volumes. |






<a name="peloton.api.volume.svc.ListVolumesResponse"/>

### ListVolumesResponse
Response message for VolumeService.List method.

Return errors:
NOT_FOUND:         if the volume is not found.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [ListVolumesResponse.VolumesEntry](#peloton.api.volume.svc.ListVolumesResponse.VolumesEntry) | repeated | volumes result map from volume uuid to volume info. |






<a name="peloton.api.volume.svc.ListVolumesResponse.VolumesEntry"/>

### ListVolumesResponse.VolumesEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [.peloton.api.volume.PersistentVolumeInfo](#peloton.api.volume.svc..peloton.api.volume.PersistentVolumeInfo) |  |  |





 

 

 


<a name="peloton.api.volume.svc.VolumeService"/>

### VolumeService
Volume Manager service interface

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListVolumes | [ListVolumesRequest](#peloton.api.volume.svc.ListVolumesRequest) | [ListVolumesResponse](#peloton.api.volume.svc.ListVolumesRequest) | List associated volumes for given job. |
| GetVolume | [GetVolumeRequest](#peloton.api.volume.svc.GetVolumeRequest) | [GetVolumeResponse](#peloton.api.volume.svc.GetVolumeRequest) | Get volume data. |
| DeleteVolume | [DeleteVolumeRequest](#peloton.api.volume.svc.DeleteVolumeRequest) | [DeleteVolumeResponse](#peloton.api.volume.svc.DeleteVolumeRequest) | Delete a persistent volume. |

 



<a name="hostsvc.proto"/>
<p align="right"><a href="#top">Top</a></p>

## hostsvc.proto



<a name="peloton.private.hostmgr.hostsvc.AcquireHostOffersFailure"/>

### AcquireHostOffersFailure
Error when AcquireHostOffers failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.AcquireHostOffersRequest"/>

### AcquireHostOffersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filter | [HostFilter](#peloton.private.hostmgr.hostsvc.HostFilter) |  |  |






<a name="peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse"/>

### AcquireHostOffersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [AcquireHostOffersResponse.Error](#peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.Error) |  |  |
| hostOffers | [HostOffer](#peloton.private.hostmgr.hostsvc.HostOffer) | repeated | The list of host offers that have been returned |
| filterResultCounts | [AcquireHostOffersResponse.FilterResultCountsEntry](#peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.FilterResultCountsEntry) | repeated | key: HostFilterResult&#39;s string form, value: count. used for debugging purpose. |






<a name="peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.Error"/>

### AcquireHostOffersResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invalidHostFilter | [InvalidHostFilter](#peloton.private.hostmgr.hostsvc.InvalidHostFilter) |  |  |
| failure | [AcquireHostOffersFailure](#peloton.private.hostmgr.hostsvc.AcquireHostOffersFailure) |  |  |






<a name="peloton.private.hostmgr.hostsvc.AcquireHostOffersResponse.FilterResultCountsEntry"/>

### AcquireHostOffersResponse.FilterResultCountsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.private.hostmgr.hostsvc.ClusterCapacityRequest"/>

### ClusterCapacityRequest







<a name="peloton.private.hostmgr.hostsvc.ClusterCapacityResponse"/>

### ClusterCapacityResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [ClusterCapacityResponse.Error](#peloton.private.hostmgr.hostsvc.ClusterCapacityResponse.Error) |  |  |
| resources | [Resource](#peloton.private.hostmgr.hostsvc.Resource) | repeated | Resources allocated |
| physicalResources | [Resource](#peloton.private.hostmgr.hostsvc.Resource) | repeated | Resources for total physical capacity. |






<a name="peloton.private.hostmgr.hostsvc.ClusterCapacityResponse.Error"/>

### ClusterCapacityResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| clusterUnavailable | [ClusterUnavailable](#peloton.private.hostmgr.hostsvc.ClusterUnavailable) |  |  |






<a name="peloton.private.hostmgr.hostsvc.ClusterUnavailable"/>

### ClusterUnavailable
Error when cluster is Unavailable


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.CreateVolumesRequest"/>

### CreateVolumesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.CreateVolumesResponse"/>

### CreateVolumesResponse
TODO: Add errors that could fail a create volumes request






<a name="peloton.private.hostmgr.hostsvc.DestroyVolumesRequest"/>

### DestroyVolumesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumes | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.DestroyVolumesResponse"/>

### DestroyVolumesResponse
TODO: Add errors that could fail a destroy volumes request






<a name="peloton.private.hostmgr.hostsvc.ExecutorOnAgent"/>

### ExecutorOnAgent
ExecutorOnAgent describes the executor to be shutdown by host manager including Mesos
agent id and executor id.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executorId | [.mesos.v1.ExecutorID](#peloton.private.hostmgr.hostsvc..mesos.v1.ExecutorID) |  |  |
| agentId | [.mesos.v1.AgentID](#peloton.private.hostmgr.hostsvc..mesos.v1.AgentID) |  |  |






<a name="peloton.private.hostmgr.hostsvc.HostFilter"/>

### HostFilter
HostFilter can be used to control whether offers from a given host should
be returned to placement engine to use.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resourceConstraint | [ResourceConstraint](#peloton.private.hostmgr.hostsvc.ResourceConstraint) |  | Resource constraint which must be satisfied. |
| schedulingConstraint | [.peloton.api.task.Constraint](#peloton.private.hostmgr.hostsvc..peloton.api.task.Constraint) |  | Attribute based affinity/anti-affinity scheduling constraints, which is typically copied from original task scheduling constraint. Only constraint with kind == HOST will be considered. |
| quantity | [QuantityControl](#peloton.private.hostmgr.hostsvc.QuantityControl) |  | Extra quantity control message which can be used to influence how many host offers are returned. |






<a name="peloton.private.hostmgr.hostsvc.HostOffer"/>

### HostOffer
HostOffer describes the resources available on a host by aggregating
a list of Mesos offers to avoid offer defragmentation issue.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  |  |
| agentId | [.mesos.v1.AgentID](#peloton.private.hostmgr.hostsvc..mesos.v1.AgentID) |  |  |
| resources | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated |  |
| attributes | [.mesos.v1.Attribute](#peloton.private.hostmgr.hostsvc..mesos.v1.Attribute) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.InvalidArgument"/>

### InvalidArgument
Error for invalid argument.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| invalidTasks | [LaunchableTask](#peloton.private.hostmgr.hostsvc.LaunchableTask) | repeated | Any LaunchableTask whose content is invalid. |






<a name="peloton.private.hostmgr.hostsvc.InvalidExecutors"/>

### InvalidExecutors
Error for invalid shutdown executors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| executors | [ExecutorOnAgent](#peloton.private.hostmgr.hostsvc.ExecutorOnAgent) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.InvalidHostFilter"/>

### InvalidHostFilter
Error for invalid filter.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| invalid | [HostFilter](#peloton.private.hostmgr.hostsvc.HostFilter) |  | Invalid filter from input. |






<a name="peloton.private.hostmgr.hostsvc.InvalidOffers"/>

### InvalidOffers
Error for invalid offers.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.InvalidTaskIDs"/>

### InvalidTaskIDs
Error for invalid task ids.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| taskIds | [.mesos.v1.TaskID](#peloton.private.hostmgr.hostsvc..mesos.v1.TaskID) | repeated | Any LaunchableTask whose content is invalid. |






<a name="peloton.private.hostmgr.hostsvc.KillFailure"/>

### KillFailure
Error when actually tasks kill failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| taskIds | [.mesos.v1.TaskID](#peloton.private.hostmgr.hostsvc..mesos.v1.TaskID) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.KillTasksRequest"/>

### KillTasksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskIds | [.mesos.v1.TaskID](#peloton.private.hostmgr.hostsvc..mesos.v1.TaskID) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.KillTasksResponse"/>

### KillTasksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [KillTasksResponse.Error](#peloton.private.hostmgr.hostsvc.KillTasksResponse.Error) |  |  |






<a name="peloton.private.hostmgr.hostsvc.KillTasksResponse.Error"/>

### KillTasksResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invalidTaskIDs | [InvalidTaskIDs](#peloton.private.hostmgr.hostsvc.InvalidTaskIDs) |  |  |
| killFailure | [KillFailure](#peloton.private.hostmgr.hostsvc.KillFailure) |  |  |






<a name="peloton.private.hostmgr.hostsvc.LaunchFailure"/>

### LaunchFailure
Error when actually tasks launch failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.LaunchTasksRequest"/>

### LaunchTasksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostname | [string](#string) |  |  |
| tasks | [LaunchableTask](#peloton.private.hostmgr.hostsvc.LaunchableTask) | repeated |  |
| agentId | [.mesos.v1.AgentID](#peloton.private.hostmgr.hostsvc..mesos.v1.AgentID) |  |  |






<a name="peloton.private.hostmgr.hostsvc.LaunchTasksResponse"/>

### LaunchTasksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [LaunchTasksResponse.Error](#peloton.private.hostmgr.hostsvc.LaunchTasksResponse.Error) |  |  |






<a name="peloton.private.hostmgr.hostsvc.LaunchTasksResponse.Error"/>

### LaunchTasksResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invalidArgument | [InvalidArgument](#peloton.private.hostmgr.hostsvc.InvalidArgument) |  |  |
| launchFailure | [LaunchFailure](#peloton.private.hostmgr.hostsvc.LaunchFailure) |  |  |
| invalidOffers | [InvalidOffers](#peloton.private.hostmgr.hostsvc.InvalidOffers) |  |  |






<a name="peloton.private.hostmgr.hostsvc.LaunchableTask"/>

### LaunchableTask
LaunchableTask describes the task to be launched by host manager including Mesos
task id and task config.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| taskId | [.mesos.v1.TaskID](#peloton.private.hostmgr.hostsvc..mesos.v1.TaskID) |  |  |
| config | [.peloton.api.task.TaskConfig](#peloton.private.hostmgr.hostsvc..peloton.api.task.TaskConfig) |  |  |
| ports | [LaunchableTask.PortsEntry](#peloton.private.hostmgr.hostsvc.LaunchableTask.PortsEntry) | repeated | Dynamic ports reserved on the host while this instance is running. The key is port name, value is port number. |
| volume | [Volume](#peloton.private.hostmgr.hostsvc.Volume) |  | Persistent volume used to launch task. |






<a name="peloton.private.hostmgr.hostsvc.LaunchableTask.PortsEntry"/>

### LaunchableTask.PortsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [uint32](#uint32) |  |  |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation"/>

### OfferOperation



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [OfferOperation.Type](#peloton.private.hostmgr.hostsvc.OfferOperation.Type) |  | Type of the operation. |
| reservationLabels | [.mesos.v1.Labels](#peloton.private.hostmgr.hostsvc..mesos.v1.Labels) |  | Reservation labels used for operations. |
| reserve | [OfferOperation.Reserve](#peloton.private.hostmgr.hostsvc.OfferOperation.Reserve) |  |  |
| create | [OfferOperation.Create](#peloton.private.hostmgr.hostsvc.OfferOperation.Create) |  |  |
| launch | [OfferOperation.Launch](#peloton.private.hostmgr.hostsvc.OfferOperation.Launch) |  |  |
| destroy | [OfferOperation.Destroy](#peloton.private.hostmgr.hostsvc.OfferOperation.Destroy) |  |  |
| unreserve | [OfferOperation.Unreserve](#peloton.private.hostmgr.hostsvc.OfferOperation.Unreserve) |  |  |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Create"/>

### OfferOperation.Create



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volume | [Volume](#peloton.private.hostmgr.hostsvc.Volume) |  | Persistent volume to be created. |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Destroy"/>

### OfferOperation.Destroy



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| volumeID | [string](#string) |  | Persistent volume ID to be destroyed. |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Launch"/>

### OfferOperation.Launch



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [LaunchableTask](#peloton.private.hostmgr.hostsvc.LaunchableTask) | repeated | List of tasks to be launched. |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Reserve"/>

### OfferOperation.Reserve



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resources | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated | Mesos resources to be reserved. |






<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Unreserve"/>

### OfferOperation.Unreserve



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| label | [string](#string) |  | The reservation label string to be unreserved. |






<a name="peloton.private.hostmgr.hostsvc.OfferOperationsRequest"/>

### OfferOperationsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| operations | [OfferOperation](#peloton.private.hostmgr.hostsvc.OfferOperation) | repeated | Repeated Operations will be performed in sequential manner. |
| hostname | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.OfferOperationsResponse"/>

### OfferOperationsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [OfferOperationsResponse.Error](#peloton.private.hostmgr.hostsvc.OfferOperationsResponse.Error) |  |  |






<a name="peloton.private.hostmgr.hostsvc.OfferOperationsResponse.Error"/>

### OfferOperationsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| failure | [OperationsFailure](#peloton.private.hostmgr.hostsvc.OperationsFailure) |  |  |
| invalidArgument | [InvalidArgument](#peloton.private.hostmgr.hostsvc.InvalidArgument) |  |  |
| invalidOffers | [InvalidOffers](#peloton.private.hostmgr.hostsvc.InvalidOffers) |  |  |






<a name="peloton.private.hostmgr.hostsvc.OperationsFailure"/>

### OperationsFailure
Error for offer operations call to mesos failed.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |






<a name="peloton.private.hostmgr.hostsvc.QuantityControl"/>

### QuantityControl
QuantityControl includes input from placement engine to control how many
host offers need to be returned.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| maxHosts | [uint32](#uint32) |  | Optinoal maximum number of hosts to return. Default zero value is no-op. |






<a name="peloton.private.hostmgr.hostsvc.ReleaseHostOffersRequest"/>

### ReleaseHostOffersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| hostOffers | [HostOffer](#peloton.private.hostmgr.hostsvc.HostOffer) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.ReleaseHostOffersResponse"/>

### ReleaseHostOffersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [ReleaseHostOffersResponse.Error](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersResponse.Error) |  |  |






<a name="peloton.private.hostmgr.hostsvc.ReleaseHostOffersResponse.Error"/>

### ReleaseHostOffersResponse.Error







<a name="peloton.private.hostmgr.hostsvc.ReserveResourcesRequest"/>

### ReserveResourcesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resources | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.ReserveResourcesResponse"/>

### ReserveResourcesResponse
TODO: Add errors that could fail a reserve resources request






<a name="peloton.private.hostmgr.hostsvc.Resource"/>

### Resource
Resource allocation for a resource


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| kind | [string](#string) |  | Type of the resource |
| capacity | [double](#double) |  | capacity of the resource |






<a name="peloton.private.hostmgr.hostsvc.ResourceConstraint"/>

### ResourceConstraint
ResourceConstraint descrbes a condition for which aggregated resources from
a host must meet in order for it to be returned in `AcquireHostOffers`.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| minimum | [.peloton.api.task.ResourceConfig](#peloton.private.hostmgr.hostsvc..peloton.api.task.ResourceConfig) |  | Minimum amount of resources NOTE: gpu resources are specially protected in the following way: - if `gpuLimit` is specified, only hosts with enough gpu resources are returned; - if `gpuLimit` is not specified, only hosts without gpu resource will be returned. |
| numPorts | [uint32](#uint32) |  | Number of dynamic ports |






<a name="peloton.private.hostmgr.hostsvc.ShutdownExecutorsRequest"/>

### ShutdownExecutorsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| executors | [ExecutorOnAgent](#peloton.private.hostmgr.hostsvc.ExecutorOnAgent) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.ShutdownExecutorsResponse"/>

### ShutdownExecutorsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [ShutdownExecutorsResponse.Error](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsResponse.Error) |  |  |






<a name="peloton.private.hostmgr.hostsvc.ShutdownExecutorsResponse.Error"/>

### ShutdownExecutorsResponse.Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| invalidExecutors | [InvalidExecutors](#peloton.private.hostmgr.hostsvc.InvalidExecutors) |  |  |
| shutdownFailure | [ShutdownFailure](#peloton.private.hostmgr.hostsvc.ShutdownFailure) |  |  |






<a name="peloton.private.hostmgr.hostsvc.ShutdownFailure"/>

### ShutdownFailure
Error for failed shutdown executors.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [string](#string) |  |  |
| executors | [ExecutorOnAgent](#peloton.private.hostmgr.hostsvc.ExecutorOnAgent) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.UnreserveResourcesRequest"/>

### UnreserveResourcesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resources | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) | repeated |  |






<a name="peloton.private.hostmgr.hostsvc.UnreserveResourcesResponse"/>

### UnreserveResourcesResponse
TODO: Add errors that could fail a unreserve resources request






<a name="peloton.private.hostmgr.hostsvc.Volume"/>

### Volume
Volume config used for offer create/launch operation.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [.peloton.api.peloton.VolumeID](#peloton.private.hostmgr.hostsvc..peloton.api.peloton.VolumeID) |  | ID of the persistent volume. |
| containerPath | [string](#string) |  | the relative volume path inside the container. |
| resource | [.mesos.v1.Resource](#peloton.private.hostmgr.hostsvc..mesos.v1.Resource) |  | Resource needed for the volume. |





 


<a name="peloton.private.hostmgr.hostsvc.HostFilterResult"/>

### HostFilterResult
HostFilterResult describes result of filtering hosts.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 | Reserved in case new result is unrecognized. |
| MATCH | 1 | Host is matched by filter and returned in HostOffer. |
| INSUFFICIENT_OFFER_RESOURCES | 2 | Host has enough total resources but offered resources is insufficient (signal for fragmentation). |
| MISMATCH_CONSTRAINTS | 3 | Host is filtered out because of mismatched task -&gt; host constraint. |
| MISMATCH_GPU | 4 | Host has GPU so reserved for GPU only task. |
| MISMATCH_STATUS | 5 | Host is in mismatch status (i.e, another placement engine) has a hold of the host. |
| MISMATCH_MAX_HOST_LIMIT | 6 | Host is filtered out because maxHosts limit is reached. |
| NO_OFFER | 7 | Host has no available offer to be matched. Usually this means the host is fully used already. |



<a name="peloton.private.hostmgr.hostsvc.OfferOperation.Type"/>

### OfferOperation.Type
Defines an operation that can be performed against offers.

| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| LAUNCH | 1 |  |
| RESERVE | 2 |  |
| UNRESERVE | 3 |  |
| CREATE | 4 |  |
| DESTROY | 5 |  |


 

 


<a name="peloton.private.hostmgr.hostsvc.InternalHostService"/>

### InternalHostService
Internal host service interface to be used by JobManager and
PlacementEngine for task, reservation, volume and offer operations

TODO: figure out a better name for InternalHostService

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| AcquireHostOffers | [AcquireHostOffersRequest](#peloton.private.hostmgr.hostsvc.AcquireHostOffersRequest) | [AcquireHostOffersResponse](#peloton.private.hostmgr.hostsvc.AcquireHostOffersRequest) | Acquire a set of host offers from the host manager. Each host offer will only be used by one client at a given time. |
| ReleaseHostOffers | [ReleaseHostOffersRequest](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersRequest) | [ReleaseHostOffersResponse](#peloton.private.hostmgr.hostsvc.ReleaseHostOffersRequest) | Release unused host offers to the host manager. |
| LaunchTasks | [LaunchTasksRequest](#peloton.private.hostmgr.hostsvc.LaunchTasksRequest) | [LaunchTasksResponse](#peloton.private.hostmgr.hostsvc.LaunchTasksRequest) | Launch tasks on Mesos cluster |
| KillTasks | [KillTasksRequest](#peloton.private.hostmgr.hostsvc.KillTasksRequest) | [KillTasksResponse](#peloton.private.hostmgr.hostsvc.KillTasksRequest) | Kill tasks that are running on Mesos cluster |
| ShutdownExecutors | [ShutdownExecutorsRequest](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsRequest) | [ShutdownExecutorsResponse](#peloton.private.hostmgr.hostsvc.ShutdownExecutorsRequest) | Shutdown executors that running on a Mesos agent |
| ReserveResources | [ReserveResourcesRequest](#peloton.private.hostmgr.hostsvc.ReserveResourcesRequest) | [ReserveResourcesResponse](#peloton.private.hostmgr.hostsvc.ReserveResourcesRequest) | Reserve resources on a host |
| UnreserveResources | [UnreserveResourcesRequest](#peloton.private.hostmgr.hostsvc.UnreserveResourcesRequest) | [UnreserveResourcesResponse](#peloton.private.hostmgr.hostsvc.UnreserveResourcesRequest) | Unreserve resources on a host |
| CreateVolumes | [CreateVolumesRequest](#peloton.private.hostmgr.hostsvc.CreateVolumesRequest) | [CreateVolumesResponse](#peloton.private.hostmgr.hostsvc.CreateVolumesRequest) | Create volumes on a Mesos host |
| DestroyVolumes | [DestroyVolumesRequest](#peloton.private.hostmgr.hostsvc.DestroyVolumesRequest) | [DestroyVolumesResponse](#peloton.private.hostmgr.hostsvc.DestroyVolumesRequest) | Destroy volumes on a Mesos host |
| ClusterCapacity | [ClusterCapacityRequest](#peloton.private.hostmgr.hostsvc.ClusterCapacityRequest) | [ClusterCapacityResponse](#peloton.private.hostmgr.hostsvc.ClusterCapacityRequest) | TODO move out to separate service if scope widens ClusterCapacity fetches the allocated resources to the framework` |
| OfferOperations | [OfferOperationsRequest](#peloton.private.hostmgr.hostsvc.OfferOperationsRequest) | [OfferOperationsResponse](#peloton.private.hostmgr.hostsvc.OfferOperationsRequest) | Performs batch offer operations. |

 



<a name="taskqueue.proto"/>
<p align="right"><a href="#top">Top</a></p>

## taskqueue.proto



<a name="peloton.private.resmgr.taskqueue.DequeueRequest"/>

### DequeueRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| limit | [uint32](#uint32) |  | Max number of tasks to dequeue |






<a name="peloton.private.resmgr.taskqueue.DequeueResponse"/>

### DequeueResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [.peloton.api.task.TaskInfo](#peloton.private.resmgr.taskqueue..peloton.api.task.TaskInfo) | repeated | The list of tasks that have been dequeued |






<a name="peloton.private.resmgr.taskqueue.EnqueueRequest"/>

### EnqueueRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tasks | [.peloton.api.task.TaskInfo](#peloton.private.resmgr.taskqueue..peloton.api.task.TaskInfo) | repeated | The list of tasks to enqueue |






<a name="peloton.private.resmgr.taskqueue.EnqueueResponse"/>

### EnqueueResponse
TODO: Add error handling here





 

 

 


<a name="peloton.private.resmgr.taskqueue.TaskQueue"/>

### TaskQueue


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| Enqueue | [EnqueueRequest](#peloton.private.resmgr.taskqueue.EnqueueRequest) | [EnqueueResponse](#peloton.private.resmgr.taskqueue.EnqueueRequest) | Enqueue a list of tasks |
| Dequeue | [DequeueRequest](#peloton.private.resmgr.taskqueue.DequeueRequest) | [DequeueResponse](#peloton.private.resmgr.taskqueue.DequeueRequest) | Dequeue a list of tasks |

 



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

