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

package common

import (
	"time"
)

const (
	// MesosMasterScheduler service name
	MesosMasterScheduler = "mesos-master-scheduler"
	// MesosMasterOperator service name
	MesosMasterOperator = "mesos-master-operator"
	// MesosMasterOperatorEndPoint is the path for Mesos Master endpoint
	MesosMasterOperatorEndPoint = "/api/v1"
	// PelotonEndpointPath is the path for Peloton mux endpoint
	PelotonEndpointPath = "/api/v1"
	// PelotonRole name to connect with Mesos Master
	PelotonRole = "peloton"
	// PelotonPrincipal name to connect with Mesos Master
	PelotonPrincipal = "peloton"

	// PelotonJobManager application name
	PelotonJobManager = "peloton-jobmgr"
	// PelotonResourceManager application name
	PelotonResourceManager = "peloton-resmgr"
	// PelotonHostManager application name
	PelotonHostManager = "peloton-hostmgr"
	// PelotonV1HostManager application name
	PelotonV1HostManager = "peloton-v1-hostmgr"
	// PelotonPlacement application name
	PelotonPlacement = "peloton-placement"
	// PelotonCLI application name
	PelotonCLI = "peloton-cli"
	// PelotonAuroraBridge application name
	PelotonAuroraBridge = "peloton-aurorabridge"
	// PelotonAPIServer application name
	PelotonAPIServer = "peloton-apiserver"

	// Cqos advisor application name
	QoSAdvisorService = "qosadvisorservice"
	// PlacementRole is the leadership election role for placement engine
	PlacementRole = "placement"
	// HostManagerRole is the leadership election role for hostmgr
	HostManagerRole = "hostmanager"
	// JobManagerRole is the leadership election role for jobmgr
	JobManagerRole = "jobmanager"
	// ResourceManagerRole is the leadership election role for resmgr
	ResourceManagerRole = "resourcemanager"
	// PelotonAuroraBridgeRole is the leadership election for peloton-aurora bridge
	PelotonAuroraBridgeRole = "aurora/scheduler"

	// DefaultLeaderElectionRoot is the default leader election root path
	DefaultLeaderElectionRoot = "/peloton"

	// CPU resource type
	CPU = "cpu"
	// GPU resource type
	GPU = "gpu"
	// MEMORY resource type
	MEMORY = "memory"
	// DISK resource type
	DISK = "disk"

	// RootResPoolID is the ID for Root node
	RootResPoolID = "root"

	// DBStmtLogField is used by storage code to log DB statements
	// It is also used by SecretsFormatter to redact DB statements
	// related to secret_info table
	DBStmtLogField = "db_stmt"

	// DBUqlLogField is used by storage code to log DB statements
	// It is also used by SecretsFormatter to redact DB statements
	// related to secret_info table
	DBUqlLogField = "db_uql"

	// DBArgsLogField is used by storage code to log DB statements
	// It is also used by SecretsFormatter to redact DB statements
	// related to secret_info table
	DBArgsLogField = "db_args"

	// StaleJobStateDurationThreshold is the duration after which we recalculate
	// the job state for a job which has been in the same active state for this
	// time duration
	StaleJobStateDurationThreshold = 100 * time.Hour

	// MesosCPU resource kind
	MesosCPU = "cpus"
	// MesosMem resource kind
	MesosMem = "mem"
	// MesosDisk resource kind
	MesosDisk = "disk"
	// MesosGPU resource kind
	MesosGPU = "gpus"
	// MesosPorts resource kind
	MesosPorts = "ports"

	// V0Api identifier string to determine if a job config API version
	V0Api = "v0"
	// V1AlphaApi identifier string to determine if a job config API version
	V1AlphaApi = "v1alpha"

	// SystemLabelKeyTemplate is the system label key template
	SystemLabelKeyTemplate = "%s.%s"
	// SystemLabelPrefix is the system label key name prefix
	SystemLabelPrefix = "peloton"
	// SystemLabelResourcePool is the system label key name for resource pool
	SystemLabelResourcePool = "resource_pool"
	// SystemLabelJobName is the system label key name for job name
	SystemLabelJobName = "job_name"
	// SystemLabelJobOwner is the system label key name for job owner
	SystemLabelJobOwner = "job_owner"
	// SystemLabelJobType is the system label key name for job type
	SystemLabelJobType = "job_type"
	// SystemLabelCluster is the system label key name for cluster
	SystemLabelCluster = "cluster"
	// ClusterEnvVar is the cluster environment variable
	ClusterEnvVar = "CLUSTER"
	// PelotonExclusiveAttributeName is the name of Mesos agent attribute
	// that indicates to Peloton that the agent will be used exclusively
	// for certain workloads only.
	PelotonExclusiveAttributeName = "peloton/exclusive"
	// PelotonExclusiveNodeLabel is the label of exclusive node
	PelotonExclusiveNodeLabel = PelotonExclusiveAttributeName
	// PelotonAuroraBridgeExecutorIDPrefix is the prefix for the executor ID
	// for tasks launched through thermos executor
	PelotonAuroraBridgeExecutorIDPrefix = "thermos-"
	// HostNameKey is the special label key for hostname.
	HostNameKey = "hostname"

	// DefaultTaskConfigID is used for storing, and retrieving, the default
	// task configuration, when no specific is available.
	DefaultTaskConfigID = -1

	// AppLogField is the log field key for app name
	AppLogField = "app"

	// TaskThrottleMessage indicates that the task is throttled due to repeated failures.
	TaskThrottleMessage = "Task throttled due to failure"

	// DefaultHostPoolID is the ID of default host pool.
	DefaultHostPoolID = "default"

	// HostPoolKey is the key of host pool constraint.
	HostPoolKey = "host_pool"

	// BatchReservedHostPoolID is reserved batch host pool for
	// non-preemptible tasks
	BatchReservedHostPoolID = "batch_reserved"

	// SharedHostPoolID is shared host pool between stateless and
	// batch (preemptible) jobs
	SharedHostPoolID = "shared"

	// StatelessHostPoolID is reserved for stateless services
	StatelessHostPoolID = "stateless"
)

const (
	// ReservedProtobufFieldPrefix represents the field prefixes in protobuf generated
	// code that is private to generated code, and should not be used by peloton.
	ReservedProtobufFieldPrefix = "XXX_"
)
