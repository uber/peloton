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
	// PelotonPlacement application name
	PelotonPlacement = "peloton-placement"
	// PelotonCLI application name
	PelotonCLI = "peloton-cli"
	// PelotonAuroraBridge application name
	PelotonAuroraBridge = "peloton-aurorabridge"

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
)

const (
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
)

const (
	// DefaultTaskConfigID is used for storing, and retrieving, the default
	// task configuration, when no specific is available.
	DefaultTaskConfigID = -1
)
