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

	// PlacementRole is the leadership election role for placement engine
	PlacementRole = "placement"
	// HostManagerRole is the leadership election role for hostmgr
	HostManagerRole = "hostmanager"
	// JobManagerRole is the leadership election role for jobmgr
	JobManagerRole = "jobmanager"
	// ResourceManagerRole is the leadership election role for resmgr
	ResourceManagerRole = "resourcemanager"

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
	// SystemLabelRespoolPath is the system label key name for respool path
	SystemLabelRespoolPath = "respool_path"
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
)
