package common

const (
	// MesosMasterScheduler service name
	MesosMasterScheduler = "mesos-master-scheduler"
	// MesosMasterOperator service name
	MesosMasterOperator = "mesos-master-operator"
	// MesosMasterOperatorEndPoint is the path for Mesos Master endpoint
	MesosMasterOperatorEndPoint = "/api/v1"
	// PelotonEndpointPath is the path for Peloton mux endpoint
	PelotonEndpointPath = "/api/v1"

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
)
