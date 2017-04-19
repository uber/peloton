package common

const (
	// MesosMaster service name
	MesosMaster = "mesos-master"
	// MesosMasterOperator service name
	MesosMasterOperator = "mesos-master-operator"
	// MesosMasterOperatorEndPoint is the path for Mesos Master endpoint
	MesosMasterOperatorEndPoint = "/api/v1"
	// PelotonEndpointPath is the path for Peloton mux endpoint
	PelotonEndpointPath = "/api/v1"
	// PelotonJobManager service name
	PelotonJobManager = "peloton-jobmgr"
	// PelotonResourceManager service name
	PelotonResourceManager = "peloton-resmgr"
	// PelotonHostManager service name
	PelotonHostManager = "peloton-hostmgr"
	// PelotonMaster service name
	PelotonMaster = "peloton-master"
	// PelotonPlacement service name
	PelotonPlacement = "peloton-placement"
	// MasterRole is the leadership election role for master process
	MasterRole = "master"
	// PlacementRole is the leadership election role for placement engine
	PlacementRole = "placement"
	// HostManagerRole is the leadership election role for hostmgr
	HostManagerRole = "hostmanager"
	// JobManagerRole is the leadership election role for jobmgr
	JobManagerRole = "jobmanager"
	// TaskManagerRole is the leadership election role for taskmgr
	TaskManagerRole = "taskmanager"
	// ResourceManagerRole is the leadership election role for resmgr
	ResourceManagerRole = "resourcemanager"
	// DefaultLeaderElectionRoot is the default leader election root path
	DefaultLeaderElectionRoot = "/peloton"
)
