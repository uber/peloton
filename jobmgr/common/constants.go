package common

// Name of the fields in pbtask.RuntimeInfo, which is used by job/task cache
// update request. This list is maintained in sorted order.
const (
	AgentIDField              = "AgentID"
	CompletionTimeField       = "CompletionTime"
	ConfigVersionField        = "ConfigVersion"
	DesiredConfigVersionField = "DesiredConfigVersion"
	DesiredMesosTaskIDField   = "DesiredMesosTaskId"
	FailureCountField         = "FailureCount"
	GoalStateField            = "GoalState"
	HealthyField              = "Healthy"
	HostField                 = "Host"
	MesosTaskIDField          = "MesosTaskId"
	MessageField              = "Message"
	PortsField                = "Ports"
	PrevMesosTaskIDField      = "PrevMesosTaskId"
	ReasonField               = "Reason"
	ResourceUsageField        = "ResourceUsage"
	RevisionField             = "Revision"
	StartTimeField            = "StartTime"
	StateField                = "State"
	VolumeIDField             = "VolumeID"
)

// MaxConcurrencyErrorRetry indicates the maximum number of times to retry
// if a concurrency error is received during optimistic concurrency
// control when writing to the cache.
const MaxConcurrencyErrorRetry = 5
