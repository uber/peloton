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

// Name of the fields in pbtask.RuntimeInfo, which is used by job/task cache
// update request. This list is maintained in sorted order.
const (
	AgentIDField              = "AgentID"
	CompletionTimeField       = "CompletionTime"
	ConfigVersionField        = "ConfigVersion"
	DesiredConfigVersionField = "DesiredConfigVersion"
	DesiredHostField          = "DesiredHost"
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
	TerminationStatusField    = "TerminationStatus"
)

const (
	// MaxConcurrencyErrorRetry indicates the maximum number of times to retry
	// if a concurrency error is received during optimistic concurrency
	// control when writing to the cache.
	MaxConcurrencyErrorRetry = 5

	// MaxSystemFailureAttempts indicates the maximum retries on mesos
	// system failures
	MaxSystemFailureAttempts = 4
)

const (
	// DummyConfigVersion is the config version of the dummy config which is used
	// for job creation. Config with this version means that the config has nothing,
	// and is used merely as a placeholder.
	DummyConfigVersion = 0
)
