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

package objects

import (
	"context"
	"fmt"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"

	"github.com/uber/peloton/pkg/common/util"
	versionutil "github.com/uber/peloton/pkg/common/util/entityversion"
	"github.com/uber/peloton/pkg/storage/objects/base"

	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// init adds a PodEvents instance to the global list of storage objects
func init() {
	Objs = append(Objs, &PodEventsObject{})
}

// PodEventsObject corresponds to a row in pod_events table.
type PodEventsObject struct {
	// base.Object DB specific annotations
	base.Object `cassandra:"name=pod_events, primaryKey=((job_id,instance_id), run_id, update_time)"`
	// JobID of the job (uuid)
	JobID string `column:"name=job_id"`
	// InstanceID of the pod event
	InstanceID uint32 `column:"name=instance_id"`
	// RunID of the pod event
	RunID uint64 `column:"name=run_id"`
	// UpdateTime of the pod event
	UpdateTime gocql.UUID `column:"name=update_time"`
	// ActualState of the pod event
	ActualState string `column:"name=actual_state"`
	// AgentID of the pod event
	AgentID string `column:"name=agent_id"`
	// ConfigVersion of the pod event
	ConfigVersion uint64 `column:"name=config_version"`
	// DesiredConfigVersion of the pod event
	DesiredConfigVersion uint64 `column:"name=desired_config_version"`
	// DesiredRunID of the pod event
	DesiredRunID uint64 `column:"name=desired_run_id"`
	// GoalState of the pod event
	GoalState string `column:"name=goal_state"`
	// Healthy of the pod event
	Healthy string `column:"name=healthy"`
	// Hostname of the pod event
	Hostname string `column:"name=hostname"`
	// Message of the pod event
	Message string `column:"name=message"`
	// PodStatus of the pod event
	PodStatus []byte `column:"name=pod_status"`
	// PreviousRunID of the pod event
	PreviousRunID uint64 `column:"name=previous_run_id"`
	// Reason of the pod event
	Reason string `column:"name=reason"`
	// VolumeID of the pod event
	VolumeID string `column:"name=volumeid"`
}

// PodEventsOps provides methods for manipulating pod_events table.
type PodEventsOps interface {
	// Add upserts single pod state change for a Job -> Instance -> Run.
	// Task state events are sorted by
	// reverse chronological run_id and time of event.
	Create(
		ctx context.Context,
		jobID *peloton.JobID,
		instanceID uint32,
		runtime *task.RuntimeInfo,
	) error

	// Get returns pod events for a Job + Instance + PodID (optional)
	// Pod events are sorted by PodID + Timestamp
	GetAll(
		ctx context.Context,
		jobID string,
		instanceID uint32,
		podID ...string,
	) ([]*pod.PodEvent, error)
}

// ensure that default implementation (podEventsOps) satisfies the interface
var _ PodEventsOps = (*podEventsOps)(nil)

// podEventsOps implements PodEventsOps using a particular Store
type podEventsOps struct {
	store *Store
}

// NewPodEventsOps constructs a PodEventsOps object for provided Store.
func NewPodEventsOps(s *Store) PodEventsOps {
	return &podEventsOps{store: s}
}

// Create upserts single pod state change for a Job -> Instance -> Run.
// Task state events are sorted by reverse chronological run_id and time of event.
func (d *podEventsOps) Create(
	ctx context.Context,
	jobID *peloton.JobID,
	instanceID uint32,
	runtime *task.RuntimeInfo,
) error {
	var runID, prevRunID, desiredRunID uint64
	var podStatus []byte
	var err error

	if runID, err = util.ParseRunID(
		runtime.GetMesosTaskId().GetValue()); err != nil {
		return errors.Wrap(err, "Failed to parse run ID")
	}

	// when creating a task, GetPrevMesosTaskId is empty,
	// set prevRunID to 0
	if len(runtime.GetPrevMesosTaskId().GetValue()) == 0 {
		prevRunID = 0
	} else if prevRunID, err = util.ParseRunID(
		runtime.GetPrevMesosTaskId().GetValue()); err != nil {
		d.store.metrics.OrmTaskMetrics.PodEventsAddFail.Inc(1)
		return errors.Wrap(err, "Failed to parse runID")
	}

	// old job does not have desired mesos task id, make it the same as runID
	// TODO: remove the line after all tasks have desired mesos task id
	if len(runtime.GetDesiredMesosTaskId().GetValue()) == 0 {
		desiredRunID = runID
	} else if desiredRunID, err = util.ParseRunID(
		runtime.GetDesiredMesosTaskId().GetValue()); err != nil {
		d.store.metrics.OrmTaskMetrics.PodEventsAddFail.Inc(1)
		return errors.Wrap(err, "Failed to parse runID")
	}
	if podStatus, err = proto.Marshal(runtime); err != nil {
		d.store.metrics.OrmTaskMetrics.PodEventsAddFail.Inc(1)
		return errors.Wrap(err, "Failed to parse runtime")
	}
	podEventsObject := &PodEventsObject{
		JobID:                jobID.GetValue(),
		InstanceID:           instanceID,
		RunID:                runID,
		DesiredRunID:         desiredRunID,
		PreviousRunID:        prevRunID,
		UpdateTime:           gocql.TimeUUID(),
		ActualState:          runtime.GetState().String(),
		GoalState:            runtime.GetGoalState().String(),
		Healthy:              runtime.GetHealthy().String(),
		Hostname:             runtime.GetHost(),
		AgentID:              runtime.AgentID.GetValue(),
		ConfigVersion:        runtime.GetConfigVersion(),
		DesiredConfigVersion: runtime.GetDesiredConfigVersion(),
		VolumeID:             runtime.GetVolumeID().GetValue(),
		Message:              runtime.GetMessage(),
		Reason:               runtime.GetReason(),
		PodStatus:            podStatus,
	}

	if err = d.store.oClient.Create(ctx, podEventsObject); err != nil {
		d.store.metrics.OrmTaskMetrics.PodEventsAddFail.Inc(1)
		return err
	}
	d.store.metrics.OrmTaskMetrics.PodEventsAdd.Inc(1)
	return nil
}

func (d *podEventsOps) GetAll(
	ctx context.Context,
	jobID string,
	instanceID uint32,
	podID ...string) ([]*pod.PodEvent, error) {
	var PodEventsObjects []*pod.PodEvent
	podEventsObject := &PodEventsObject{
		JobID:      jobID,
		InstanceID: instanceID,
	}

	if len(podID) > 0 && len(podID[0]) > 0 {
		runID, err := util.ParseRunID(podID[0])
		if err != nil {
			return nil, errors.Wrap(err, "Failed to parse runID")
		}
		// Events are sorted in descending order by run_id and then update_time.
		podEventsObject = &PodEventsObject{
			JobID:      jobID,
			InstanceID: instanceID,
			RunID:      runID,
		}
	}
	result, err := d.store.oClient.GetAll(ctx,
		podEventsObject)
	if err != nil {
		d.store.metrics.OrmTaskMetrics.PodEventsGetFail.Inc(1)
		return nil, err
	}

	var podEvents []*pod.PodEvent
	for _, value := range result {
		podEvent := &pod.PodEvent{}

		podEventsObjectValue := value.(*PodEventsObject)
		podID := fmt.Sprintf("%s-%d-%d",
			podEventsObjectValue.JobID,
			podEventsObjectValue.InstanceID,
			podEventsObjectValue.RunID)

		prevPodID := fmt.Sprintf("%s-%d-%d",
			podEventsObjectValue.JobID,
			podEventsObjectValue.InstanceID,
			podEventsObjectValue.PreviousRunID)

		desiredPodID := fmt.Sprintf("%s-%d-%d",
			podEventsObjectValue.JobID,
			podEventsObjectValue.InstanceID,
			podEventsObjectValue.DesiredRunID)

		// Set podEvent fields
		podEvent.PodId = &v1alphapeloton.PodID{
			Value: podID,
		}
		podEvent.PrevPodId = &v1alphapeloton.PodID{
			Value: prevPodID,
		}
		podEvent.DesiredPodId = &v1alphapeloton.PodID{
			Value: desiredPodID,
		}
		podEvent.Timestamp =
			podEventsObjectValue.UpdateTime.Time().Format(time.RFC3339)
		podEvent.Version = versionutil.GetPodEntityVersion(podEventsObjectValue.ConfigVersion)
		podEvent.DesiredVersion = versionutil.GetPodEntityVersion(podEventsObjectValue.DesiredConfigVersion)

		podEvent.ActualState = podEventsObjectValue.ActualState
		podEvent.DesiredState = podEventsObjectValue.GoalState
		podEvent.Message = podEventsObjectValue.Message
		podEvent.Reason = podEventsObjectValue.Reason
		podEvent.AgentId = podEventsObjectValue.AgentID
		podEvent.Hostname = podEventsObjectValue.Hostname
		podEvent.Healthy = podEventsObjectValue.Healthy

		podEvents = append(PodEventsObjects, podEvent)
	}
	d.store.metrics.OrmTaskMetrics.PodEventsGet.Inc(1)

	return podEvents, nil
}
