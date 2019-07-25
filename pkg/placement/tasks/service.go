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

package tasks

import (
	"context"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	"github.com/uber/peloton/pkg/placement/config"
	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/models/v0"
)

const (
	_timeout               = 10 * time.Second
	_failedToEnqueueTasks  = "failed to enqueue tasks back to resource manager"
	_failedToDequeueTasks  = "failed to dequeue tasks from resource manager"
	_failedToSetPlacements = "failed to set placements"
)

// Service will manage gangs/tasks and placements used by any placement strategy.
type Service interface {
	// Dequeue fetches some tasks from the service.
	Dequeue(ctx context.Context, taskType resmgr.TaskType, batchSize int, timeout int) (assignments []models.Task)

	// SetPlacements sets successful and unsuccessful placements back to the service.
	SetPlacements(
		ctx context.Context,
		successFullPlacements []models.Task,
		failedAssignments []models.Task,
	)
}

// NewService will create a new task service.
func NewService(
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient,
	cfg *config.PlacementConfig,
	metrics *metrics.Metrics) Service {
	return &service{
		config:          cfg,
		resourceManager: resourceManager,
		metrics:         metrics,
	}
}

type service struct {
	config          *config.PlacementConfig
	resourceManager resmgrsvc.ResourceManagerServiceYARPCClient
	metrics         *metrics.Metrics
}

// Dequeue fetches some tasks from the resource manager.
func (s *service) Dequeue(
	ctx context.Context,
	taskType resmgr.TaskType,
	batchSize int,
	timeout int) []models.Task {
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	request := &resmgrsvc.DequeueGangsRequest{
		Limit:   uint32(batchSize),
		Type:    taskType,
		Timeout: uint32(timeout),
	}

	response, err := s.resourceManager.DequeueGangs(ctx, request)
	if err != nil {
		log.WithFields(log.Fields{
			"task_type":              taskType,
			"batch_size":             batchSize,
			"dequeue_gangs_request":  request,
			"dequeue_gangs_response": response,
		}).WithError(err).Error(_failedToDequeueTasks)
		return nil
	}

	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"task_type":              taskType,
			"batch_size":             batchSize,
			"dequeue_gangs_request":  request,
			"dequeue_gangs_response": response,
		}).WithError(errors.New(response.Error.String())).Error(_failedToDequeueTasks)
		return nil
	}

	numberOfTasks := 0
	for _, gang := range response.Gangs {
		numberOfTasks += len(gang.GetTasks())
	}

	s.metrics.TasksDequeued.Update(float64(numberOfTasks))

	if numberOfTasks == 0 {
		log.WithFields(log.Fields{
			"num_tasks": numberOfTasks,
		}).Debug("no tasks dequeued from resource manager")
		return nil
	}

	// Create assignments from the tasks but without any offers
	assignments := make([]models.Task, 0, numberOfTasks)
	now := time.Now()
	for _, gang := range response.Gangs {
		for _, task := range s.createTasks(gang, now) {
			assignments = append(assignments, models_v0.NewAssignment(task))
		}
	}

	if len(assignments) > 0 {
		log.WithFields(log.Fields{
			"request":         request,
			"response":        response,
			"task_type":       taskType,
			"batch_size":      batchSize,
			"timeout":         timeout,
			"number_of_tasks": numberOfTasks,
			"number_of_gangs": len(response.Gangs),
			"assignments_len": len(assignments),
			"assignments":     assignments,
		}).Debug("Dequeued gangs")
		log.WithField("tasks", len(assignments)).Info("Dequeued from task queue")
	}

	return assignments
}

// SetPlacements sets placements in the resource manager.
func (s *service) SetPlacements(
	ctx context.Context,
	successes []models.Task,
	failures []models.Task,
) {
	if len(successes) == 0 && len(failures) == 0 {
		log.Debug("No task to place")
		return
	}

	setPlacementStart := time.Now()
	ctx, cancelFunc := context.WithTimeout(ctx, _timeout)
	defer cancelFunc()

	// create the failed placements and populate the reason.
	failedPlacements := make([]*resmgrsvc.SetPlacementsRequest_FailedPlacement, len(failures))
	for i, a := range failures {
		failedPlacements[i] = &resmgrsvc.SetPlacementsRequest_FailedPlacement{
			Reason: a.GetPlacementFailure(),
			Gang: &resmgrsvc.Gang{
				Tasks: []*resmgr.Task{
					{
						Id: &peloton.TaskID{
							Value: a.PelotonID(),
						},
					},
				},
			},
		}
		log.WithField("task_id", a.PelotonID()).
			WithField("reason", a.GetPlacementFailure()).
			Info("failed placement")
	}

	var request = &resmgrsvc.SetPlacementsRequest{
		Placements:       s.createPlacements(successes),
		FailedPlacements: failedPlacements,
	}
	response, err := s.resourceManager.SetPlacements(ctx, request)
	if err != nil {
		log.WithFields(log.Fields{
			"num_placements":          len(successes),
			"num_failed_placements":   len(failedPlacements),
			"placements":              successes,
			"failed_placements":       failedPlacements,
			"set_placements_request":  request,
			"set_placements_response": response,
		}).WithError(err).
			Error(_failedToSetPlacements)
		return
	}

	if response.GetError().GetFailure() != nil {
		s.metrics.SetPlacementFail.Inc(
			int64(len(response.GetError().GetFailure().GetFailed())))
	}

	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"num_placements":          len(successes),
			"num_failed_placements":   len(failedPlacements),
			"placements":              successes,
			"failed_placements":       failedPlacements,
			"set_placements_request":  request,
			"set_placements_response": response,
		}).WithError(errors.New(response.Error.String())).
			Error(_failedToSetPlacements)
		return
	}

	log.WithField("num_placements", len(successes)).
		WithField("num_failed_placements", len(failedPlacements)).
		Debug("Set placements succeeded")

	setPlacementDuration := time.Since(setPlacementStart)
	s.metrics.SetPlacementDuration.Record(setPlacementDuration)
	s.metrics.SetPlacementSuccess.Inc(int64(len(successes)))
}

func (s *service) createPlacements(assigned []models.Task) []*resmgr.Placement {
	createPlacementStart := time.Now()
	// For each offer find all tasks assigned to it.
	offersByID := map[string]models.Offer{}
	offersToTasks := map[string][]models.Task{}
	for _, placement := range assigned {
		offer := placement.GetPlacement()
		if offer == nil {
			continue
		}
		offersByID[offer.ID()] = offer
		if _, exists := offersToTasks[offer.ID()]; !exists {
			offersToTasks[offer.ID()] = []models.Task{}
		}
		offersToTasks[offer.ID()] = append(offersToTasks[offer.ID()], placement)
	}

	// For each offer create a placement with all the tasks assigned to it.
	var resPlacements []*resmgr.Placement
	for offerID, tasks := range offersToTasks {
		offer := offersByID[offerID]
		selectedPorts := models.AssignPorts(offer, tasks)
		agentID := offer.AgentID()
		placement := &resmgr.Placement{
			Hostname:    offer.Hostname(),
			AgentId:     &mesos.AgentID{Value: &agentID},
			Type:        s.config.TaskType,
			TaskIDs:     getPlacementTasks(tasks),
			Ports:       formatPorts(selectedPorts),
			HostOfferID: &peloton.HostOfferID{Value: offer.ID()},
		}
		resPlacements = append(resPlacements, placement)
	}
	createPlacementDuration := time.Since(createPlacementStart)
	s.metrics.CreatePlacementDuration.Record(createPlacementDuration)
	return resPlacements
}

func (s *service) createTasks(gang *resmgrsvc.Gang, now time.Time) []*models_v0.TaskV0 {
	resTasks := gang.GetTasks()
	tasks := make([]*models_v0.TaskV0, len(resTasks))
	if len(resTasks) == 0 {
		return tasks
	}
	// A value for maxRounds of <= 0 means there is no limit
	maxRounds := s.config.MaxRounds.Value(resTasks[0].Type)
	duration := s.config.MaxDurations.Value(resTasks[0].Type)
	deadline := now.Add(duration)
	desiredHostPlacementDeadline := now.Add(s.config.MaxDesiredHostPlacementDuration)
	for i, task := range resTasks {
		tasks[i] = models_v0.NewTask(gang, task, deadline,
			desiredHostPlacementDeadline, maxRounds)
	}
	return tasks
}

func getPlacementTasks(tasks []models.Task) []*resmgr.Placement_Task {
	placementTasks := make([]*resmgr.Placement_Task, len(tasks))
	for i, task := range tasks {
		mesosID := task.OrchestrationID()
		placementTasks[i] = &resmgr.Placement_Task{
			PelotonTaskID: &peloton.TaskID{Value: task.PelotonID()},
			MesosTaskID:   &mesos.TaskID{Value: &mesosID},
		}
	}
	return placementTasks
}

func formatPorts(ports []uint64) []uint32 {
	result := make([]uint32, len(ports))
	for i, port := range ports {
		result[i] = uint32(port)
	}
	return result
}
