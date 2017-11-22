package mimir

import (
	"fmt"
	"math"

	log "github.com/sirupsen/logrus"

	"code.uber.internal/infra/peloton/.gen/peloton/api/task"
	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"code.uber.internal/infra/peloton/.gen/peloton/private/resmgr"
	lib_mimir "code.uber.internal/infra/peloton/mimir-lib"
	"code.uber.internal/infra/peloton/mimir-lib/model/placement"
	"code.uber.internal/infra/peloton/placement/config"
	"code.uber.internal/infra/peloton/placement/models"
	"code.uber.internal/infra/peloton/placement/plugins"
)

var _offersFactor = map[resmgr.TaskType]float64{
	resmgr.TaskType_UNKNOWN:   1.0,
	resmgr.TaskType_BATCH:     1.5,
	resmgr.TaskType_STATELESS: 3.0,
	resmgr.TaskType_DAEMON:    3.0,
	resmgr.TaskType_STATEFUL:  3.0,
}

// New will create a new strategy using Mimir-lib to do the placement logic.
func New(placer lib_mimir.Placer, config *config.PlacementConfig) plugins.Strategy {
	return &mimir{
		placer: placer,
		config: config,
	}
}

// mimir is a placement strategy that uses the mimir library to decide on how to assign tasks to offers.
type mimir struct {
	placer lib_mimir.Placer
	config *config.PlacementConfig
}

func (mimir *mimir) convertAssignments(pelotonAssignments []*models.Assignment) ([]*placement.Assignment,
	map[*placement.Entity]*models.Assignment) {
	// Convert the Peloton assignments to mimir assignments and keep a map
	// from entities to Peloton assignments.
	assignments := make([]*placement.Assignment, 0, len(pelotonAssignments))
	entitiesToAssignments := make(map[*placement.Entity]*models.Assignment, len(pelotonAssignments))
	for _, p := range pelotonAssignments {
		data := p.Task().Data()
		if data == nil {
			entity := TaskToEntity(p.Task().Task())
			p.Task().SetData(entity)
			data = entity
		}
		entity, ok := data.(*placement.Entity)
		if !ok {
			log.WithFields(log.Fields{
				"actual_type":   fmt.Sprintf("%T", data),
				"expected_type": fmt.Sprintf("%T", &placement.Entity{}),
			}).Fatal("unexpected type")
		}
		assignments = append(assignments, placement.NewAssignment(entity))
		entitiesToAssignments[entity] = p
	}
	return assignments, entitiesToAssignments
}

func (mimir *mimir) convertHosts(hosts []*models.Host) ([]*placement.Group,
	map[*placement.Group]*models.Host) {
	// Convert the hosts to groups and keep a map from groups to hosts
	groups := make([]*placement.Group, 0, len(hosts))
	groupsToHosts := make(map[*placement.Group]*models.Host, len(hosts))
	for _, host := range hosts {
		data := host.Data()
		if data == nil {
			group := OfferToGroup(host.Offer())
			entities := placement.Entities{}
			for _, task := range host.Tasks() {
				entity := TaskToEntity(task)
				entities.Add(entity)
			}
			group.Entities = entities
			host.SetData(group)
			data = group
		}
		group, ok := data.(*placement.Group)
		if !ok {
			log.WithFields(log.Fields{
				"actual_type":   fmt.Sprintf("%T", data),
				"expected_type": fmt.Sprintf("%T", &placement.Group{}),
			}).Fatal("unexpected type")
		}
		groupsToHosts[group] = host
		groups = append(groups, group)
	}
	return groups, groupsToHosts
}

func (mimir *mimir) updateAssignments(assignments []*placement.Assignment,
	entitiesToAssignments map[*placement.Entity]*models.Assignment, groupsToOffers map[*placement.Group]*models.Host) {
	// Update the Peloton assignments from the mimir assignments
	for _, assignment := range assignments {
		if assignment.Failed {
			continue
		}
		offer := groupsToOffers[assignment.AssignedGroup]
		pelotonAssignment := entitiesToAssignments[assignment.Entity]
		pelotonAssignment.SetHost(offer)
	}
}

// PlaceOnce is an implementation of the placement.Strategy interface.
func (mimir *mimir) PlaceOnce(pelotonAssignments []*models.Assignment, hosts []*models.Host) {
	assignments, entitiesToAssignments := mimir.convertAssignments(pelotonAssignments)
	groups, groupsToHosts := mimir.convertHosts(hosts)

	log.WithFields(log.Fields{
		"assignments": pelotonAssignments,
		"hosts":       hosts,
	}).Debug("batch placement before")

	// Place the assignments onto the groups
	mimir.placer.Place(assignments, groups)

	mimir.updateAssignments(assignments, entitiesToAssignments, groupsToHosts)

	log.WithFields(log.Fields{
		"assignments": pelotonAssignments,
		"hosts":       hosts,
	}).Debug("batch placement after")
}

// Filters is an implementation of the placement.Strategy interface.
func (mimir *mimir) Filters(assignments []*models.Assignment) map[*hostsvc.HostFilter][]*models.Assignment {
	assignmentsCopy := make([]*models.Assignment, 0, len(assignments))
	var maxCPU, maxGPU, maxMemory, maxDisk, maxPorts float64
	for _, assignment := range assignments {
		assignmentsCopy = append(assignmentsCopy, assignment)
		resmgrTask := assignment.Task().Task()
		maxCPU = math.Max(maxCPU, resmgrTask.Resource.CpuLimit)
		maxGPU = math.Max(maxGPU, resmgrTask.Resource.GpuLimit)
		maxMemory = math.Max(maxMemory, resmgrTask.Resource.MemLimitMb)
		maxDisk = math.Max(maxDisk, resmgrTask.Resource.DiskLimitMb)
		maxPorts = math.Max(maxPorts, float64(resmgrTask.NumPorts))
	}
	maxOffers := mimir.config.OfferDequeueLimit
	factor := _offersFactor[mimir.config.TaskType]
	neededOffers := math.Ceil(float64(len(assignments)) * factor)
	if float64(maxOffers) > neededOffers {
		maxOffers = int(neededOffers)
	}
	return map[*hostsvc.HostFilter][]*models.Assignment{
		&hostsvc.HostFilter{
			ResourceConstraint: &hostsvc.ResourceConstraint{
				NumPorts: uint32(maxPorts),
				Minimum: &task.ResourceConfig{
					CpuLimit:    maxCPU,
					GpuLimit:    maxGPU,
					MemLimitMb:  maxMemory,
					DiskLimitMb: maxDisk,
				},
			},
			Quantity: &hostsvc.QuantityControl{
				MaxHosts: uint32(maxOffers),
			},
		}: assignmentsCopy,
	}
}

// ConcurrencySafe is an implementation of the placement.Strategy interface.
func (mimir *mimir) ConcurrencySafe() bool {
	return false
}
