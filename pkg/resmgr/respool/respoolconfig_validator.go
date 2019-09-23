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

package respool

import (
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/uber/peloton/pkg/common"
)

// Validator performs validations on the resource pool config
type Validator interface {
	// Validate validates the resource pool config
	Validate(data ResourcePoolConfigData) error
	// register a slice of Validator functions
	Register(validatorFuncs []ResourcePoolConfigValidatorFunc) (Validator, error)
}

// ResourcePoolConfigValidatorFunc Validator func for registering custom validator
type ResourcePoolConfigValidatorFunc func(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error

// ResourcePoolConfigData holds the data that needs to be validated
type ResourcePoolConfigData struct {
	ID                 *peloton.ResourcePoolID     // Resource Pool ID
	Path               *respool.ResourcePoolPath   // Resource Pool path
	ResourcePoolConfig *respool.ResourcePoolConfig // Resource Pool Configuration
}

// Implements Validator
type resourcePoolConfigValidator struct {
	resTree                          Tree
	resourcePoolConfigValidatorFuncs []ResourcePoolConfigValidatorFunc
}

// NewResourcePoolConfigValidator returns a new resource pool config validator
func NewResourcePoolConfigValidator(rTree Tree) (Validator, error) {
	resourcePoolConfigValidator := &resourcePoolConfigValidator{
		resTree: rTree,
	}

	return resourcePoolConfigValidator.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePool,
			ValidateCycle,
			ValidateParent,
			ValidateSiblings,
			ValidateChildrenReservations,
			ValidateControllerLimit,
		},
	)
}

// Validate validates the resource pool config
func (rv *resourcePoolConfigValidator) Validate(data ResourcePoolConfigData) error {
	for _, validatorFunc := range rv.resourcePoolConfigValidatorFuncs {
		err := validatorFunc(rv.resTree, data)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// Register a slice of validator functions
func (rv *resourcePoolConfigValidator) Register(validatorFuncs []ResourcePoolConfigValidatorFunc) (
	Validator, error) {
	rv.resourcePoolConfigValidatorFuncs = validatorFuncs
	return rv, nil
}

// ValidateParent {current} resource pool against it's {parent}
func ValidateParent(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {

	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID

	cResources := resPoolConfig.Resources

	// get parent ID
	newParentID := resPoolConfig.Parent

	existingResourcePool, _ := resTree.Get(ID)

	// lookup parent
	parent, err := resTree.Get(newParentID)
	if err != nil {
		return errors.WithStack(err)
	}

	// for existing resource pool check if parent changed
	if existingResourcePool != nil {
		existingParentID := existingResourcePool.Parent().ID()

		// avoid overriding child's parent
		if newParentID.Value != existingParentID {
			return errors.Errorf(
				"parent override not allowed, actual %s, override %s",
				existingParentID,
				newParentID.Value)
		}
	}

	// get parent resources
	pResources := parent.Resources()

	// iterate over child resource(s) and check with parent resource(s)
	for _, cResource := range cResources {
		if pResource, ok := pResources[cResource.Kind]; ok {
			// check child resource {limit} is not greater than parent {limit}
			if cResource.Limit > pResource.Limit {
				return errors.Errorf(
					"resource %s, limit %v exceeds parent limit %v",
					cResource.Kind,
					cResource.Limit,
					pResource.Limit,
				)
			}
		} else {
			return errors.Errorf(
				"parent %s doesn't have resource kind %s",
				newParentID.Value,
				cResource.Kind)
		}
	}
	return nil
}

// ValidateSiblings validates the resource pool name is unique amongst its
// siblings
func ValidateSiblings(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {
	name := resourcePoolConfigData.ResourcePoolConfig.Name
	parentID := resourcePoolConfigData.ResourcePoolConfig.Parent
	resourcePoolID := resourcePoolConfigData.ID

	parentResPool, err := resTree.Get(parentID)
	if err != nil {
		return errors.WithStack(err)
	}

	siblings := parentResPool.Children()
	siblingNames := make(map[string]bool)

	for e := siblings.Front(); e != nil; e = e.Next() {
		sibling := e.Value.(ResPool)
		siblingNames[sibling.Name()] = true
	}
	existingResPool, _ := resTree.Get(resourcePoolID)
	if existingResPool != nil {
		// In case of update API, we need to remove the existing node before
		// performing the check
		delete(siblingNames, existingResPool.Name())
	}
	log.WithField("siblingNames", siblingNames).
		WithField("name", name).
		Info("siblings to check")

	if _, ok := siblingNames[name]; ok {
		return errors.Errorf("resource pool:%s already exists", name)
	}
	return nil
}

// ValidateChildrenReservations All Child reservations against it parent
func ValidateChildrenReservations(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {

	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID

	// get parent ID
	parentID := resPoolConfig.Parent
	cResources := resPoolConfig.Resources

	// lookup parent
	parent, err := resTree.Get(parentID)
	if err != nil {
		return errors.WithStack(err)
	}

	// get child reservations
	childReservations, err := parent.AggregatedChildrenReservations()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sibling reservations")
	}

	existingResPool, _ := resTree.Get(ID)

	for _, cResource := range cResources {
		cResourceReservations := cResource.Reservation

		// agg with sibling reservations
		if siblingReservations, ok := childReservations[cResource.Kind]; ok {
			cResourceReservations += siblingReservations
		}

		// remove self reservations if we are updating resource pool config
		if existingResPool != nil {

			if existingResourceConfig, ok := existingResPool.Resources()[cResource.Kind]; ok {
				cResourceReservations -= existingResourceConfig.Reservation
			}
		}

		// check with parent and short circuit if aggregate reservations exceed parent reservations
		if parentResourceConfig, ok := parent.Resources()[cResource.Kind]; ok {
			if cResourceReservations > parentResourceConfig.Reservation {
				return errors.Errorf(
					"Aggregated child reservation %v of kind `%s` exceed parent `%s` reservations %v",
					cResourceReservations,
					cResource.Kind,
					parentID.Value,
					parentResourceConfig.Reservation,
				)
			}

		} else {
			return errors.Errorf(
				"parent %s doesn't have resource kind %s",
				parentID.Value,
				cResource.Kind)
		}

	}
	return nil
}

// ValidateResourcePool if resource configurations are correct
func ValidateResourcePool(_ Tree,
	resourcePoolConfigData ResourcePoolConfigData) error {
	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID

	// ID nil check
	if ID == nil {
		return errors.New("resource pool ID cannot be <nil>")
	}

	// resPoolConfig nil check
	if resPoolConfig == nil {
		return errors.New("resource pool config cannot be <nil>")
	}

	// root override check
	if ID.Value == common.RootResPoolID {
		return errors.Errorf("cannot override %s", common.RootResPoolID)
	}

	// use default if scheduling policy is SchedulingPolicy_UNKNOWN (not set)
	if resPoolConfig.Policy == respool.SchedulingPolicy_UNKNOWN {
		log.Infof("Scheduling policy is not set, use default %v", DefaultResPoolSchedulingPolicy)
		resPoolConfig.Policy = DefaultResPoolSchedulingPolicy
	}

	resconfigSet := map[string]bool{
		common.CPU:    false,
		common.GPU:    false,
		common.MEMORY: false,
		common.DISK:   false,
	}
	cResources := resPoolConfig.Resources
	for _, cResource := range cResources {
		kind := cResource.Kind
		configed, ok := resconfigSet[kind]
		if !ok {
			return errors.Errorf("resource pool config has unknown resource type %s", kind)
		}
		if configed {
			return errors.Errorf("resource pool config has multiple configurations for resource type %s", kind)
		}
		resconfigSet[kind] = true
		if cResource.Reservation < 0 {
			return errors.Errorf("resource pool config resource values can not be negative "+
				"%s: Reservation %v",
				cResource.Kind,
				cResource.Reservation)
		}
		if cResource.Share < 0 {
			return errors.Errorf("resource pool config resource values can not be negative "+
				"%s: Share %v",
				cResource.Kind,
				cResource.Share)
		}
		if cResource.Limit < cResource.Reservation {
			return errors.Errorf(
				"resource %s, reservation %v exceeds limit %v",
				cResource.Kind,
				cResource.Reservation,
				cResource.Limit)
		}
	}

	resUpdated := false
	for k, set := range resconfigSet {
		if !set {
			log.WithFields(log.Fields{
				"Respool":     ID.Value,
				"Kind":        k,
				"Reservation": _defaultReservation,
				"Limit":       _defaultLimit,
				"Share":       _defaultShare,
			}).Info("Resource not configured set to default value")
			cResources = append(cResources, &respool.ResourceConfig{
				Kind:        k,
				Reservation: _defaultReservation,
				Limit:       _defaultLimit,
				Share:       _defaultShare,
			})
			resUpdated = true
		}
	}
	if resUpdated {
		resPoolConfig.Resources = cResources
	}
	return nil
}

// ValidateCycle if adding/updating current pool would result in a cycle
func ValidateCycle(_ Tree,
	resourcePoolConfigData ResourcePoolConfigData) error {
	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID

	// get parent ID
	parentID := resPoolConfig.Parent

	// check if parent != child
	if ID.Value == parentID.Value {
		return errors.Errorf(
			"resource pool ID: %s and parent ID: %s cannot be same",
			ID.Value,
			parentID.Value)
	}
	return nil
}

// ValidateResourcePoolPath validates the resource pool path
func ValidateResourcePoolPath(_ Tree,
	resourcePoolConfigData ResourcePoolConfigData) error {
	path := resourcePoolConfigData.Path

	if path == nil {
		return errors.New("path cannot be nil")
	}

	if path.Value == "" {
		return errors.New("path cannot be empty")
	}

	if path.Value == "/" {
		return nil
	}

	if !strings.HasPrefix(path.Value, ResourcePoolPathDelimiter) {
		return errors.Errorf("path should begin with %s", ResourcePoolPathDelimiter)
	}

	return nil
}

// ValidateControllerLimit validates the controller limit
func ValidateControllerLimit(_ Tree,
	resourcePoolConfigData ResourcePoolConfigData) error {
	controllerLimit := resourcePoolConfigData.ResourcePoolConfig.GetControllerLimit()
	if controllerLimit == nil {
		return nil
	}

	maxPercent := controllerLimit.GetMaxPercent()
	if maxPercent > 100 {
		return errors.New("controller limit, " +
			"max percent cannot be more than 100")
	}
	return nil
}
