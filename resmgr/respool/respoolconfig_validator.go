package respool

import (
	"peloton/api/respool"

	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

// ResourcePoolConfigValidatorFunc validator func for registering custom validator
type ResourcePoolConfigValidatorFunc func(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error

// ResourcePoolConfigData holds the data that needs to be validated
type ResourcePoolConfigData struct {
	ID                                *respool.ResourcePoolID     // Resource Pool Config ID
	ResourcePoolConfig                *respool.ResourcePoolConfig // Resource Pool Configuration
	SkipRootChildResourceConfigChecks bool                        // TODO Skip child resource config checks for Parent Root T799105
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
			ValidateChildrenReservations,
		},
	)
}

// Validate validates the resource pool config
func (rv *resourcePoolConfigValidator) Validate(data interface{}) error {

	if resourcePoolConfigData, ok := data.(ResourcePoolConfigData); ok {
		for _, validatorFunc := range rv.resourcePoolConfigValidatorFuncs {
			err := validatorFunc(rv.resTree, resourcePoolConfigData)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	} else {
		return errors.New("assertion failed, need type <ResourcePoolConfigData>")
	}

	return nil
}

// Register a slice of validator functions
func (rv *resourcePoolConfigValidator) Register(validatorFuncs interface{}) (Validator, error) {
	if vFuncs, ok := validatorFuncs.([]ResourcePoolConfigValidatorFunc); ok {
		rv.resourcePoolConfigValidatorFuncs = vFuncs
		return rv, nil
	}
	return nil, errors.New("assertion failed, need type <ResourcePoolConfigValidatorFunc>")
}

// ValidateParent {current} resource pool against it's {parent}
func ValidateParent(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {

	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID
	skipRootChildResourceConfigChecks := resourcePoolConfigData.SkipRootChildResourceConfigChecks

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

	// bypass check if parent is root
	if newParentID.Value == RootResPoolID && skipRootChildResourceConfigChecks {
		log.WithField("respool_id", ID.Value).Info("skipping parent limit check")
		return nil
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
					pResource.Share,
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

// ValidateChildrenReservations All Child reservations against it parent
func ValidateChildrenReservations(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {

	resPoolConfig := resourcePoolConfigData.ResourcePoolConfig
	ID := resourcePoolConfigData.ID
	skipRootChildResourceConfigChecks := resourcePoolConfigData.SkipRootChildResourceConfigChecks

	// get parent ID
	parentID := resPoolConfig.Parent

	// bypass check if parent is root
	if parentID.Value == RootResPoolID && skipRootChildResourceConfigChecks {
		log.WithField("respool_id", ID.Value).Info("skipping child reservations check")
		return nil
	}

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
func ValidateResourcePool(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {
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
	if ID.Value == RootResPoolID {
		return errors.Errorf("cannot override %s", RootResPoolID)
	}

	// validate unknown policy
	if resPoolConfig.Policy == respool.SchedulingPolicy_UNKNOWN {
		return errors.Errorf(
			"invalid policy type %d",
			resPoolConfig.Policy)
	}

	cResources := resPoolConfig.Resources
	for _, cResource := range cResources {
		// check child resource {limit} is not less than child {reservation}
		if cResource.Limit < cResource.Reservation {
			return errors.Errorf(
				"resource %s, reservation %v exceeds limit %v",
				cResource.Kind,
				cResource.Reservation,
				cResource.Limit,
			)
		}
	}
	return nil
}

// ValidateCycle if adding/updating current pool would result in a cycle
func ValidateCycle(resTree Tree, resourcePoolConfigData ResourcePoolConfigData) error {
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
