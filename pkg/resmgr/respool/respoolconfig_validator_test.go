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
	"context"
	"fmt"
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pb_respool "github.com/uber/peloton/.gen/peloton/api/v0/respool"

	"github.com/uber/peloton/pkg/common"
	store_mocks "github.com/uber/peloton/pkg/storage/mocks"
	objectmocks "github.com/uber/peloton/pkg/storage/objects/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type resPoolConfigValidatorSuite struct {
	suite.Suite
	resourceTree Tree
	mockCtrl     *gomock.Controller
}

func (s *resPoolConfigValidatorSuite) SetupSuite() {
	fmt.Println("setting up resPoolConfigValidatorSuite")
	s.mockCtrl = gomock.NewController(s.T())
	mockResPoolOps := objectmocks.NewMockResPoolOps(s.mockCtrl)
	mockResPoolOps.EXPECT().GetAll(context.Background()).
		Return(s.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(s.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(s.mockCtrl)
	s.resourceTree = &tree{
		respoolOps: mockResPoolOps,
		root:       nil,
		metrics:    NewMetrics(tally.NoopScope),
		resPools:   make(map[string]ResPool),
		jobStore:   mockJobStore,
		taskStore:  mockTaskStore,
		scope:      tally.NoopScope,
	}
}

func (s *resPoolConfigValidatorSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

func (s *resPoolConfigValidatorSuite) SetupTest() {
	err := s.resourceTree.Start()
	s.NoError(err)
}

func (s *resPoolConfigValidatorSuite) TearDownTest() {
	err := s.resourceTree.Stop()
	s.NoError(err)
}

// Returns resource configs
func (s *resPoolConfigValidatorSuite) getResourceConfig() []*pb_respool.ResourceConfig {

	resConfigs := []*pb_respool.ResourceConfig{
		{
			Share:       1,
			Kind:        "cpu",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "memory",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "disk",
			Reservation: 100,
			Limit:       1000,
		},
		{
			Share:       1,
			Kind:        "gpu",
			Reservation: 2,
			Limit:       4,
		},
	}
	return resConfigs
}

// Returns resource pools
func (s *resPoolConfigValidatorSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		common.RootResPoolID: {
			Name:      common.RootResPoolID,
			Parent:    nil,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: s.getResourceConfig(),
			Policy:    policy,
		},
		"respool23": {
			Name:   "respool23",
			Parent: &peloton.ResourcePoolID{Value: "respool22"},
			Resources: []*pb_respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
		"respool99": {
			Name:   "respool99",
			Parent: &peloton.ResourcePoolID{Value: "respool21"},
			Resources: []*pb_respool.ResourceConfig{
				{
					Kind:        "cpu",
					Reservation: 50,
					Limit:       100,
					Share:       1,
				},
			},
			Policy: policy,
		},
	}
}

func (s *resPoolConfigValidatorSuite) TestNewValidator() {
	v, err := NewResourcePoolConfigValidator(s.resourceTree)
	s.NoError(err)

	rcv, ok := v.(*resourcePoolConfigValidator)
	s.True(ok)
	s.Equal(6, len(rcv.resourcePoolConfigValidatorFuncs))
}

func (s *resPoolConfigValidatorSuite) TestValidateOverrideRoot() {
	mockResourcePoolID := &peloton.ResourcePoolID{Value: common.RootResPoolID}
	mockParentPoolID := &peloton.ResourcePoolID{Value: "respool11"}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       10,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateResourcePool})

	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)

	s.EqualError(err, fmt.Sprintf("cannot override %s", common.RootResPoolID))
}

func (s *resPoolConfigValidatorSuite) TestValidateCycle() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool33",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateCycle})

	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "resource pool ID: respool33 and parent "+
		"ID: respool33 cannot be same")
}

func (s *resPoolConfigValidatorSuite) TestValidateParentLookupError() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "i_do_not_exist",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})

	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "resource pool (i_do_not_exist) not found")
}

func (s *resPoolConfigValidatorSuite) TestValidateParentChanged() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool1",
	}
	mockChangedParentPoolID := &peloton.ResourcePoolID{
		Value: "respool2",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockChangedParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)

	s.EqualError(err, "parent override not allowed, "+
		"actual root, override respool2")
}

func (s *resPoolConfigValidatorSuite) TestValidateParentExceedLimit() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool11",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "cpu",
				Limit:       99999,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "resource cpu, limit 99999 exceeds"+
		" parent limit 1000")
}

func (s *resPoolConfigValidatorSuite) TestValidateInvalidResourceKind() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool33",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool11",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 50,
				Kind:        "aaa",
				Limit:       99999,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "parent respool11 doesn't have "+
		"resource kind aaa")
}

func (s *resPoolConfigValidatorSuite) TestValidateChildrenReservationsError() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool34",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: "respool21",
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 51,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateChildrenReservations})
	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(
		err,
		"Aggregated child reservation 101 of kind `cpu` "+
			"exceed parent `respool21` reservations 100",
	)
}

func (s *resPoolConfigValidatorSuite) TestRootValidationReservations() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool3",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	rootResPool, err := s.resourceTree.Get(mockParentPoolID)
	s.NoError(err)
	resourcePoolConfig := rootResPool.ResourcePoolConfig()
	resourcePoolConfig.Resources = s.getResourceConfig()
	rootResPool.SetResourcePoolConfig(resourcePoolConfig)

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				// child.Reservations > parent.Reservation
				Reservation: 100,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err = rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateChildrenReservations,
		},
	)

	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.Error(err)
	s.Equal(err.Error(), "Aggregated child reservation 300 "+
		"of kind `cpu` exceed parent `root` reservations 100")
}

func (s *resPoolConfigValidatorSuite) TestRootValidationParent() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool3",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	rootResPool, err := s.resourceTree.Get(mockParentPoolID)
	s.NoError(err)
	resourcePoolConfig := rootResPool.ResourcePoolConfig()
	resourcePoolConfig.Resources = s.getResourceConfig()
	rootResPool.SetResourcePoolConfig(resourcePoolConfig)

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				// child.Limit > parent.Limit
				Reservation: 100,
				Kind:        "cpu",
				Limit:       10001,
				Share:       2,
			},
		},
		Policy: pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:   mockParentPoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err = rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateParent,
		},
	)

	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.Error(err)
	s.Equal(err.Error(), "resource cpu, limit 10001 exceeds parent limit 1000")
}

func (s *resPoolConfigValidatorSuite) TestNoPolicy() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool99",
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{
			{
				Reservation: 51,
				Kind:        "cpu",
				Limit:       100,
				Share:       2,
			},
		},
		Name: mockParentPoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePool,
		},
	)

	s.NoError(err)
	s.EqualValues(mockResourcePoolConfig.Policy,
		pb_respool.SchedulingPolicy_UNKNOWN)

	err = rv.Validate(resourcePoolConfigData)

	s.NoError(err)
	s.EqualValues(mockResourcePoolConfig.Policy,
		pb_respool.SchedulingPolicy_PriorityFIFO)
}

func (s *resPoolConfigValidatorSuite) TestValidatePathError() {
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePoolPath,
		},
	)

	// empty path
	mockResourcePath := &pb_respool.ResourcePoolPath{
		Value: "",
	}
	resourcePoolConfigData := ResourcePoolConfigData{
		Path: mockResourcePath,
	}
	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "path cannot be empty")

	// nil path
	resourcePoolConfigData = ResourcePoolConfigData{
		Path: nil,
	}
	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "path cannot be nil")

	// invalid path
	mockResourcePath.Value = "infrastructure/compute"
	resourcePoolConfigData.Path = mockResourcePath
	err = rv.Validate(resourcePoolConfigData)
	s.EqualError(err, "path should begin with /")
}

func (s *resPoolConfigValidatorSuite) TestValidatePath() {
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePoolPath,
		},
	)
	s.NoError(err)

	// valid path
	mockResourcePath := &pb_respool.ResourcePoolPath{
		Value: "/infrastructure/compute",
	}
	resourcePoolConfigData := ResourcePoolConfigData{
		Path: mockResourcePath,
	}
	err = rv.Validate(resourcePoolConfigData)
	s.NoError(err)

	// root path
	mockResourcePath.Value = "/"
	resourcePoolConfigData.Path = mockResourcePath
	err = rv.Validate(resourcePoolConfigData)
	s.NoError(err)
}

// tests creating pool with existing name should fail
func (s *resPoolConfigValidatorSuite) TestValidateSiblingsCreate() {
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateSiblings,
		},
	)
	s.NoError(err)

	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: uuid.New(),
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Name:   "respool1", // duplicate name
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	err = rv.Validate(resourcePoolConfigData)
	s.Error(err)
	s.Equal("resource pool:respool1 already exists",
		err.Error())
}

// tests renaming pool to existing name should fail
func (s *resPoolConfigValidatorSuite) TestValidateSiblingsUpdate() {
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateSiblings,
		},
	)
	s.NoError(err)

	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool2", // existing ID
	}
	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent: mockParentPoolID,
		Name:   "respool1", // duplicate name
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}
	err = rv.Validate(resourcePoolConfigData)
	s.Error(err)
	s.Equal("resource pool:respool1 already exists",
		err.Error())
}

func (s *resPoolConfigValidatorSuite) TestValidateControllerLimit() {
	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateControllerLimit,
		},
	)
	s.NoError(err)

	tt := []struct {
		maxPercent float64
		err        error
	}{
		{
			maxPercent: 200,
			err:        errors.New("controller limit, max percent cannot be more than 100"),
		},
		{
			maxPercent: 10,
			err:        nil,
		},
	}

	for _, t := range tt {
		resourcePoolConfigData := ResourcePoolConfigData{
			ResourcePoolConfig: &pb_respool.ResourcePoolConfig{
				ControllerLimit: &pb_respool.ControllerLimit{
					MaxPercent: t.maxPercent,
				},
			},
		}
		err = rv.Validate(resourcePoolConfigData)
		if t.err != nil {
			s.EqualError(t.err, err.Error())
		} else {
			s.NoError(err)
		}
	}
}

func (s *resPoolConfigValidatorSuite) TestValidateNoConfigResources() {
	mockResourcePoolID := &peloton.ResourcePoolID{Value: "respool33"}
	mockParentPoolID := &peloton.ResourcePoolID{Value: "respool11"}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent:    mockParentPoolID,
		Resources: []*pb_respool.ResourceConfig{},
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:      mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateResourcePool})

	s.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	s.NoError(err)
	kinds := []string{}
	var reserves, limits, shares []float64
	for _, v := range mockResourcePoolConfig.Resources {
		kinds = append(kinds, v.Kind)
		reserves = append(reserves, v.Reservation)
		limits = append(limits, v.Limit)
		shares = append(shares, v.Share)
	}

	s.Equal(4, len(mockResourcePoolConfig.Resources))
	s.ElementsMatch(kinds, []string{common.CPU, common.MEMORY, common.DISK, common.GPU})
	s.ElementsMatch(reserves, []float64{0, 0, 0, 0})
	s.ElementsMatch(limits, []float64{0, 0, 0, 0})
	s.ElementsMatch(shares, []float64{1, 1, 1, 1})
}

func (s *resPoolConfigValidatorSuite) validateOnWrongResources(resources []*pb_respool.ResourceConfig) error {
	mockParentPoolID := &peloton.ResourcePoolID{Value: "respoolp"}
	mockResourcePoolID := &peloton.ResourcePoolID{Value: "respoolc"}

	mockResourcePoolConfig := &pb_respool.ResourcePoolConfig{
		Parent:    mockParentPoolID,
		Resources: resources,
		Policy:    pb_respool.SchedulingPolicy_PriorityFIFO,
		Name:      mockResourcePoolID.Value,
	}

	resourcePoolConfigData := ResourcePoolConfigData{
		ID:                 mockResourcePoolID,
		ResourcePoolConfig: mockResourcePoolConfig,
	}

	rv := &resourcePoolConfigValidator{resTree: s.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateResourcePool})

	s.NoError(err)

	return rv.Validate(resourcePoolConfigData)
}

func (s *resPoolConfigValidatorSuite) TestValidateResourcePoolOnWrongResources() {
	var test = []struct {
		resources []*pb_respool.ResourceConfig
		//f ResourcePoolConfigValidatorFunc
		expectedErr string
	}{
		{
			resources: []*pb_respool.ResourceConfig{
				{
					Reservation: -5,
					Kind:        "cpu",
					Limit:       10,
					Share:       2,
				},
			},
			expectedErr: "resource pool config resource values can not be negative cpu: Reservation -5",
		},
		{
			resources: []*pb_respool.ResourceConfig{
				{
					Reservation: 5,
					Kind:        "cpu",
					Limit:       10,
					Share:       2,
				},
				{
					Reservation: 6,
					Kind:        "cpu",
					Limit:       10,
					Share:       2,
				},
			},
			expectedErr: "resource pool config has multiple configurations for resource type cpu",
		},
		{
			resources: []*pb_respool.ResourceConfig{
				{
					Reservation: 50,
					Kind:        "cpu",
					Limit:       10,
					Share:       2,
				},
			},
			expectedErr: "resource cpu, reservation 50 exceeds limit 10",
		},
		{
			resources: []*pb_respool.ResourceConfig{
				{
					Reservation: 5,
					Kind:        "aaa",
					Limit:       10,
					Share:       2,
				},
			},
			expectedErr: "resource pool config has unknown resource type aaa",
		},
	}
	for _, t := range test {
		err := s.validateOnWrongResources(t.resources)
		s.Error(err)
		s.Equal(err.Error(), t.expectedErr)
	}
}

func TestResPoolConfigValidator(t *testing.T) {
	suite.Run(t, new(resPoolConfigValidatorSuite))
}
