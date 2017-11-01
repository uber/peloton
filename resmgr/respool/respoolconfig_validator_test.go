package respool

import (
	"context"
	"fmt"
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	pb_respool "code.uber.internal/infra/peloton/.gen/peloton/api/respool"

	"code.uber.internal/infra/peloton/common"
	store_mocks "code.uber.internal/infra/peloton/storage/mocks"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type resPoolConfigValidatorSuite struct {
	suite.Suite
	resourceTree Tree
	mockCtrl     *gomock.Controller
}

func (suite *resPoolConfigValidatorSuite) SetupSuite() {
	fmt.Println("setting up resPoolConfigValidatorSuite")
	suite.mockCtrl = gomock.NewController(suite.T())
	mockResPoolStore := store_mocks.NewMockResourcePoolStore(suite.mockCtrl)
	mockResPoolStore.EXPECT().GetAllResourcePools(context.Background()).
		Return(suite.getResPools(), nil).AnyTimes()
	mockJobStore := store_mocks.NewMockJobStore(suite.mockCtrl)
	mockTaskStore := store_mocks.NewMockTaskStore(suite.mockCtrl)
	gomock.InOrder(mockJobStore.EXPECT().GetJobsByStates(context.Background(),
		gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	suite.resourceTree = &tree{
		store:     mockResPoolStore,
		root:      nil,
		metrics:   NewMetrics(tally.NoopScope),
		resPools:  make(map[string]ResPool),
		jobStore:  mockJobStore,
		taskStore: mockTaskStore,
		scope:     tally.NoopScope,
	}
}

func (suite *resPoolConfigValidatorSuite) TearDownSuite() {
	suite.mockCtrl.Finish()
}

func (suite *resPoolConfigValidatorSuite) SetupTest() {
	err := suite.resourceTree.Start()
	suite.NoError(err)
}

func (suite *resPoolConfigValidatorSuite) TearDownTest() {
	err := suite.resourceTree.Stop()
	suite.NoError(err)
}

// Returns resource configs
func (suite *resPoolConfigValidatorSuite) getResourceConfig() []*pb_respool.ResourceConfig {

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
func (suite *resPoolConfigValidatorSuite) getResPools() map[string]*pb_respool.ResourcePoolConfig {

	rootID := peloton.ResourcePoolID{Value: common.RootResPoolID}
	policy := pb_respool.SchedulingPolicy_PriorityFIFO

	return map[string]*pb_respool.ResourcePoolConfig{
		common.RootResPoolID: {
			Name:      common.RootResPoolID,
			Parent:    nil,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool1": {
			Name:      "respool1",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool2": {
			Name:      "respool2",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool3": {
			Name:      "respool3",
			Parent:    &rootID,
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool11": {
			Name:      "respool11",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool12": {
			Name:      "respool12",
			Parent:    &peloton.ResourcePoolID{Value: "respool1"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool21": {
			Name:      "respool21",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
			Policy:    policy,
		},
		"respool22": {
			Name:      "respool22",
			Parent:    &peloton.ResourcePoolID{Value: "respool2"},
			Resources: suite.getResourceConfig(),
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

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateReservationsExceedLimit() {
	mockResourcePoolID := &peloton.ResourcePoolID{Value: "respool33"}
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateResourcePool})

	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)

	suite.EqualError(err, "resource cpu, reservation 50 exceeds limit 10")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateOverrideRoot() {
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateResourcePool})

	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)

	suite.EqualError(err, fmt.Sprintf("cannot override %s", common.RootResPoolID))
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateCycle() {
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateCycle})

	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "resource pool ID: respool33 and parent ID: respool33 cannot be same")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateParentLookupError() {
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})

	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "Resource pool (i_do_not_exist) not found")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateParentChanged() {
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
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)

	suite.EqualError(err, "parent override not allowed, actual root, override respool2")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateParentExceedLimit() {
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
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "resource cpu, limit 99999 exceeds parent limit 1000")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateInvalidResourceKind() {
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
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateParent})
	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "parent respool11 doesn't have resource kind aaa")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateChildrenReservationsError() {
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
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{ValidateChildrenReservations})
	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(
		err,
		"Aggregated child reservation 101 of kind `cpu` exceed parent `respool21` reservations 100",
	)
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_RootValidationReservations() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool3",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	rootResPool, err := suite.resourceTree.Get(mockParentPoolID)
	suite.NoError(err)
	resourcePoolConfig := rootResPool.ResourcePoolConfig()
	resourcePoolConfig.Resources = suite.getResourceConfig()
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err = rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateChildrenReservations,
		},
	)

	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.Error(err)
	suite.Equal(err.Error(), "Aggregated child reservation 300 of kind `cpu` exceed parent `root` reservations 100")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_RootValidationParent() {
	mockResourcePoolID := &peloton.ResourcePoolID{
		Value: "respool3",
	}

	mockParentPoolID := &peloton.ResourcePoolID{
		Value: common.RootResPoolID,
	}
	rootResPool, err := suite.resourceTree.Get(mockParentPoolID)
	suite.NoError(err)
	resourcePoolConfig := rootResPool.ResourcePoolConfig()
	resourcePoolConfig.Resources = suite.getResourceConfig()
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err = rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateParent,
		},
	)

	suite.NoError(err)

	err = rv.Validate(resourcePoolConfigData)
	suite.Error(err)
	suite.Equal(err.Error(), "resource cpu, limit 10001 exceeds parent limit 1000")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_NoPolicy() {
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

	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePool,
		},
	)

	suite.NoError(err)
	suite.EqualValues(mockResourcePoolConfig.Policy, pb_respool.SchedulingPolicy_UNKNOWN)

	err = rv.Validate(resourcePoolConfigData)

	suite.NoError(err)
	suite.EqualValues(mockResourcePoolConfig.Policy, pb_respool.SchedulingPolicy_PriorityFIFO)
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidatePathError() {
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
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
	suite.EqualError(err, "path cannot be empty")

	// nil path
	resourcePoolConfigData = ResourcePoolConfigData{
		Path: nil,
	}
	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "path cannot be nil")

	// invalid path
	mockResourcePath.Value = "infrastructure/compute"
	resourcePoolConfigData.Path = mockResourcePath
	err = rv.Validate(resourcePoolConfigData)
	suite.EqualError(err, "path should begin with /")
}

func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidatePath() {
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateResourcePoolPath,
		},
	)
	suite.NoError(err)

	// valid path
	mockResourcePath := &pb_respool.ResourcePoolPath{
		Value: "/infrastructure/compute",
	}
	resourcePoolConfigData := ResourcePoolConfigData{
		Path: mockResourcePath,
	}
	err = rv.Validate(resourcePoolConfigData)
	suite.NoError(err)

	// root path
	mockResourcePath.Value = "/"
	resourcePoolConfigData.Path = mockResourcePath
	err = rv.Validate(resourcePoolConfigData)
	suite.NoError(err)
}

// tests creating pool with existing name should fail
func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateSiblingsCreate() {
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateSiblings,
		},
	)
	suite.NoError(err)

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
	suite.Error(err)
	suite.Equal("resource pool:respool1 already exists",
		err.Error())
}

// tests renaming pool to existing name should fail
func (suite *resPoolConfigValidatorSuite) TestResourcePoolConfigValidator_ValidateSiblingsUpdate() {
	rv := &resourcePoolConfigValidator{resTree: suite.resourceTree}
	_, err := rv.Register(
		[]ResourcePoolConfigValidatorFunc{
			ValidateSiblings,
		},
	)
	suite.NoError(err)

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
	suite.Error(err)
	suite.Equal("resource pool:respool1 already exists",
		err.Error())
}

func TestResPoolConfigValidator(t *testing.T) {
	suite.Run(t, new(resPoolConfigValidatorSuite))
}
