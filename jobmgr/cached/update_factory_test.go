package cached

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbupdate "code.uber.internal/infra/peloton/.gen/peloton/api/v0/update"

	"code.uber.internal/infra/peloton/common/lifecycle"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type UpdateFactoryTestSuite struct {
	suite.Suite
	ctrl *gomock.Controller
}

func TestUpdateFactory(t *testing.T) {
	suite.Run(t, new(UpdateFactoryTestSuite))
}

func (suite *UpdateFactoryTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
}

func (suite *UpdateFactoryTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

// TestInitUpdateFactory tests initialization of the update factory
func (suite *UpdateFactoryTestSuite) TestInitUpdateFactory() {
	f := InitUpdateFactory(nil, nil, nil, nil, tally.NoopScope)
	suite.NotNil(f)
}

// TestAddAndGetAndClearUpdate tests adding, getting and
// clearing of a job update in the factory.
func (suite *UpdateFactoryTestSuite) TestAddAndGetAndClearUpdate() {
	updateID := &peloton.UpdateID{Value: uuid.NewRandom().String()}

	f := &updateFactory{
		updates:   map[string]*update{},
		lifeCycle: lifecycle.NewLifeCycle(),
	}

	suite.Nil(f.GetUpdate(updateID))

	u := f.AddUpdate(updateID)
	suite.NotNil(u)

	suite.Equal(u, f.GetUpdate(updateID))
	suite.Equal(u, f.AddUpdate(updateID))

	f.ClearUpdate(updateID)
	suite.Nil(f.GetUpdate(updateID))
}

// TestStartStop tests starting and then stopping the factory.
func (suite *UpdateFactoryTestSuite) TestStartStop() {
	f := &updateFactory{
		updates:   map[string]*update{},
		mtx:       NewMetrics(tally.NoopScope),
		lifeCycle: lifecycle.NewLifeCycle(),
	}

	f.Start()
	updateID := &peloton.UpdateID{Value: uuid.NewRandom().String()}

	f.AddUpdate(updateID)
	suite.Equal(1, len(f.GetAllUpdates()))

	f.Stop()
	suite.Nil(f.GetUpdate(updateID))
}

// TestPublishMetrics tests publishing metrics from the update factory.
func (suite *UpdateFactoryTestSuite) TestPublishMetrics() {
	f := &updateFactory{
		updates:   map[string]*update{},
		mtx:       NewMetrics(tally.NoopScope),
		lifeCycle: lifecycle.NewLifeCycle(),
	}

	updateID := &peloton.UpdateID{Value: uuid.NewRandom().String()}
	f.AddUpdate(updateID)
	u := f.updates[updateID.GetValue()]
	u.state = pbupdate.State_ROLLING_FORWARD
	f.publishMetrics()
}
