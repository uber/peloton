package adminsvc

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"

	lifecyclemgrmocks "github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
)

type taskLifeCycleComponentTestSuite struct {
	suite.Suite

	launchLockComponent *launchLockComponent
	killLockComponent   *killLockComponent

	ctrl     *gomock.Controller
	lockable *lifecyclemgrmocks.MockLockable
}

func (suite *taskLifeCycleComponentTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.lockable = lifecyclemgrmocks.NewMockLockable(suite.ctrl)
	suite.launchLockComponent = &launchLockComponent{lockable: suite.lockable}
	suite.killLockComponent = &killLockComponent{lockable: suite.lockable}
}

func (suite *taskLifeCycleComponentTestSuite) TearDownTest() {
	suite.ctrl.Finish()
}

func TestTaskLifeCycleComponentTestSuite(t *testing.T) {
	suite.Run(t, new(taskLifeCycleComponentTestSuite))
}

func (suite *taskLifeCycleComponentTestSuite) TestLaunchLockComponent() {
	suite.lockable.EXPECT().LockLaunch()
	suite.lockable.EXPECT().UnlockLaunch()

	suite.NoError(suite.launchLockComponent.lock())
	suite.NoError(suite.launchLockComponent.unlock())
	suite.Equal(suite.launchLockComponent.component(), svc.Component_Launch)
}

func (suite *taskLifeCycleComponentTestSuite) TestKillLockComponent() {
	suite.lockable.EXPECT().LockKill()
	suite.lockable.EXPECT().UnlockKill()

	suite.NoError(suite.killLockComponent.lock())
	suite.NoError(suite.killLockComponent.unlock())
	suite.Equal(suite.killLockComponent.component(), svc.Component_Kill)
}
