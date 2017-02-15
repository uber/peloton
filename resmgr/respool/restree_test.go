package respool

import (
	"code.uber.internal/infra/peloton/storage/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc"

	"fmt"
	"peloton/api/respool"
	"testing"

	"github.com/stretchr/testify/assert"
)

type RespoolTestSuite struct {
	suite.Suite
	store      *mysql.JobStore
	db         *sqlx.DB
	dispatcher yarpc.Dispatcher
	resTree    Tree
	resPools   map[string]*respool.ResourcePoolConfig
	allNodes   map[string]*Node
	root       *Node
}

func (suite *RespoolTestSuite) SetupTest() {
	suite.resPools = make(map[string]*respool.ResourcePoolConfig)
	suite.allNodes = make(map[string]*Node)
	suite.setUpRespools()
	suite.root = suite.resTree.createTree(nil, RootResPoolID, suite.resPools, suite.allNodes)

}

func (suite *RespoolTestSuite) getResourceConfig() []*respool.ResourceConfig {
	resConfigs := make([]*respool.ResourceConfig, 4)
	resConfigcpu := new(respool.ResourceConfig)
	resConfigcpu.Share = 1
	resConfigcpu.Kind = "cpu"
	resConfigcpu.Reservation = 100
	resConfigcpu.Limit = 1000
	resConfigs[0] = resConfigcpu

	resConfigmem := new(respool.ResourceConfig)
	resConfigmem.Share = 1
	resConfigmem.Kind = "memory"
	resConfigmem.Reservation = 100
	resConfigmem.Limit = 1000
	resConfigs[1] = resConfigmem

	resConfigdisk := new(respool.ResourceConfig)
	resConfigdisk.Share = 1
	resConfigdisk.Kind = "disk"
	resConfigdisk.Reservation = 100
	resConfigdisk.Limit = 1000
	resConfigs[2] = resConfigdisk

	resConfiggpu := new(respool.ResourceConfig)
	resConfiggpu.Share = 1
	resConfiggpu.Kind = "gpu"
	resConfiggpu.Reservation = 2
	resConfiggpu.Limit = 4
	resConfigs[3] = resConfiggpu

	return resConfigs
}

func (suite *RespoolTestSuite) setUpRespools() {
	var parentID respool.ResourcePoolID
	parentID.Value = "root"

	var respoolConfig1 respool.ResourcePoolConfig
	respoolConfig1.Name = "respool1"
	respoolConfig1.Parent = &parentID
	respoolConfig1.Resources = suite.getResourceConfig()
	suite.resPools["respool1"] = &respoolConfig1

	var respoolConfig2 respool.ResourcePoolConfig
	respoolConfig2.Name = "respool2"
	respoolConfig2.Parent = &parentID
	respoolConfig2.Resources = suite.getResourceConfig()
	suite.resPools["respool2"] = &respoolConfig2

	var respoolConfig3 respool.ResourcePoolConfig
	respoolConfig3.Name = "respool3"
	respoolConfig3.Parent = &parentID
	respoolConfig3.Resources = suite.getResourceConfig()
	suite.resPools["respool3"] = &respoolConfig3

	var parent1ID respool.ResourcePoolID
	parent1ID.Value = "respool1"

	var respoolConfig11 respool.ResourcePoolConfig
	respoolConfig11.Name = "respool11"
	respoolConfig11.Parent = &parent1ID
	respoolConfig11.Resources = suite.getResourceConfig()
	suite.resPools["respool11"] = &respoolConfig11

	var respoolConfig12 respool.ResourcePoolConfig
	respoolConfig12.Name = "respool12"
	respoolConfig12.Parent = &parent1ID
	respoolConfig12.Resources = suite.getResourceConfig()
	suite.resPools["respool12"] = &respoolConfig12

	var parent2ID respool.ResourcePoolID
	parent2ID.Value = "respool2"

	var respoolConfig21 respool.ResourcePoolConfig
	respoolConfig21.Name = "respool21"
	respoolConfig21.Parent = &parent2ID
	respoolConfig21.Resources = suite.getResourceConfig()
	suite.resPools["respool21"] = &respoolConfig21

	var respoolConfig22 respool.ResourcePoolConfig
	respoolConfig22.Name = "respool22"
	respoolConfig22.Parent = &parent2ID
	respoolConfig22.Resources = suite.getResourceConfig()
	suite.resPools["respool22"] = &respoolConfig22

}

func (suite *RespoolTestSuite) TearDownTest() {
	fmt.Println("tearing down")
}

func TestPelotonResPool(t *testing.T) {
	suite.Run(t, new(RespoolTestSuite))
}

func (suite *RespoolTestSuite) TestPrintTree() {
	// TODO: serialize the tree and compare it
	suite.resTree.printTree(suite.root)
}

func (suite *RespoolTestSuite) TestGetChildren() {
	list := suite.root.GetChildren()
	suite.Equal(list.Len(), 3)
	n := suite.allNodes["respool1"]
	list = n.GetChildren()
	suite.Equal(list.Len(), 2)
	n = suite.allNodes["respool2"]
	list = n.GetChildren()
	suite.Equal(list.Len(), 2)
}

func (suite *RespoolTestSuite) TestResourceConfig() {
	n := suite.allNodes["respool1"]
	suite.Equal(n.ID, "respool1")
	for _, res := range n.resourceConfigs {
		if res.Kind == "cpu" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "memory" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "disk" {
			assert.Equal(suite.T(), res.Reservation, 100.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 1000.00, "Limit is not equal")
		}
		if res.Kind == "gpu" {
			assert.Equal(suite.T(), res.Reservation, 2.00, "Reservation is not Equal")
			assert.Equal(suite.T(), res.Limit, 4.00, "Limit is not equal")
		}
	}
}
