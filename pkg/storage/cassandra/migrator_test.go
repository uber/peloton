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

package cassandra

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type MigratorTestSuite struct {
	suite.Suite

	migrator *Migrator
}

func TestMigrator(t *testing.T) {
	suite.Run(t, &MigratorTestSuite{})
}

func (suite *MigratorTestSuite) SetupTest() {
	conf := GenerateTestCassandraConfig()

	suite.migrator = &Migrator{
		Config: conf,
	}
}

// TestConnString tests connString functionality
func (suite *MigratorTestSuite) TestConnString() {
	expectedStr := fmt.Sprintf("cassandra://%v:%v/%v?protocol=4&disable_init_host_lookup",
		suite.migrator.Config.CassandraConn.ContactPoints[0],
		suite.migrator.Config.CassandraConn.Port,
		suite.migrator.Config.StoreName)
	expectedStr = strings.Replace(expectedStr, " ", "", -1)
	connStr := suite.migrator.connString()
	suite.Equal(connStr, expectedStr)
}

// TestUpSyncVersion tests UpSync and Version functionality
func (suite *MigratorTestSuite) TestUpSyncVersion() {
	// Perform downSync to make sure keyspace is nuked
	errs := suite.migrator.downSync()
	suite.Len(errs, 0)

	v, err := suite.migrator.Version()
	suite.NoError(err)
	suite.EqualValues(0, v)

	// Do UpSync
	errs = suite.migrator.UpSync()
	suite.Len(errs, 0)

	v, err = suite.migrator.Version()
	suite.NoError(err)
	suite.True(v > 0)
}
