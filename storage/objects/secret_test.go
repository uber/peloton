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

package objects

import (
	"context"
	"testing"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
)

type SecretObjectTestSuite struct {
	suite.Suite
}

func (suite *SecretObjectTestSuite) SetupTest() {
}

func TestSecretObjectSuite(t *testing.T) {
	suite.Run(t, new(SecretObjectTestSuite))
}

// TestSecretObject create and get from DB
func (suite *SecretObjectTestSuite) TestSecretObject() {
	jobID := &peloton.JobID{Value: uuid.New()}
	secretID := uuid.New()
	now := time.Now().UTC()

	expectedSecret := NewSecretObject(
		jobID, now, secretID, "some data", "path")

	// write the secret object to DB
	err := testStore.CreateSecret(context.Background(), expectedSecret)
	suite.NoError(err)

	// read secret object from DB
	secret, err := testStore.GetSecret(context.Background(), secretID)
	suite.NoError(err)
	suite.Equal(secret.SecretID, expectedSecret.SecretID)
	suite.Equal(secret.JobID, expectedSecret.JobID)
	suite.Equal(secret.Version, expectedSecret.Version)
	suite.Equal(secret.Valid, expectedSecret.Valid)
	suite.Equal(secret.Data, expectedSecret.Data)
	suite.Equal(secret.Path, expectedSecret.Path)

	// update secret object to DB
	err = testStore.UpdateSecretData(
		context.Background(),
		secretID,
		"new data")
	suite.NoError(err)

	// read secret object from DB
	secret, err = testStore.GetSecret(context.Background(), secretID)
	suite.NoError(err)
	suite.Equal(secret.SecretID, expectedSecret.SecretID)
	suite.Equal(secret.JobID, expectedSecret.JobID)
	suite.Equal(secret.Version, expectedSecret.Version)
	suite.Equal(secret.Valid, expectedSecret.Valid)
	suite.Equal(secret.Path, expectedSecret.Path)
	suite.Equal(secret.Data, "new data")

	// Delete secret object from DB
	err = testStore.DeleteSecret(context.Background(), secretID)
	suite.NoError(err)
	_, err = testStore.GetSecret(context.Background(), secretID)
	suite.Error(err)
	suite.Equal(err, gocql.ErrNotFound)
}
