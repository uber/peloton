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
	"encoding/base64"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type SecretInfoObjectTestSuite struct {
	suite.Suite
}

func (suite *SecretInfoObjectTestSuite) SetupTest() {
	setupTestStore()
}

func TestSecretObjectSuite(t *testing.T) {
	suite.Run(t, new(SecretInfoObjectTestSuite))
}

// TestSecretInfoOps tests SecretObject CRUD operations.
func (suite *SecretInfoObjectTestSuite) TestSecretInfoOps() {
	db := NewSecretInfoOps(testStore)
	ctx := context.Background()

	jobID := uuid.New()
	secretID := uuid.New()
	now := time.Now().UTC()
	testSecretStr := "some secrets"
	testSecretByteStr := base64.StdEncoding.
		EncodeToString([]byte(testSecretStr))
	testSecretPath := "some path"

	// CREATE and GET ops.
	err := db.CreateSecret(ctx, jobID, now, secretID, testSecretByteStr, testSecretPath)
	suite.NoError(err)

	secretInfoObj, err := db.GetSecret(ctx, secretID)
	suite.NoError(err)
	suite.Equal(secretInfoObj.JobID, jobID)
	suite.Equal(secretInfoObj.SecretID, secretID)
	suite.Equal(secretInfoObj.Data, testSecretByteStr)
	suite.Equal(secretInfoObj.Path, testSecretPath)

	// UPDATE and GET ops.
	testUpdatedSecretStr := "new secret"
	testUpdatedSecretByteStr := base64.StdEncoding.
		EncodeToString([]byte(testUpdatedSecretStr))

	err = db.UpdateSecretData(ctx, secretID, testUpdatedSecretByteStr)
	suite.NoError(err)

	secretInfoObj, err = db.GetSecret(ctx, secretID)
	suite.NoError(err)
	suite.Equal(secretInfoObj.JobID, jobID)
	suite.Equal(secretInfoObj.SecretID, secretID)
	suite.Equal(secretInfoObj.Data, testUpdatedSecretByteStr)
	suite.Equal(secretInfoObj.Path, testSecretPath)

	// DELETE op.
	err = db.DeleteSecret(ctx, secretID)
	suite.NoError(err)

	// Not found error, because secret is deleted.
	_, err = db.GetSecret(ctx, secretID)
	suite.Error(err)
	suite.True(yarpcerrors.IsNotFound(err))
}
