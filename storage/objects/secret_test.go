package objects

import (
	"context"
	"testing"
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	"code.uber.internal/infra/peloton/storage/cassandra"

	"github.com/gocql/gocql"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ObjectsTestSuite struct {
	suite.Suite
}

func (suite *ObjectsTestSuite) SetupTest() {
}

func TestObjectsTestSuite(t *testing.T) {
	suite.Run(t, new(ObjectsTestSuite))
}

// TestSecretObject create and get from DB
func (suite *ObjectsTestSuite) TestSecretObject() {
	conf := cassandra.MigrateForTest()
	var testScope = tally.NewTestScope("", map[string]string{})

	estore, err := NewCassandraStore(conf, testScope)
	suite.NoError(err)

	jobID := &peloton.JobID{Value: uuid.New()}
	secretID := uuid.New()
	now := time.Now().UTC()

	expectedSecret := NewSecretObject(
		jobID, now, secretID, "some data", "path")

	// write the secret object to DB
	err = estore.CreateSecret(context.Background(), expectedSecret)
	suite.NoError(err)

	// read secret object from DB
	secret, err := estore.GetSecret(context.Background(), secretID)
	suite.NoError(err)
	suite.Equal(secret.SecretID, expectedSecret.SecretID)
	suite.Equal(secret.JobID, expectedSecret.JobID)
	suite.Equal(secret.Version, expectedSecret.Version)
	suite.Equal(secret.Valid, expectedSecret.Valid)
	suite.Equal(secret.Data, expectedSecret.Data)
	suite.Equal(secret.Path, expectedSecret.Path)

	// Delete secret object from DB
	err = estore.DeleteSecret(context.Background(), secretID)
	suite.NoError(err)
	_, err = estore.GetSecret(context.Background(), secretID)
	suite.Error(err)
	suite.Equal(err, gocql.ErrNotFound)
}
