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
	"go.uber.org/yarpc/yarpcerrors"
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/pkg/storage/objects/base"
)

const (
	// default secret version that we use
	secretVersion0 = 0
	// this flag is used to indicate that the secret is valid, it is more
	// forward looking in case we end up revoking secrets.
	secretValid = true
)

// Init to add the secret object instance to the global list of storage objects
func init() {
	Objs = append(Objs, &SecretInfoObject{})
}

// SecretInfoObject corresponds to a peloton secret. All fields should be exported.
// SecretInfoObject contains base.Object which has ORM annotations
// that describe the secret_info table and each column name as well as primary
// key information. This is used by ORM when creating DB queries.
type SecretInfoObject struct {
	// DB specific annotations
	base.Object `cassandra:"name=secret_info, primaryKey=((secret_id), valid)"`
	// SecretID is the ID of the secret being created
	SecretID string `column:"name=secret_id"`
	// JobID of the job for which the secret is created
	JobID string `column:"name=job_id"`
	// Container mount path of this secret
	Path string `column:"name=path"`
	// Secret Data (base64 encoded string)
	Data string `column:"name=data"`
	// Creation time of the secret
	CreationTime time.Time `column:"name=creation_time"`
	// Version of this secret
	Version int64 `column:"name=version"`
	// This flag indicates that the secret is valid or invalid
	Valid bool `column:"name=valid"`
}

// transform will convert all the value from DB into the corresponding type
// in ORM object to be interpreted by base store client
func (o *SecretInfoObject) transform(row map[string]interface{}) {
	o.SecretID = row["secret_id"].(string)
	o.JobID = row["job_id"].(string)
	o.Path = row["path"].(string)
	o.Data = row["data"].(string)
	o.CreationTime = row["creation_time"].(time.Time)
	o.Version = int64(row["version"].(uint64))
	o.Valid = row["valid"].(bool)
}

// SecretInfoOps provides methods for manipulating secret table.
type SecretInfoOps interface {
	// Create inserts the SecretInfoObject in the table.
	CreateSecret(
		ctx context.Context,
		jobID string,
		now time.Time,
		secretID, secretString, secretPath string,
	) error

	// Get retrieves the SecretInfoObject from the table.
	GetSecret(
		ctx context.Context,
		secretID string,
	) (*SecretInfoObject, error)

	// Update modifies the SecretInfoObject in the table.
	UpdateSecretData(
		ctx context.Context,
		secretID, secretString string,
	) error

	// Delete removes the SecretInfoObject from the table.
	DeleteSecret(
		ctx context.Context,
		secretID string,
	) error
}

// secretInfoOps implements SecretInfoOps interface using a particular Store.
type secretInfoOps struct {
	store *Store
}

// NewSecretInfoOps constructs a SecretInfoOps object for provided Store.
func NewSecretInfoOps(s *Store) SecretInfoOps {
	return &secretInfoOps{store: s}
}

// ensure that default implementation (secretInfoOps) satisfies the interface
var _ SecretInfoOps = (*secretInfoOps)(nil)

// NewSecretObject creates a new secret object
func newSecretObject(
	jobID string,
	now time.Time,
	secretID, secretString, secretPath string,
) *SecretInfoObject {
	return &SecretInfoObject{
		SecretID:     secretID,
		JobID:        jobID,
		Version:      secretVersion0,
		Valid:        secretValid,
		Data:         secretString,
		Path:         secretPath,
		CreationTime: now,
	}
}

// ToProto returns the unmarshaled *peloton.Secret
func (s *SecretInfoObject) ToProto() *peloton.Secret {
	return &peloton.Secret{
		Id:   &peloton.SecretID{Value: s.SecretID},
		Path: s.Path,
		Value: &peloton.Secret_Value{
			Data: []byte(s.Data),
		},
	}
}

// CreateSecret creates a secret object in db
func (s *secretInfoOps) CreateSecret(
	ctx context.Context,
	jobID string,
	now time.Time,
	secretID, secretString, secretPath string,
) error {
	obj := newSecretObject(jobID, now, secretID, secretString, secretPath)

	if err := s.store.oClient.Create(ctx, obj); err != nil {
		s.store.metrics.OrmJobMetrics.SecretInfoCreateFail.Inc(1)
		return err
	}
	s.store.metrics.OrmJobMetrics.SecretInfoCreate.Inc(1)
	return nil
}

// GetSecret gets a secret object from db
func (s *secretInfoOps) GetSecret(
	ctx context.Context,
	secretID string,
) (*SecretInfoObject, error) {
	secretInfoObject := &SecretInfoObject{
		SecretID: secretID,
		Valid:    true,
	}
	row, err := s.store.oClient.Get(ctx, secretInfoObject)
	if err != nil {
		s.store.metrics.OrmJobMetrics.SecretInfoGetFail.Inc(1)
		return nil, err
	}

	if len(row) == 0 {
		return nil, yarpcerrors.NotFoundErrorf(
			"Secret is not found %s", secretID)
	}
	secretInfoObject.transform(row)
	s.store.metrics.OrmJobMetrics.SecretInfoGet.Inc(1)
	return secretInfoObject, nil
}

// UpdateSecretData updates a secret data in db
func (s *secretInfoOps) UpdateSecretData(
	ctx context.Context,
	secretID, secretString string,
) error {
	secretInfoObject := &SecretInfoObject{
		SecretID: secretID,
		Valid:    true,
		Data:     secretString,
	}
	fieldToUpdate := []string{"Data"}
	if err := s.store.oClient.Update(ctx, secretInfoObject, fieldToUpdate...); err != nil {
		s.store.metrics.OrmJobMetrics.SecretInfoUpdateFail.Inc(1)
		return err
	}
	s.store.metrics.OrmJobMetrics.SecretInfoUpdate.Inc(1)
	return nil
}

// DeleteSecret deletes a secret object in db
func (s *secretInfoOps) DeleteSecret(
	ctx context.Context,
	secretID string,
) error {
	secretInfoObject := &SecretInfoObject{
		SecretID: secretID,
		Valid:    true,
	}
	if err := s.store.oClient.Delete(ctx, secretInfoObject); err != nil {
		s.store.metrics.OrmJobMetrics.SecretInfoDeleteFail.Inc(1)
		return err
	}
	s.store.metrics.OrmJobMetrics.SecretInfoDelete.Inc(1)
	return nil
}
