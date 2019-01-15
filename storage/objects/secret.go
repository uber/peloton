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
	"time"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/storage/objects/base"
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
	Objs = append(Objs, &SecretObject{})
}

// SecretObject corresponds to a peloton secret. All fields should be exported.
// SecretObject contains base.Object which has ORM annotations
// that describe the secret_info table and each column name as well as primary
// key information. This is used by ORM when creating DB queries.
type SecretObject struct {
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

// NewSecretObject creates a new secret object
func NewSecretObject(
	id *peloton.JobID,
	now time.Time,
	secretID, secretString, secretPath string,
) *SecretObject {
	return &SecretObject{
		SecretID:     secretID,
		JobID:        id.GetValue(),
		Version:      secretVersion0,
		Valid:        secretValid,
		Data:         secretString,
		Path:         secretPath,
		CreationTime: now,
	}
}

// ToProto returns the unmarshaled *peloton.Secret
func (s *SecretObject) ToProto() *peloton.Secret {
	return &peloton.Secret{
		Id:   &peloton.SecretID{Value: s.SecretID},
		Path: s.Path,
		Value: &peloton.Secret_Value{
			Data: []byte(s.Data),
		},
	}
}

// CreateSecret creates a secret object in db
func (s *Store) CreateSecret(
	ctx context.Context,
	secretObject *SecretObject,
) error {
	return s.oClient.Create(ctx, secretObject)
}

// GetSecret gets a secret object from db
func (s *Store) GetSecret(
	ctx context.Context,
	secretID string,
) (*SecretObject, error) {
	secretObject := &SecretObject{
		SecretID: secretID,
		Valid:    true,
	}
	err := s.oClient.Get(ctx, secretObject)
	return secretObject, err
}

// UpdateSecretData updates a secret data in db
func (s *Store) UpdateSecretData(
	ctx context.Context,
	secretID, secretString string,
) error {
	secretObject := &SecretObject{
		SecretID: secretID,
		Valid:    true,
		Data:     secretString,
	}
	return s.oClient.Update(ctx, secretObject, "Data")
}

// DeleteSecret deletes a secret object in db
func (s *Store) DeleteSecret(
	ctx context.Context,
	secretID string,
) error {
	secretObject := &SecretObject{
		SecretID: secretID,
		Valid:    true,
	}
	return s.oClient.Delete(ctx, secretObject)
}
