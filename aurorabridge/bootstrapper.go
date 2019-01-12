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

package aurorabridge

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"go.uber.org/yarpc/yarpcerrors"
)

// Bootstrapper performs required bootstrapping to start aurorabridge in a
// cluster.
type Bootstrapper struct {
	config        BootstrapConfig
	respoolClient respool.ResourceManagerYARPCClient
}

// NewBootstrapper creates a new Bootstrapper.
func NewBootstrapper(
	config BootstrapConfig,
	respoolClient respool.ResourceManagerYARPCClient,
) *Bootstrapper {
	config.normalize()
	return &Bootstrapper{config, respoolClient}
}

// BootstrapRespool returns the ResourcePoolID for the configured path if it
// exists, else it creates a new respool using the configured spec.
func (b *Bootstrapper) BootstrapRespool() (*v1peloton.ResourcePoolID, error) {
	log.Info("Boostrapping respool")

	if b.config.RespoolPath == "" {
		return nil, errors.New("no path configured")
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.config.Timeout)
	defer cancel()

	for {
		id, err := b.bootstrapRespool(ctx)
		if err != nil {
			select {
			case <-time.After(b.config.RetryInterval):
				// Retry.
				continue
			case <-ctx.Done():
				// Timed out while waiting to retry.
				return nil, err
			}
		}
		return id, nil
	}
}

func (b *Bootstrapper) bootstrapRespool(
	ctx context.Context,
) (*v1peloton.ResourcePoolID, error) {

	id, err := b.lookupRespoolID(ctx, b.config.RespoolPath)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			id, err = b.createDefaultRespool(ctx)
			if err != nil {
				return nil, fmt.Errorf("create default: %s", err)
			}
			log.WithFields(log.Fields{
				"id": id.GetValue(),
			}).Info("Created default respool")
		} else {
			return nil, fmt.Errorf("lookup %s id: %s", b.config.RespoolPath, err)
		}
	} else {
		log.WithFields(log.Fields{
			"id": id.GetValue(),
		}).Info("Reusing pre-existing respool")
	}
	return &v1peloton.ResourcePoolID{
		Value: id.GetValue(),
	}, nil
}

func (b *Bootstrapper) lookupRespoolID(
	ctx context.Context,
	path string,
) (*v0peloton.ResourcePoolID, error) {

	req := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{Value: path},
	}
	resp, err := b.respoolClient.LookupResourcePoolID(ctx, req)
	if err != nil {
		return nil, err
	}
	rerr := resp.GetError()
	if rerr != nil {
		// Convert these embedded response errors to YARPC errors since this API
		// is getting deprecated.
		if rerr.GetNotFound() != nil {
			return nil, yarpcerrors.NotFoundErrorf(rerr.String())
		}
		return nil, yarpcerrors.UnknownErrorf(rerr.String())
	}
	return resp.GetId(), nil
}

func (b *Bootstrapper) createDefaultRespool(
	ctx context.Context,
) (*v0peloton.ResourcePoolID, error) {

	root, err := b.lookupRespoolID(ctx, "/")
	if err != nil {
		return nil, fmt.Errorf("lookup root id: %s", err)
	}
	req := &respool.CreateRequest{
		Config: &respool.ResourcePoolConfig{
			Name:            strings.TrimPrefix(b.config.RespoolPath, "/"),
			OwningTeam:      b.config.DefaultRespoolSpec.OwningTeam,
			LdapGroups:      b.config.DefaultRespoolSpec.LDAPGroups,
			Description:     b.config.DefaultRespoolSpec.Description,
			Resources:       b.config.DefaultRespoolSpec.Resources,
			Parent:          root,
			Policy:          b.config.DefaultRespoolSpec.Policy,
			ControllerLimit: b.config.DefaultRespoolSpec.ControllerLimit,
			SlackLimit:      b.config.DefaultRespoolSpec.SlackLimit,
		},
	}
	resp, err := b.respoolClient.CreateResourcePool(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create resource pool: %s", err)
	}
	rerr := resp.GetError()
	if rerr != nil {
		return nil, yarpcerrors.UnknownErrorf(rerr.String())
	}
	return resp.GetResult(), nil
}
