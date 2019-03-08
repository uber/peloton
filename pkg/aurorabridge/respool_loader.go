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
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	v0peloton "github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
	v1peloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"go.uber.org/yarpc/yarpcerrors"
)

// RespoolLoader lazily loads a resource pool. If the resource pool does not
// exist, it boostraps one with provided defaults.
type RespoolLoader interface {
	Load(context.Context) (*v1peloton.ResourcePoolID, error)
}

type respoolLoader struct {
	config RespoolLoaderConfig
	client respool.ResourceManagerYARPCClient

	// Cached respoolID for lazy lookup.
	mu        sync.Mutex
	respoolID *v1peloton.ResourcePoolID
}

// NewRespoolLoader creates a new RespoolLoader.
func NewRespoolLoader(
	config RespoolLoaderConfig,
	client respool.ResourceManagerYARPCClient,
) RespoolLoader {
	config.normalize()
	return &respoolLoader{
		config: config,
		client: client,
	}
}

// Load lazily loads a respool using the configured configured path if it exists,
// else it creates a new respool using the configured spec.
func (l *respoolLoader) Load(ctx context.Context) (*v1peloton.ResourcePoolID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.respoolID == nil {
		// respoolID has not been initialized yet, attempt to bootstrap.
		if l.config.RespoolPath == "" {
			return nil, errors.New("no path configured")
		}
		for {
			id, err := l.bootstrapRespool(ctx)
			if err != nil {
				select {
				case <-time.After(l.config.RetryInterval):
					// Retry.
					continue
				case <-ctx.Done():
					// Timed out while waiting to retry.
					return nil, err
				}
			}
			l.respoolID = id
			break
		}
	}
	return &v1peloton.ResourcePoolID{Value: l.respoolID.GetValue()}, nil
}

func (l *respoolLoader) bootstrapRespool(
	ctx context.Context,
) (*v1peloton.ResourcePoolID, error) {

	id, err := l.lookupRespoolID(ctx, l.config.RespoolPath)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			id, err = l.createDefaultRespool(ctx)
			if err != nil {
				return nil, fmt.Errorf("create default: %s", err)
			}
			log.WithFields(log.Fields{
				"id": id.GetValue(),
			}).Info("Created default respool")
		} else {
			return nil, fmt.Errorf("lookup %s id: %s", l.config.RespoolPath, err)
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

func (l *respoolLoader) lookupRespoolID(
	ctx context.Context,
	path string,
) (*v0peloton.ResourcePoolID, error) {

	req := &respool.LookupRequest{
		Path: &respool.ResourcePoolPath{Value: path},
	}
	resp, err := l.client.LookupResourcePoolID(ctx, req)
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

func (l *respoolLoader) createDefaultRespool(
	ctx context.Context,
) (*v0peloton.ResourcePoolID, error) {

	root, err := l.lookupRespoolID(ctx, "/")
	if err != nil {
		return nil, fmt.Errorf("lookup root id: %s", err)
	}
	req := &respool.CreateRequest{
		Config: &respool.ResourcePoolConfig{
			Name:            strings.TrimPrefix(l.config.RespoolPath, "/"),
			OwningTeam:      l.config.DefaultRespoolSpec.OwningTeam,
			LdapGroups:      l.config.DefaultRespoolSpec.LDAPGroups,
			Description:     l.config.DefaultRespoolSpec.Description,
			Resources:       l.config.DefaultRespoolSpec.Resources,
			Parent:          root,
			Policy:          l.config.DefaultRespoolSpec.Policy,
			ControllerLimit: l.config.DefaultRespoolSpec.ControllerLimit,
			SlackLimit:      l.config.DefaultRespoolSpec.SlackLimit,
		},
	}
	resp, err := l.client.CreateResourcePool(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("create resource pool: %s", err)
	}
	rerr := resp.GetError()
	if rerr != nil {
		return nil, yarpcerrors.UnknownErrorf(rerr.String())
	}
	return resp.GetResult(), nil
}
