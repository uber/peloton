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
	Load(context.Context, bool) (*v1peloton.ResourcePoolID, error)
}

type respoolLoader struct {
	config RespoolLoaderConfig
	client respool.ResourceManagerYARPCClient

	// Cached respoolID for lazy lookup.
	mu           sync.Mutex
	respoolID    *v1peloton.ResourcePoolID
	gpuRespoolID *v1peloton.ResourcePoolID
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
func (l *respoolLoader) Load(ctx context.Context, isGpu bool) (*v1peloton.ResourcePoolID, error) {
	if isGpu {
		return l.load(ctx, l.gpuRespoolID, l.config.GPURespoolPath)
	}
	return l.load(ctx, l.respoolID, l.config.RespoolPath)
}

func (l *respoolLoader) load(
	ctx context.Context,
	respoolID *v1peloton.ResourcePoolID,
	respoolPath string) (*v1peloton.ResourcePoolID, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if respoolID == nil {
		// respoolID has not been initialized yet, attempt to bootstrap.
		if respoolPath == "" {
			return nil, errors.New("no path configured")
		}
		for {
			id, err := l.bootstrapRespool(ctx, respoolPath)
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
			respoolID = id
			break
		}
	}
	return &v1peloton.ResourcePoolID{Value: respoolID.GetValue()}, nil
}

func (l *respoolLoader) bootstrapRespool(
	ctx context.Context,
	respoolPath string,
) (*v1peloton.ResourcePoolID, error) {

	id, err := l.lookupRespoolID(ctx, respoolPath)
	if err != nil {
		if yarpcerrors.IsNotFound(err) {
			id, err = l.createDefaultRespool(ctx, respoolPath)
			if err != nil {
				return nil, fmt.Errorf("create respool %s: %s", respoolPath, err)
			}
			log.WithFields(log.Fields{
				"id":      id.GetValue(),
				"respool": respoolPath,
			}).Info("Created respool")
		} else {
			return nil, fmt.Errorf("lookup %s id: %s", respoolPath, err)
		}
	} else {
		log.WithFields(log.Fields{
			"id":      id.GetValue(),
			"respool": respoolPath,
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

// This should be called for only one resource pool.
// Cannot have both default and GPU resource pools auto-created.
func (l *respoolLoader) createDefaultRespool(
	ctx context.Context,
	respoolPath string,
) (*v0peloton.ResourcePoolID, error) {

	root, err := l.lookupRespoolID(ctx, "/")
	if err != nil {
		return nil, fmt.Errorf("lookup root id: %s", err)
	}
	req := &respool.CreateRequest{
		Config: &respool.ResourcePoolConfig{
			Name:            strings.TrimPrefix(respoolPath, "/"),
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
		return nil, fmt.Errorf("create resource pool %s: %s", respoolPath, err)
	}
	rerr := resp.GetError()
	if rerr != nil {
		return nil, yarpcerrors.UnknownErrorf(rerr.String())
	}
	return resp.GetResult(), nil
}
