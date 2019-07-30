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

package inbound

import (
	"context"
	"sync"

	"github.com/uber/peloton/pkg/common/procedure"

	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
)

const (
	readLabel  = "read"
	writeLabel = "write"
)

// APILockInboundMiddleware implements Read/Write lock on API
type APILockInboundMiddleware struct {
	labelManager *procedure.LabelManager
	*lockState
}

// APILockConfig specifies which APIs are Read/Write
type APILockConfig struct {
	ReadAPIs  []string `yaml:"read_apis"`
	WriteAPIs []string `yaml:"write_apis"`
}

// NewAPILockInboundMiddleware creates new APILockInboundMiddleware
func NewAPILockInboundMiddleware(config *APILockConfig) *APILockInboundMiddleware {
	mw := &APILockInboundMiddleware{}

	mw.lockState = &lockState{}
	mw.labelManager = procedure.NewLabelManager(&procedure.LabelManagerConfig{
		Entries: []*procedure.LabelManagerConfigEntry{
			{Procedures: config.ReadAPIs, Labels: []string{readLabel}},
			{Procedures: config.WriteAPIs, Labels: []string{writeLabel}},
		},
	})

	return mw
}

// APILockInterface enables users to lock a specific set
// of APIs
type APILockInterface interface {
	// Lock read APIs
	LockRead()
	// Lock write APIs
	LockWrite()
	// Unlock read APIs
	UnlockRead()
	// Unlock write APIs
	UnlockWrite()
}

// Handle checks if a request is blocked due to lock down and invokes the underlying handler
func (m *APILockInboundMiddleware) Handle(ctx context.Context, req *transport.Request, resw transport.ResponseWriter, h transport.UnaryHandler) error {
	if err := m.checkLock(req.Procedure); err != nil {
		return err
	}

	return h.Handle(ctx, req, resw)
}

// HandleOneway checks if a request is blocked due to lock down and invokes the underlying handler
func (m *APILockInboundMiddleware) HandleOneway(ctx context.Context, req *transport.Request, h transport.OnewayHandler) error {
	if err := m.checkLock(req.Procedure); err != nil {
		return err
	}

	return h.HandleOneway(ctx, req)
}

// HandleStream checks if a request is blocked due to lock down and invokes the underlying handler
func (m *APILockInboundMiddleware) HandleStream(s *transport.ServerStream, h transport.StreamHandler) error {
	if err := m.checkLock(s.Request().Meta.Procedure); err != nil {
		return err
	}

	return h.HandleStream(s)
}

// lockState represents if API has no lock/ read lock or write lock
type lockState struct {
	sync.RWMutex
	// if first bit is set, it means the API has read lock.
	// if second bit is set, it means the API has write lock
	state int
}

func (m *APILockInboundMiddleware) checkLock(procedure string) error {
	if m.hasNoLock() {
		return nil
	}

	if m.hasReadLock() && m.labelManager.HasLabel(procedure, readLabel) {
		return yarpcerrors.InternalErrorf("All read APIs are locked down")
	}

	if m.hasWriteLock() && m.labelManager.HasLabel(procedure, writeLabel) {
		return yarpcerrors.InternalErrorf("All write APIs are locked down")
	}

	return nil
}

func (l *lockState) LockRead() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state | 0x1
}

func (l *lockState) LockWrite() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state | (0x1 << 1)
}

func (l *lockState) UnlockRead() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state & (^0x1)
}

func (l *lockState) UnlockWrite() {
	l.Lock()
	defer l.Unlock()

	l.state = l.state & (^(0x1 << 1))
}

func (l *lockState) hasNoLock() bool {
	l.RLock()
	defer l.RUnlock()

	return l.state == 0
}

func (l *lockState) hasReadLock() bool {
	l.RLock()
	defer l.RUnlock()

	return (l.state & 0x1) != 0
}

func (l *lockState) hasWriteLock() bool {
	l.RLock()
	defer l.RUnlock()

	return (l.state & (0x1 << 1)) != 0
}
