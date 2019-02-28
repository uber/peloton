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

package peer

import (
	"context"

	"go.uber.org/yarpc/api/peer"
	"go.uber.org/yarpc/api/transport"
)

// Chooser is the interface for a YARPC endpoint that can be updated dynamically.
// This is an implementation of go.uber.org/yarpc/peer/Chooser
type Chooser interface {
	Start() error
	Stop() error
	Choose(context.Context, *transport.Request) (peer.Peer, func(error), error)
	IsRunning() bool
	UpdatePeer(urlString string) error
}
