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

package offers

import (
	"context"

	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/placement/models"
	"github.com/uber/peloton/pkg/placement/plugins"
)

// Service will manage offers used by any placement strategy.
type Service interface {
	// Acquire fetches a batch of offers from the host manager.
	Acquire(ctx context.Context,
		fetchTasks bool,
		taskType resmgr.TaskType,
		needs plugins.PlacementNeeds,
	) (offers []models.Offer, reason string)

	// Release returns the acquired offers back to host manager.
	Release(ctx context.Context, offers []models.Offer)
}
