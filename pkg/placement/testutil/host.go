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

package testutil

import (
	"time"

	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	"github.com/uber/peloton/pkg/placement/models/v0"
	"github.com/uber/peloton/pkg/placement/testutil/v0"
)

// SetupHostOffers creates an host offer.
func SetupHostOffers() *models_v0.HostOffers {
	hostOffer := v0_testutil.SetupHostOffer()
	return models_v0.NewHostOffers(hostOffer, []*resmgr.Task{}, time.Now())
}
