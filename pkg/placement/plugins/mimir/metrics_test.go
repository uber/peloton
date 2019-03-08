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

package mimir

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/peloton/pkg/placement/plugins/mimir/lib/model/metrics"
)

func TestDerivation_Calculate(t *testing.T) {
	derivation := free(CPUAvailable, CPUReserved)
	metricSet := metrics.NewSet()
	metricSet.Add(CPUAvailable, 200.0)
	metricSet.Add(CPUReserved, 50.0)
	derivation.Calculate(CPUFree, metricSet)
	assert.Equal(t, 150.0, metricSet.Get(CPUFree))
}

func TestDerivation_Dependencies(t *testing.T) {
	derivation := free(CPUAvailable, CPUReserved)
	assert.Equal(t, []metrics.Type{CPUAvailable, CPUReserved}, derivation.Dependencies())
}
