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

package util

import (
	"sync"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/yarpc/yarpcerrors"
)

type TaskTestSuite struct {
	suite.Suite
}

var countMap = make(map[uint32]uint32)
var mu sync.Mutex

func TestTask(t *testing.T) {
	suite.Run(t, new(TaskTestSuite))
}

// TestRunInParallel tests an action taken on list of instances
// in parallel.
func (suite *TaskTestSuite) TestRunInParallel() {
	instances := []uint32{0, 1, 2, 3, 4}

	// worker should increment value of countMap by 1.
	worker := func(id uint32) error {
		mu.Lock()
		defer mu.Unlock()
		countMap[id] = id
		return nil
	}

	err := RunInParallel(uuid.NewRandom().String(), instances, worker)
	suite.NoError(err)

	for _, i := range instances {
		suite.Equal(i, countMap[i])
	}
}

// TestRunInParallelFail tests failure scenario for running actions
// in parallel.
func (suite *TaskTestSuite) TestRunInParallelFail() {
	instances := []uint32{0, 1, 2, 3, 4}

	// worker should increment value of countMap by 1.
	worker := func(id uint32) error {
		if id == 2 {
			return yarpcerrors.AbortedErrorf("test error")
		}
		return nil
	}
	err := RunInParallel(uuid.NewRandom().String(), instances, worker)
	suite.True(yarpcerrors.IsAborted(err))
}
