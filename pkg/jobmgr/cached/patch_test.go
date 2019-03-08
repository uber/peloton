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

package cached

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	pbtask "github.com/uber/peloton/.gen/peloton/api/v0/task"

	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"

	"github.com/stretchr/testify/suite"
)

type PatchTestSuite struct {
	suite.Suite
}

func TestPatch(t *testing.T) {
	suite.Run(t, new(PatchTestSuite))
}

// TestPatch tests patch can patch diff successfully
func (suite *PatchTestSuite) TestPatch() {
	input := &pbtask.RuntimeInfo{
		State:  pbtask.TaskState_SUCCEEDED,
		Reason: "test",
	}
	suite.NoError(patch(input, map[string]interface{}{
		jobmgrcommon.StateField:  pbtask.TaskState_FAILED,
		jobmgrcommon.ReasonField: "test2",
		jobmgrcommon.PortsField: map[string]uint32{
			"http": 8080,
		},
		jobmgrcommon.RevisionField: &peloton.ChangeLog{
			Version: 3,
		},
	}))

	suite.Equal(input.State, pbtask.TaskState_FAILED)
	suite.Equal(input.Reason, "test2")
	suite.Equal(input.Ports["http"], uint32(8080))
	suite.Equal(input.Revision.Version, uint64(3))
}

// TestPatch_Unset tests patch can unset fields
func (suite *PatchTestSuite) TestPatch_Unset() {
	input := &pbtask.RuntimeInfo{
		State:  pbtask.TaskState_SUCCEEDED,
		Reason: "test",
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
	}
	suite.NoError(patch(input, map[string]interface{}{
		jobmgrcommon.StateField:    pbtask.TaskState_UNKNOWN,
		jobmgrcommon.ReasonField:   "",
		jobmgrcommon.RevisionField: nil,
	}))

	suite.Equal(input.State, pbtask.TaskState_UNKNOWN)
	suite.Equal(input.Reason, "")
	suite.Nil(input.Revision)
}

// TestPatch_NonExistentField tests the func fails when
// diff contains non-existent fields
func (suite *PatchTestSuite) TestPatch_NonExistentField() {
	input := &pbtask.RuntimeInfo{
		State:  pbtask.TaskState_SUCCEEDED,
		Reason: "test",
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
	}
	err := patch(input, map[string]interface{}{
		"NonExistent": nil,
	})
	suite.Error(err)
}

// TestPatch_NilEntity tests the func fails when
// entity is nil
func (suite *PatchTestSuite) TestPatch_NilEntity() {
	var input *pbtask.RuntimeInfo
	err := patch(input, map[string]interface{}{
		jobmgrcommon.StateField:    pbtask.TaskState_UNKNOWN,
		jobmgrcommon.ReasonField:   "",
		jobmgrcommon.RevisionField: nil,
	})
	suite.Error(err)
}

// TestPatch_NilDiff tests the func succeeds with
// nil diff
func (suite *PatchTestSuite) TestPatch_NilDiff() {
	input := &pbtask.RuntimeInfo{
		State:  pbtask.TaskState_SUCCEEDED,
		Reason: "test",
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
	}
	suite.NoError(patch(input, nil))

	suite.Equal(input.State, pbtask.TaskState_SUCCEEDED)
	suite.Equal(input.Reason, "test")
	suite.Equal(input.Revision.Version, uint64(3))
}

// TestPatch_WrongType tests the func returns an error
// when type of value in diff is wrong
func (suite *PatchTestSuite) TestPatch_WrongType() {
	input := &pbtask.RuntimeInfo{
		State:  pbtask.TaskState_SUCCEEDED,
		Reason: "test",
		Revision: &peloton.ChangeLog{
			Version: 3,
		},
	}
	suite.Error(patch(input, map[string]interface{}{
		jobmgrcommon.StateField: "wrong value",
	}))
}
