package cached

import (
	"testing"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v0/peloton"
	pbtask "code.uber.internal/infra/peloton/.gen/peloton/api/v0/task"

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
		"State":  pbtask.TaskState_FAILED,
		"Reason": "test2",
		"Ports": map[string]uint32{
			"http": 8080,
		},
		"Revision": &peloton.ChangeLog{
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
		"State":    pbtask.TaskState_UNKNOWN,
		"Reason":   "",
		"Revision": nil,
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
		"State":    pbtask.TaskState_UNKNOWN,
		"Reason":   "",
		"Revision": nil,
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
		"State": "wrong value",
	}))
}
