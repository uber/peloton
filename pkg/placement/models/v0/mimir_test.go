package models_v0

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssignmentMimir(t *testing.T) {
	t.Run("assignment to entity", func(t *testing.T) {
		_, _, _, _, _, assignment := setupAssignmentVariables()
		// Check the the type casts don't panic.
		entity := assignment.ToMimirEntity()
		require.NotNil(t, entity)
	})
	t.Run("offer to group", func(t *testing.T) {
		_, _, _, host, _, _ := setupAssignmentVariables()
		// Check the the type casts don't panic.
		group := host.ToMimirGroup()
		require.NotNil(t, group)
	})
}
