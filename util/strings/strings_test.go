package strings

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateString(t *testing.T) {
	assert.False(t, ValidateString(""))
	assert.True(t, ValidateString("a"))
	assert.True(t, ValidateString("ab"))
}
