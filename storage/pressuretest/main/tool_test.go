package main

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPressureTool(t *testing.T) {
	args := strings.Split("-s peloton_test -t 5 -w 5 -h 127.0.0.1 -p 9043", " ")
	errors := run(args)
	assert.Equal(t, 0, len(errors))
}
