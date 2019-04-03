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

package logging

import (
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLogFieldFormatterFormat(t *testing.T) {
	logFields := log.Fields{
		"dk1": "dv1",
		"dk2": "dv2",
	}

	formatter := LogFieldFormatter{Fields: logFields, Formatter: &log.JSONFormatter{}}
	b, err := formatter.Format(log.WithField("k1", "v1"))
	assert.NoError(t, err)

	s := string(b)
	assert.Contains(t, s, "\"dk1\":\"dv1\"")
	assert.Contains(t, s, "\"dk2\":\"dv2\"")
	assert.Contains(t, s, "\"k1\":\"v1\"")
}
