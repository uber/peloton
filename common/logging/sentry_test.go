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
)

func TestConfigureSentry(t *testing.T) {
	var configureSentryTest = []struct {
		enabled bool
		dsn     string
		tags    map[string]string
	}{
		{
			enabled: false,
			dsn:     "",
			tags:    nil,
		},
		{
			enabled: true,
			dsn:     "http://87d77e2cf0472caa1f52f458f:2064b09aab6240389018224dee@sentry.local.internal/1111",
			tags:    nil,
		},
	}

	for _, tt := range configureSentryTest {
		if tt.dsn == "" && tt.tags == nil {
			ConfigureSentry(nil)
			continue
		} else {
			ConfigureSentry(&SentryConfig{
				Enabled: tt.enabled,
				DSN:     tt.dsn,
				Tags:    tt.tags,
			})
		}
	}
}
