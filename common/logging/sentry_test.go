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
