package logging

import (
	"github.com/evalphobia/logrus_sentry"
	log "github.com/sirupsen/logrus"
)

// SentryConfig is sentry logging specific
type SentryConfig struct {
	Enabled bool `yaml:"enabled"`
	// DSN is the sentry DSN name.
	DSN string `yaml:"dsn"`
	// Tags are forwarded to the raven client, and enables sentry logs to be
	// filtered by the given tags.
	Tags map[string]string
}

// ConfigureSentry add sentry hook into logger.
func ConfigureSentry(cfg *SentryConfig) {
	if cfg == nil || !cfg.Enabled {
		log.Debug("skip configuring sentry due to not enabled.")
		return
	}
	log.Debug("Adding Sentry hook to logrus")

	if cfg.Tags == nil {
		cfg.Tags = make(map[string]string)
	}

	levels := []log.Level{
		log.PanicLevel,
		log.FatalLevel,
		log.ErrorLevel,
		log.WarnLevel,
	}
	hook, err := logrus_sentry.NewWithTagsSentryHook(cfg.DSN, cfg.Tags, levels)
	if err != nil {
		log.WithError(err).Fatal("Failed to create Sentry hook")
	}

	log.Debug("sentry hook added successfully")
	log.AddHook(hook)
}
