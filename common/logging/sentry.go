package logging

import (
	"os"

	"github.com/evalphobia/logrus_sentry"
	log "github.com/sirupsen/logrus"
)

const (
	_clusterEnv = "CLUSTER"
)

// SentryConfig is sentry logging specific configuration.
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
	if v := os.Getenv(_clusterEnv); v != "" {
		log.WithField("cluster_tag", v).Info("tag cluster in sentry event.")
		cfg.Tags[_clusterEnv] = v
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

	log.Info("sentry hook added successfully")
	log.AddHook(hook)
}
