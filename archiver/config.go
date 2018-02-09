package archiver

import (
	"time"
)

const (
	_defaultArchiveInterval      = 24 * time.Hour
	_defaultMaxArchiveEntries    = 100
	_defaultArchiveAge           = 720 * time.Hour
	_defaultPelotonClientTimeout = 20 * time.Second
)

// Config is Archiver specific configuration
type Config struct {
	// HTTP port which Archiver is listening on
	HTTPPort int `yaml:"http_port"`

	// gRPC port which Archiver is listening on
	GRPCPort int `yaml:"grpc_port"`

	// Sleep interval between consecutive archive runs
	ArchiveInterval time.Duration `yaml:"archive_interval"`

	// Maximum number of entries that can be archived on a single run
	MaxArchiveEntries int `yaml:"max_archive_entries"`

	// Minimum age of the jobs to be archived
	ArchiveAge time.Duration `yaml:"archive_age"`

	// Peloton client timeout when querying Peloton API
	PelotonClientTimeout time.Duration `yaml:"peloton_client_timeout"`
}

// Normalize configuration by setting unassigned fields to default values.
func (c *Config) Normalize() {
	if c.ArchiveInterval == 0 {
		c.ArchiveInterval = _defaultArchiveInterval
	}
	if c.MaxArchiveEntries == 0 {
		c.MaxArchiveEntries = _defaultMaxArchiveEntries
	}
	if c.ArchiveAge == 0 {
		c.ArchiveAge = _defaultArchiveAge
	}
	if c.PelotonClientTimeout == 0 {
		c.PelotonClientTimeout = _defaultPelotonClientTimeout
	}
}
