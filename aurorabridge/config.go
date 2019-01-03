package aurorabridge

import (
	"time"

	"code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/respool"
)

// BootstrapConfig defines configuration for boostrapping bridge.
type BootstrapConfig struct {
	Timeout            time.Duration      `yaml:"timeout"`
	RespoolPath        string             `yaml:"respool_path"`
	DefaultRespoolSpec DefaultRespoolSpec `yaml:"default_respool_spec"`
}

// DefaultRespoolSpec defines parameters used to create a default respool for
// bridge when boostrapping a new cluster.
type DefaultRespoolSpec struct {
	OwningTeam      string                   `yaml:"owning_team"`
	LDAPGroups      []string                 `yaml:"ldap_groups"`
	Description     string                   `yaml:"description"`
	Resources       []*respool.ResourceSpec  `yaml:"resources"`
	Policy          respool.SchedulingPolicy `yaml:"policy"`
	ControllerLimit *respool.ControllerLimit `yaml:"controller_limit"`
	SlackLimit      *respool.SlackLimit      `yaml:"slack_limit"`
}

func (c *BootstrapConfig) normalize() {
	if c.Timeout == 0 {
		c.Timeout = 10 * time.Second
	}
}
