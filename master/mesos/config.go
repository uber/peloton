package mesos

// Mesos specific configuration
type Config struct {
	HostPort  string          `yaml:"hostPort"`
	Framework FrameworkConfig `yaml:"framework"`
}

// Framework specific configuration
type FrameworkConfig struct {
	User            string  `yaml:"user"`
	Name            string  `yaml:"name"`
	Role            string  `yaml:"role"`
	Principal       string  `yaml:"principal"`
	FailoverTimeout float64 `yaml:"failoverTimeout"`
	Checkpoint      bool    `yaml:"checkpoint"`
}
