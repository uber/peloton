package mesos

// Config for mesos specific configuration
type Config struct {
	HostPort  string           `yaml:"host_port"`
	Framework *FrameworkConfig `yaml:"framework"`
	ZkPath    string           `yaml:"zk_path"`
	Encoding  string           `yaml:"encoding"`
}

// FrameworkConfig for framework specific configuration
type FrameworkConfig struct {
	User            string  `yaml:"user"`
	Name            string  `yaml:"name"`
	Role            string  `yaml:"role"`
	Principal       string  `yaml:"principal"`
	FailoverTimeout float64 `yaml:"failover_timeout"`
	Checkpoint      bool    `yaml:"checkpoint"`
	GPUSupported    bool    `yaml:"gpu_supported"`
}
