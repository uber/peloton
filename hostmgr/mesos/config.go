package mesos

// Config for Mesos specific configuration
type Config struct {
	Framework *FrameworkConfig `yaml:"framework"`
	ZkPath    string           `yaml:"zk_path"`
	Encoding  string           `yaml:"encoding"`
}

// FrameworkConfig for framework specific configuration
type FrameworkConfig struct {
	User                        string  `yaml:"user"`
	Name                        string  `yaml:"name"`
	Role                        string  `yaml:"role"`
	Principal                   string  `yaml:"principal"`
	FailoverTimeout             float64 `yaml:"failover_timeout"`
	GPUSupported                bool    `yaml:"gpu_supported"`
	TaskKillingStateSupported   bool    `yaml:"task_killing_state"`
	PartitionAwareSupported     bool    `yaml:"partition_aware"`
	RevocableResourcesSupported bool    `yaml:"revocable_resources"`
}
