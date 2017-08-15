package mesos

// Config for Mesos specific configuration
type Config struct {
	Framework *FrameworkConfig `yaml:"framework"`
	ZkPath    string           `yaml:"zk_path"`
	Encoding  string           `yaml:"encoding"`

	// Filename whose one-line content includes the secret.
	// Leaving this empty will result in no-authentication.
	SecretFile string `yaml:"secret_file"`
}

// FrameworkConfig for framework specific configuration
type FrameworkConfig struct {
	User            string  `yaml:"user"`
	Name            string  `yaml:"name"`
	Role            string  `yaml:"role"`
	Principal       string  `yaml:"principal"`
	FailoverTimeout float64 `yaml:"failover_timeout"`
	GPUSupported    bool    `yaml:"gpu_supported"`
}
