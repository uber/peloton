package metrics

// Config will be contianing the metrics configuration
type Config struct {
	Prometheus *prometheusConfig `yaml:"prometheus"`
	Statsd     *statsdConfig     `yaml:"statsd"`
}

type prometheusConfig struct {
	Enable bool `yaml:"enable"`
}

type statsdConfig struct {
	Enable   bool   `yaml:"enable"`
	Endpoint string `yaml:"endpoint"`
}
