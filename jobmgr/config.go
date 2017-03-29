package jobmgr

// Config is JobManager specific configuration
type Config struct {
	Port int `yaml:"port"`
	// FIXME(gabe): this isnt really the DB write concurrency. This is
	// only used for processing task updates and should be moved into
	// the storage namespace, and made clearer what this controls
	// (threads? rows? statements?)
	DbWriteConcurrency int `yaml:"db_write_concurrency"`

	// PlacementDequeueLimit is the limit which task launcher get the
	// placements
	PlacementDequeueLimit int `yaml:"placement_dequeue_limit"`

	// GetPlacementsTimeout is the timeout value for task launcher to
	// call GetPlacements
	GetPlacementsTimeout int `yaml:"get_placements_timeout_ms"`
}
