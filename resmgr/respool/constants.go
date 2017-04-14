package respool

const (
	// RootResPoolID is the ID for Root node
	RootResPoolID = "root"
	// RequeueBatchSize defines the batch size in tasks to requeue a
	// job upon leader fail-over
	RequeueBatchSize = uint32(1000)
)
