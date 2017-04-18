package async

import "context"

// Job to be inserted to a Pool or Queue.
type Job interface {
	// Run the Job, provided the given context.
	// TODO: Error result?
	Run(ctx context.Context)
}

// JobFunc is an convenient type for easily converting function literals
// to a Job compatible object.
type JobFunc func(context.Context)

// Run the JobFunc by invoking itself.
func (j JobFunc) Run(ctx context.Context) {
	j(ctx)
}
