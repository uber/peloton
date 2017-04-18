package async

import (
	"context"
	"testing"
)

func TestJobFuncToJobConversion(t *testing.T) {
	var job Job
	job = JobFunc(func(ctx context.Context) {})
	job.Run(context.Background())
}
