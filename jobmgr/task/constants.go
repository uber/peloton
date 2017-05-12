package task

import (
	"time"
)

const (
	// PelotonJobID is the environment variable name for job ID
	PelotonJobID = "PELOTON_JOB_ID"
	// PelotonInstanceID is the environment variable name for instance ID
	PelotonInstanceID = "PELOTON_INSTANCE_ID"
	// PelotonTaskID is the environment variable name for task ID
	PelotonTaskID = "PELOTON_TASK_ID"

	// Time out for the function to time out
	timeoutFunctionCall = 120 * time.Second
	// Time out for the resmgr queue
	getPlacementsTimeout = 10 * 1000 // 10 Secs
)
