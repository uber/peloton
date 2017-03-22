package task

import (
	"time"
)

const (
	// Time out for the function to time out
	timeoutFunctionCall = 120 * time.Second
	// Time out for the resmgr queue
	getPlacementsTimeout = 10 * 1000 // 10 Secs
)
