package backoff

import (
	"time"
)

// Retriable is a function returning an error which can be retried.
type Retriable func() error

// Retry will retry the given function until it succeeded or hit maximum number
// of retries then return last error.
func Retry(f Retriable, p RetryPolicy) error {
	var err error
	var backoff time.Duration

	r := NewRetrier(p)
	for {
		// function executed successfully. no need to retry.
		if err = f(); err == nil {
			return nil
		}

		if backoff = r.NextBackOff(); backoff == done {
			return err
		}

		time.Sleep(backoff)
	}
}
