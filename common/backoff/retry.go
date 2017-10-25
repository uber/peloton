package backoff

import (
	"time"
)

// Retryable is a function returning an error which can be retried.
type Retryable func() error

// IsErrorRetryable could be used to exclude certain errors during retry
type IsErrorRetryable func(error) bool

// Retry will retry the given function until it succeeded or hit maximum number
// of retries then return last error.
func Retry(f Retryable, p RetryPolicy, isRetryable IsErrorRetryable) error {
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

		if isRetryable != nil && !isRetryable(err) {
			return err
		}

		time.Sleep(backoff)
	}
}

// CheckRetry checks if retry is allowed, and if it is allowed,
// it will sleep for the backoff duration. It is used when the
// function to be retried takes in arguments or returns a result.
func CheckRetry(r Retrier) bool {
	var backoff time.Duration

	if backoff = r.NextBackOff(); backoff == done {
		return false
	}
	time.Sleep(backoff)
	return true
}
