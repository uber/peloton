// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backoff

import (
	"time"
)

const (
	done time.Duration = -1
)

// Retrier is interface for managing backoff.
type Retrier interface {
	NextBackOff() time.Duration
}

// NewRetrier is used for creating a new instance of Retrier
func NewRetrier(policy RetryPolicy) Retrier {
	return &retrierImpl{
		policy:         policy,
		currentAttempt: 1,
	}
}

type retrierImpl struct {
	policy         RetryPolicy
	currentAttempt int
}

// NextBackOff returns the next delay interval.
func (r *retrierImpl) NextBackOff() time.Duration {
	nextInterval := r.policy.CalculateNextDelay(r.currentAttempt)

	r.currentAttempt++
	return nextInterval
}

// RetryPolicy is interface for defining retry policy.
type RetryPolicy interface {
	CalculateNextDelay(attempts int) time.Duration
}

// NewRetryPolicy is used to create a new instance or RetryPolicy.
func NewRetryPolicy(maxAttempts int, retryInterval time.Duration) RetryPolicy {
	return &retryPolicy{
		maxAttempts:   maxAttempts,
		retryInterval: retryInterval,
	}
}

type retryPolicy struct {
	maxAttempts   int
	retryInterval time.Duration
}

// CalculateNextDelay returns next delay.
func (p *retryPolicy) CalculateNextDelay(attempts int) time.Duration {
	// TODO: add backoff into retry.
	if attempts >= p.maxAttempts {
		return done
	}
	return p.retryInterval
}
