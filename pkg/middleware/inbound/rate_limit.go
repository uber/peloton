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

package inbound

import (
	"context"
	"strings"

	"go.uber.org/yarpc/api/transport"
	"go.uber.org/yarpc/yarpcerrors"
	"golang.org/x/time/rate"
)

var rateLimitError = yarpcerrors.ResourceExhaustedErrorf("rate limit reached for the endpoint")

type RateLimitInboundMiddleware struct {
	enabled bool

	// key is service Name, value rateLimiter in the
	// same order as defined in RateLimitConfig.Methods
	rateLimits       map[string][]*rateLimiter
	defaultRateLimit *rate.Limiter
}

type rateLimiter struct {
	*rate.Limiter
	// rule is method name with wildcard (e.g. GetJob, Get*, List*, *).
	// if a certain method matches the rule, then that method would use
	// the *rate.Limiter associated with the rule.
	rule string
}

const (
	_ruleSeparator = ":"
	// rule that matches all methods under all services
	_matchAllRule = "*"
)

type TokenBucket struct {
	// Rate for the token bucket rate limit algorithm,
	// If Rate < 0, there would be no rate limit
	Rate rate.Limit
	// Burst for the token bucket rate limit algorithm,
	// If Burst < 0, there would be no rate limit
	Burst int
}

type RateLimitConfig struct {
	Enabled bool
	// Methods is evaluated from top to down, and the first matched
	// config is applied
	Methods []struct {
		Name        string
		TokenBucket `yaml:",inline"`
	}
	// Default is the default rate limit config,
	// if the method called is not defined in Methods field.
	// If not set, there is no rate limit.
	Default *TokenBucket `yaml:",omitempty"`
}

func NewRateLimitInboundMiddleware(config RateLimitConfig) (*RateLimitInboundMiddleware, error) {
	result := &RateLimitInboundMiddleware{rateLimits: make(map[string][]*rateLimiter)}
	if !config.Enabled {
		return result, nil
	}

	result.enabled = config.Enabled
	for _, method := range config.Methods {
		results := strings.Split(method.Name, _ruleSeparator)
		if len(results) != 2 {
			return nil, yarpcerrors.InvalidArgumentErrorf(
				"invalid config for method: %s", method.Name)
		}

		service := results[0]
		rule := results[1]
		result.rateLimits[service] =
			append(result.rateLimits[service],
				&rateLimiter{rule: rule, Limiter: createLimiter(method.Rate, method.Burst)},
			)

	}

	if config.Default == nil {
		// if default is not set, no rate limit for default
		result.defaultRateLimit = createLimiter(rate.Inf, 0)
	} else {
		result.defaultRateLimit = createLimiter(config.Default.Rate, config.Default.Burst)
	}
	return result, nil
}

func createLimiter(r rate.Limit, b int) *rate.Limiter {
	// no rate limit
	if r < 0 || b < 0 {
		return rate.NewLimiter(rate.Inf, 0)
	}

	return rate.NewLimiter(r, b)
}

// Handle checks rate limit quota and invokes underlying handler
func (m *RateLimitInboundMiddleware) Handle(
	ctx context.Context,
	req *transport.Request,
	resw transport.ResponseWriter,
	h transport.UnaryHandler,
) error {
	if !m.allow(req.Procedure) {
		return rateLimitError
	}

	return h.Handle(ctx, req, resw)
}

// HandleOneway checks rate limit quota and invokes underlying handler
func (m *RateLimitInboundMiddleware) HandleOneway(
	ctx context.Context,
	req *transport.Request,
	h transport.OnewayHandler,
) error {
	if !m.allow(req.Procedure) {
		return rateLimitError
	}

	return h.HandleOneway(ctx, req)
}

// HandleStream checks rate limit quota and invokes underlying handler
func (m *RateLimitInboundMiddleware) HandleStream(
	s *transport.ServerStream,
	h transport.StreamHandler,
) error {
	if !m.allow(s.Request().Meta.Procedure) {
		return rateLimitError
	}

	return h.HandleStream(s)
}

// allow returns if a procedure can be called given the rate limit
func (m *RateLimitInboundMiddleware) allow(procedure string) bool {
	// if rate limit is not enabled, always allow a method call
	if !m.enabled {
		return true
	}

	results := strings.Split(procedure, _procedureSeparator)
	service := results[0]
	method := results[1]

	// service is not configured, check if there is
	// default rate limit
	var ok bool
	var rls []*rateLimiter
	if rls, ok = m.rateLimits[service]; !ok {
		return m.defaultRateLimit.Allow()
	}

	// found the service, check if method is configured
	for _, rl := range rls {
		if matchRule(method, rl.rule) {
			return rl.Allow()
		}
	}

	// no rate limit configured,
	return m.defaultRateLimit.Allow()

}

func matchRule(method string, rule string) bool {
	if len(rule) == 0 {
		return false
	}

	if rule == _matchAllRule {
		return true
	}

	if rule[len(rule)-1:] == _matchAllRule {
		return strings.HasPrefix(method, rule[0:len(rule)-1])
	}

	return rule == method
}
