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

package statemachine

import (
	"github.com/pkg/errors"
)

// Builder is the state machine builder
type Builder struct {
	statemachine       StateMachine
	rules              map[State]*Rule
	timeoutrules       map[State]*TimeoutRule
	current            State
	name               string
	transitionCallback Callback
}

// NewBuilder creates new state machine builder
func NewBuilder() *Builder {
	return &Builder{
		statemachine: &statemachine{},
		rules:        make(map[State]*Rule),
		timeoutrules: make(map[State]*TimeoutRule),
	}
}

// WithName adds the name to state machine
func (b *Builder) WithName(name string) *Builder {
	b.name = name
	return b
}

// WithCurrentState adds the current state
func (b *Builder) WithCurrentState(current State) *Builder {
	b.current = current
	return b
}

// AddRule adds the rule for state machine
func (b *Builder) AddRule(rule *Rule) *Builder {
	b.rules[rule.From] = rule
	return b
}

// AddTimeoutRule adds the rule for state machine
func (b *Builder) AddTimeoutRule(timeoutRule *TimeoutRule) *Builder {
	b.timeoutrules[timeoutRule.From] = timeoutRule
	return b
}

// WithTransitionCallback adds the transition call back
func (b *Builder) WithTransitionCallback(callback Callback) *Builder {
	b.transitionCallback = callback
	return b
}

// Build builds the state machine
func (b *Builder) Build() (StateMachine, error) {
	var err error
	b.statemachine, err = NewStateMachine(
		b.name,
		b.current,
		b.rules,
		b.timeoutrules,
		b.transitionCallback,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b.statemachine, nil
}
