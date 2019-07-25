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

package cli

import (
	"fmt"

	adminsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
)

const (
	lockOp   = "lock"
	unlockOp = "unlock"
)

func (c *Client) LockComponents(components []string) error {
	lockRequest := &adminsvc.LockdownRequest{
		Components: getComponentFromStrings(components),
	}

	result, err := c.adminClient.Lockdown(c.ctx, lockRequest)

	printSuccessMessage(lockOp, result.Successes)
	printFailureMessage(lockOp, result.Failures)

	return err
}

func (c *Client) UnlockComponents(components []string) error {
	removeLockdownRequest := &adminsvc.RemoveLockdownRequest{
		Components: getComponentFromStrings(components),
	}

	result, err := c.adminClient.RemoveLockdown(c.ctx, removeLockdownRequest)

	printSuccessMessage(unlockOp, result.Successes)
	printFailureMessage(unlockOp, result.Failures)

	return err
}

func getComponentFromStrings(components []string) []adminsvc.Component {
	var result []adminsvc.Component
	for _, component := range components {
		result = append(result, adminsvc.Component(adminsvc.Component_value[component]))
	}

	return result
}

func printSuccessMessage(op string, successes []adminsvc.Component) {
	for _, success := range successes {
		fmt.Printf("%s on %s succeeded\n", op, success.String())
	}
}

func printFailureMessage(op string, failures []*adminsvc.ComponentFailure) {
	for _, failure := range failures {
		fmt.Printf("%s on %s failed due to: %s\n", op, failure.Component.String(), failure.FailureMessage)
	}
}
