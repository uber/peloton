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

	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
)

// EventStreamAction prints all task status update events present in the event stream.
func (c *Client) EventStreamAction() error {

	resp, err := c.hostMgrClient.GetStatusUpdateEvents(
		c.ctx,
		&hostsvc.GetStatusUpdateEventsRequest{})

	if err != nil {
		fmt.Fprintf(tabWriter, err.Error())
		tabWriter.Flush()
		return nil
	}

	if len(resp.GetEvents()) == 0 {
		fmt.Fprintf(tabWriter, "no status update events present in event stream\n")
		tabWriter.Flush()
		return nil
	}

	printResponseJSON(resp)
	return nil
}
