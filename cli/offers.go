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

// OffersGetAction prints all the outstanding offers present in Host Manager offer pool.
func (c *Client) OffersGetAction() error {

	resp, _ := c.hostMgrClient.GetOutstandingOffers(
		c.ctx,
		&hostsvc.GetOutstandingOffersRequest{})

	printGetOutstandingOffersResponse(resp)

	return nil
}

func printGetOutstandingOffersResponse(resp *hostsvc.GetOutstandingOffersResponse) {
	if resp.GetError().GetNoOffers() != nil {
		fmt.Fprint(tabWriter, "No outstanding offers are present in offer pool \n")
	} else {
		out, err := marshallResponse(jsonResponseFormat, resp)
		if err != nil {
			fmt.Fprint(tabWriter, "Unable to marshall response \n")
		}
		fmt.Printf("%v\n", string(out))
	}
	tabWriter.Flush()
}
