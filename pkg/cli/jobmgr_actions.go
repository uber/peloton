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

	"github.com/uber/peloton/.gen/peloton/private/jobmgrsvc"
)

func (c *Client) JobMgrGetThrottledPods() error {
	resp, err := c.jobmgrClient.GetThrottledPods(
		c.ctx,
		&jobmgrsvc.GetThrottledPodsRequest{},
	)
	if err != nil {
		return err
	}

	out, err := marshallResponse("yaml", resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))
	return nil
}

func (c *Client) JobMgrQueryJobCache(
	labels string,
	name string,
) error {
	pelotonLabels, err := parseLabels(labels)
	if err != nil {
		return err
	}

	spec := &jobmgrsvc.QueryJobCacheRequest_CacheQuerySpec{
		Labels: pelotonLabels,
		Name:   name,
	}

	resp, err := c.jobmgrClient.QueryJobCache(
		c.ctx,
		&jobmgrsvc.QueryJobCacheRequest{
			Spec: spec,
		},
	)

	if err != nil {
		return err
	}

	if len(resp.GetResult()) == 0 {
		fmt.Println("No result")
		return nil
	}

	out, err := marshallResponse("yaml", resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))
	return nil
}
