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
	"io"
	"strings"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/watch"
	watchsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/watch/svc"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"go.uber.org/yarpc/yarpcerrors"
)

// WatchEventStreamEvents is the action for starting a watch stream for mesos task update events
func (c *Client) WatchEventStreamEvents(topicToWatch string) error {
	stream, err := c.hostMgrClient.WatchEventStreamEvent(
		c.ctx,
		&hostsvc.WatchEventRequest{Topic: topicToWatch},
	)
	if err != nil {
		fmt.Println("watch stream for the eventstream has  failed")
		return nil
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Println("watch stream has ended")
			return err
		}

		out, err := marshallResponse(defaultResponseFormat, resp)
		if err != nil {
			return err
		}

		fmt.Printf("%v\n", string(out))
		tabWriter.Flush()
	}
}

// WatchHostSummaryEvent is the action for starting a watch stream for host summary events
func (c *Client) WatchHostSummaryEvent(topicToWatch string) error {
	stream, err := c.hostMgrClient.WatchHostSummaryEvent(
		c.ctx,
		&hostsvc.WatchEventRequest{Topic: topicToWatch},
	)
	if err != nil {
		fmt.Println("watch stream for the host summary event failed")
		return nil
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Println("watch stream has ended")
			return err
		}

		out, err := marshallResponse(defaultResponseFormat, resp)
		if err != nil {
			return err
		}

		fmt.Printf("%v\n", string(out))
		tabWriter.Flush()
	}
}

func (c *Client) createLabelFilter(labels []string) ([]*peloton.Label, error) {
	var labelFilter []*peloton.Label

	for _, label := range labels {
		keyValue := strings.Split(label, ":")
		if len(keyValue) != 2 {
			return nil, yarpcerrors.InvalidArgumentErrorf("unable to parse label %v", label)
		}
		labelFilter = append(labelFilter, &peloton.Label{
			Key:   keyValue[0],
			Value: keyValue[1],
		})
	}

	return labelFilter, nil
}

func (c *Client) watchStream(stream watchsvc.WatchServiceServiceWatchYARPCClient) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			fmt.Println("watch stream has ended")
			return nil
		}

		if err != nil {
			return err
		}

		out, err := marshallResponse(defaultResponseFormat, resp)
		if err != nil {
			return err
		}

		fmt.Printf("%v\n", string(out))
		tabWriter.Flush()
	}
}

// WatchJob is the action for starting a watch stream for job, specified
// by job ids.
func (c *Client) WatchJob(jobIDs []string, labels []string) error {
	var js []*peloton.JobID

	for _, j := range jobIDs {
		js = append(js, &peloton.JobID{
			Value: j,
		})
	}

	labelFilter, err := c.createLabelFilter(labels)
	if err != nil {
		return err
	}

	stream, err := c.watchClient.Watch(
		c.ctx,
		&watchsvc.WatchRequest{
			StatelessJobFilter: &watch.StatelessJobFilter{
				JobIds: js,
				Labels: labelFilter,
			},
		},
	)
	if err != nil {
		return nil
	}

	return c.watchStream(stream)
}

// WatchPod is the action for starting a watch stream for pod, specified
// by job id and pod names.
func (c *Client) WatchPod(jobID string, podNames []string, labels []string) error {
	var j *peloton.JobID
	if jobID != "" {
		j = &peloton.JobID{
			Value: jobID,
		}
	}

	var ps []*peloton.PodName
	for _, p := range podNames {
		ps = append(ps, &peloton.PodName{
			Value: p,
		})
	}

	labelFilter, err := c.createLabelFilter(labels)
	if err != nil {
		return err
	}

	stream, err := c.watchClient.Watch(
		c.ctx,
		&watchsvc.WatchRequest{
			PodFilter: &watch.PodFilter{
				JobId:    j,
				PodNames: ps,
				Labels:   labelFilter,
			},
		},
	)
	if err != nil {
		return nil
	}

	return c.watchStream(stream)
}

// CancelWatch is the action for cancelling an existing watch stream.
func (c *Client) CancelWatch(watchID string) error {
	resp, err := c.watchClient.Cancel(
		c.ctx,
		&watchsvc.CancelRequest{WatchId: watchID},
	)
	if err != nil {
		return err
	}

	out, err := marshallResponse(defaultResponseFormat, resp)
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", string(out))
	tabWriter.Flush()

	return nil
}
