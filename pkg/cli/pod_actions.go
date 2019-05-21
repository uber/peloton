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
	"io/ioutil"
	"net/http"
	"strings"

	v1alphapeloton "github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	podsvc "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod/svc"
)

const (
	podGetEventsV1AlphaFormatHeader = "Pod Id\tDesired Pod Id\tActual State\tDesired State\tJob Version\tDesired Job Version\tHealthy\tHost\tMessage\tReason\tUpdate Time\t\n"
	podGetEventsV1AlphaFormatBody   = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// PodGetCacheAction is the action to get pod status from cache
func (c *Client) PodGetCacheAction(podName string) error {
	resp, err := c.podClient.GetPodCache(
		c.ctx,
		&podsvc.GetPodCacheRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
		})
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

// PodGetEventsV1AlphaAction is the action to get the events of a given pod
func (c *Client) PodGetEventsV1AlphaAction(podName string, podID string) error {
	var request = &podsvc.GetPodEventsRequest{
		PodName: &v1alphapeloton.PodName{
			Value: podName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: podID,
		},
	}

	response, err := c.podClient.GetPodEvents(c.ctx, request)
	if err != nil {
		return err
	}

	printPodGetEventsV1AlphaResponse(response, c.Debug)
	return nil
}

// PodRefreshAction is the action to refresh the pod
func (c *Client) PodRefreshAction(podName string) error {
	resp, err := c.podClient.RefreshPod(
		c.ctx,
		&podsvc.RefreshPodRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
		})
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

// PodLogsGetAction is the action to get logs files for given pod
func (c *Client) PodLogsGetAction(filename string, podName string, podID string) error {
	request := &podsvc.BrowsePodSandboxRequest{
		PodName: &v1alphapeloton.PodName{
			Value: podName,
		},
		PodId: &v1alphapeloton.PodID{
			Value: podID,
		},
	}
	response, err := c.podClient.BrowsePodSandbox(c.ctx, request)
	if err != nil {
		return err
	}

	var filePath string
	for _, path := range response.GetPaths() {
		if strings.HasSuffix(path, filename) {
			filePath = path
		}
	}

	if len(filePath) == 0 {
		return fmt.Errorf(
			"filename:%s not found in sandbox files: %s",
			filename,
			response.GetPaths())
	}

	logFileDownloadURL := fmt.Sprintf(
		"http://%s:%s/files/download?path=%s",
		response.GetHostname(),
		response.GetPort(),
		filePath)

	resp, err := http.Get(logFileDownloadURL)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	fmt.Printf("\n\n%s", body)

	return nil
}

func printPodGetEventsV1AlphaResponse(r *podsvc.GetPodEventsResponse, debug bool) {
	defer tabWriter.Flush()

	if debug {
		printResponseJSON(r)
		return
	}

	fmt.Fprint(tabWriter, podGetEventsV1AlphaFormatHeader)
	for _, event := range r.GetEvents() {
		fmt.Fprintf(
			tabWriter,
			podGetEventsV1AlphaFormatBody,
			event.GetPodId().GetValue(),
			event.GetDesiredPodId().GetValue(),
			event.GetActualState(),
			event.GetDesiredState(),
			event.GetVersion().GetValue(),
			event.GetDesiredVersion().GetValue(),
			event.GetHealthy(),
			event.GetHostname(),
			event.GetMessage(),
			event.GetReason(),
			event.GetTimestamp(),
		)
	}
}

// PodStartAction is the action for starting the pod
func (c *Client) PodStartAction(podName string) error {
	resp, err := c.podClient.StartPod(
		c.ctx,
		&podsvc.StartPodRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
		})
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

// PodRestartAction is the action for restarting the pod
func (c *Client) PodRestartAction(podName string) error {
	resp, err := c.podClient.RestartPod(
		c.ctx,
		&podsvc.RestartPodRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
		})
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

// PodStopAction is the action for stopping the pod
func (c *Client) PodStopAction(podName string) error {
	resp, err := c.podClient.StopPod(
		c.ctx,
		&podsvc.StopPodRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
		})
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

// PodGetAction is the action for getting the info of the pod
func (c *Client) PodGetAction(
	podName string,
	statusOnly bool,
	limit uint32,
) error {
	resp, err := c.podClient.GetPod(
		c.ctx,
		&podsvc.GetPodRequest{
			PodName:    &v1alphapeloton.PodName{Value: podName},
			StatusOnly: statusOnly,
			Limit:      limit,
		},
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

// PodDeleteEvents is the action for deleting events of the pod
func (c *Client) PodDeleteEvents(podName string, podID string) error {
	resp, err := c.podClient.DeletePodEvents(
		c.ctx,
		&podsvc.DeletePodEventsRequest{
			PodName: &v1alphapeloton.PodName{Value: podName},
			PodId:   &v1alphapeloton.PodID{Value: podID},
		},
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
