package cli

import (
	"fmt"

	v1alphapeloton "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/peloton"
	podsvc "code.uber.internal/infra/peloton/.gen/peloton/api/v1alpha/pod/svc"
)

const (
	podGetEventsV1AlphaFormatHeader = "Pod Id\tDesired Pod Id\tActual State\tDesired State\tJob Version\tDesired Job Version\tHealthy\tHost\tMessage\tReason\tUpdate Time\t\n"
	podGetEventsV1AlphaFormatBody   = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n"
)

// PodGetCache get pod status from cache"
func (c *Client) PodGetCache(podName string) error {
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

// PodGetEventsV1AlphaAction is the action to get the pod events of a given pod
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
			event.GetJobVersion().GetValue(),
			event.GetDesiredJobVersion().GetValue(),
			event.GetHealthy(),
			event.GetHostname(),
			event.GetMessage(),
			event.GetReason(),
			event.GetTimestamp(),
		)
	}
}
