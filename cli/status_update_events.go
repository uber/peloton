package cli

import (
	"fmt"

	"code.uber.internal/infra/peloton/.gen/peloton/private/hostmgr/hostsvc"
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
