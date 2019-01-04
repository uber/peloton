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
