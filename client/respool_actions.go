package client

import (
	"fmt"
	"io/ioutil"

	"peloton/resmgr"

	"go.uber.org/yarpc"
	"gopkg.in/yaml.v2"
)

// ResPoolCreateAction is the action for creating a job
func (client *Client) ResPoolCreateAction(respoolName string, cfgFile string) error {
	var respoolConfig resmgr.ResourcePoolConfig
	buffer, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return fmt.Errorf("Unable to open file %s: %v", cfgFile, err)
	}
	if err := yaml.Unmarshal(buffer, &respoolConfig); err != nil {
		return fmt.Errorf("Unable to parse file %s: %v", cfgFile, err)
	}

	var response resmgr.CreateResponse
	var request = &resmgr.CreateRequest{
		Id: &resmgr.ResourcePoolID{
			Value: respoolName,
		},
		Config: &respoolConfig,
	}
	_, err = client.resClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("ResourceManager.CreateResourcePool"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printResPoolCreateResponse(response, client.Debug)
	return nil
}

func printResPoolCreateResponse(r resmgr.CreateResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.AlreadyExists != nil {
			fmt.Fprintf(tabWriter, "Resource Pool %s already exists: %s\n", r.AlreadyExists.Id.Value, r.AlreadyExists.Message)
		} else {
			fmt.Fprintf(tabWriter, "Resource Pool %s created\n", r.Result.Value)
		}
		tabWriter.Flush()
	}
}
