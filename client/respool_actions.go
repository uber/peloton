package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"peloton/api/respool"

	"github.com/pkg/errors"
	"go.uber.org/yarpc"
	"gopkg.in/yaml.v2"
)

// ResPoolCreateAction is the action for creating a job
func (client *Client) ResPoolCreateAction(respoolName string, cfgFile string) error {
	var respoolConfig respool.ResourcePoolConfig
	buffer, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return fmt.Errorf("Unable to open file %s: %v", cfgFile, err)
	}
	if err := yaml.Unmarshal(buffer, &respoolConfig); err != nil {
		return fmt.Errorf("Unable to parse file %s: %v", cfgFile, err)
	}

	var response respool.CreateResponse
	var request = &respool.CreateRequest{
		Id: &respool.ResourcePoolID{
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

// ResPoolDumpAction dumps the resource pool tree
func (client *Client) ResPoolDumpAction(resPoolDumpFormat string) error {
	var response respool.QueryResponse
	request := &respool.QueryRequest{}
	_, err := client.resClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("ResourceManager.Query"),
		request,
		&response,
	)
	if err != nil {
		return err
	}

	err = printResPoolDumpResponse(resPoolDumpFormat, response)
	return err
}

func printResPoolDumpResponse(resPoolDumpFormat string, r respool.QueryResponse) error {
	if r.Error != nil {
		return errors.New("error dumping resource pools")
	}

	// Marshall response to specified format
	out, err := marshall(resPoolDumpFormat, r.ResourcePools)

	if err != nil {
		return err
	}
	// print to stdout
	fmt.Printf("%v\n", string(out))
	return nil
}

// Marshall the data in the desired format
func marshall(
	format string,
	data interface{}) ([]byte, error) {

	switch strings.ToLower(format) {
	case "yaml", "yml":
		return yaml.Marshal(data)
	case "json":
		return json.MarshalIndent(
			data,
			"",
			"  ")
	default:
		return nil, fmt.Errorf("invalid format %s", format)
	}
}

func printResPoolCreateResponse(r respool.CreateResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.Error != nil {

			if r.Error.AlreadyExists != nil {
				fmt.Fprintf(
					tabWriter,
					"Already exists: %s\n",
					r.Error.AlreadyExists.Message,
				)
			} else if r.Error.InvalidResourcePoolConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid resource pool %s config: %s\n",
					r.Error.InvalidResourcePoolConfig.Id,
					r.Error.InvalidResourcePoolConfig.Message,
				)
			}
		} else {
			fmt.Fprintf(tabWriter, "Resource Pool %s created\n", r.Result.Value)
		}
		tabWriter.Flush()
	}
}
