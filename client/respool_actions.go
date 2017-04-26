package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/yarpc"
	"gopkg.in/yaml.v2"

	"peloton/api/respool"
)

// ResourcePoolPathDelim is the resource pool path delimiter
const ResourcePoolPathDelim = "/"

// ResPoolCreateAction is the action for creating a resource pool
func (client *Client) ResPoolCreateAction(respoolPath string, cfgFile string) error {
	if respoolPath == ResourcePoolPathDelim {
		return errors.New("cannot create root resource pool")
	}

	respoolConfig, err := readResourcePoolConfig(cfgFile)
	if err != nil {
		return err
	}

	if respoolConfig.GetParent() != nil {
		return errors.New("parent should not be supplied in the config")
	}

	respoolName := parseRespoolName(respoolPath)
	if respoolName != respoolConfig.Name {
		return fmt.Errorf("resource pool name in path:%s and "+
			"config:%s don't match", respoolName, respoolConfig.Name)
	}

	parentPath := parseParentPath(respoolPath)
	parentID, err := client.LookupResourcePoolID(parentPath)
	if err != nil {
		return err
	}
	if parentID == nil {
		return errors.Errorf("unable to find resource pool ID "+
			"for parent:%s", parentPath)
	}

	// set parent ID
	respoolConfig.Parent = parentID

	var response respool.CreateResponse
	var request = &respool.CreateRequest{
		Id: &respool.ResourcePoolID{
			Value: respoolPath,
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

func readResourcePoolConfig(cfgFile string) (respool.ResourcePoolConfig, error) {
	var respoolConfig respool.ResourcePoolConfig
	buffer, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return respoolConfig, fmt.Errorf("Unable to open file %s: %v", cfgFile, err)
	}
	if err := yaml.Unmarshal(buffer, &respoolConfig); err != nil {
		return respoolConfig, fmt.Errorf("Unable to parse file %s: %v", cfgFile, err)
	}
	return respoolConfig, nil
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

func parseRespoolName(resourcePoolPath string) string {
	var resourcePoolName string
	resourcePoolPath = strings.TrimSuffix(resourcePoolPath, ResourcePoolPathDelim)
	resourcePools := strings.Split(resourcePoolPath, ResourcePoolPathDelim)
	resourcePoolName = resourcePools[len(resourcePools)-1]
	return resourcePoolName
}

func parseParentPath(resourcePoolPath string) string {
	resourcePoolName := parseRespoolName(resourcePoolPath)
	resourcePoolPath = strings.TrimSuffix(resourcePoolPath, ResourcePoolPathDelim)
	parentPath := strings.TrimSuffix(resourcePoolPath, resourcePoolName)
	return parentPath
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
