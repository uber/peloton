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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"github.com/uber/peloton/.gen/peloton/api/v0/respool"
)

// ResourcePoolPathDelim is the resource pool path delimiter
const ResourcePoolPathDelim = "/"

// ResPoolCreateAction is the action for creating a resource pool
func (c *Client) ResPoolCreateAction(respoolPath string, cfgFile string) error {
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
	parentID, err := c.LookupResourcePoolID(parentPath)
	if err != nil {
		return err
	}
	if parentID == nil {
		return errors.Errorf("unable to find resource pool ID "+
			"for parent:%s", parentPath)
	}

	// set parent ID
	respoolConfig.Parent = parentID

	var request = &respool.CreateRequest{
		Config: &respoolConfig,
	}
	response, err := c.resClient.CreateResourcePool(c.ctx, request)
	if err != nil {
		return err
	}
	printResPoolCreateResponse(response, respoolPath, c.Debug)
	return nil
}

// ResPoolUpdateAction is the action for updating an existing resource pool
func (c *Client) ResPoolUpdateAction(
	respoolPath string,
	cfgFile string,
	force bool) error {

	if force {
		confirm := c.AskConfirm("Are you sure you want to force a Resource Pool Update? ")
		if !confirm {
			return nil
		}
	}

	if respoolPath == ResourcePoolPathDelim {
		return errors.New("cannot update root resource pool")
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
	parentID, err := c.LookupResourcePoolID(parentPath)
	if err != nil {
		return err
	}
	if parentID == nil {
		return errors.Errorf("unable to find resource pool ID "+
			"for parent:%s", parentPath)
	}
	// set parent ID
	respoolConfig.Parent = parentID

	respoolID, err := c.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return errors.Errorf("unable to lookup resource pool ID")
	}
	var request = &respool.UpdateRequest{
		Id:     respoolID,
		Config: &respoolConfig,
		Force:  force,
	}
	response, err := c.resClient.UpdateResourcePool(c.ctx, request)
	if err != nil {
		return err
	}
	printResPoolUpdateResponse(response, respoolPath, c.Debug)
	return nil
}

// ResPoolDeleteAction is the action for deleting a resource pool
func (c *Client) ResPoolDeleteAction(respoolPath string) error {
	if respoolPath == ResourcePoolPathDelim {
		return errors.New("cannot delete root resource pool")
	}

	var request = &respool.DeleteRequest{
		Path: &respool.ResourcePoolPath{
			Value: respoolPath,
		},
	}
	response, err := c.resClient.DeleteResourcePool(c.ctx, request)
	if err != nil {
		return err
	}
	printResPoolDeleteResponse(response, respoolPath, c.Debug)
	return nil
}

func readResourcePoolConfig(cfgFile string) (respool.ResourcePoolConfig, error) {
	var respoolConfig respool.ResourcePoolConfig
	buffer, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		return respoolConfig, fmt.Errorf("unable to open file %s: %v",
			cfgFile, err)
	}
	if err := yaml.Unmarshal(buffer, &respoolConfig); err != nil {
		return respoolConfig, fmt.Errorf("unable to parse file %s: %v",
			cfgFile, err)
	}
	return respoolConfig, nil
}

// ResPoolDumpAction dumps the resource pool tree
func (c *Client) ResPoolDumpAction(resPoolDumpFormat string) error {
	response, err := c.resClient.Query(c.ctx, &respool.QueryRequest{})
	if err != nil {
		return err
	}

	return printResPoolDumpResponse(resPoolDumpFormat, response, c.Debug)
}

func printResPoolDumpResponse(resPoolDumpFormat string,
	r *respool.QueryResponse, debug bool) error {
	if debug {
		printResponseJSON(r)
		return nil
	}

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

func printResPoolCreateResponse(r *respool.CreateResponse, respoolPath string, debug bool) {
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
				fmt.Fprintf(tabWriter, "Invalid resource pool config: %s\n",
					r.Error.InvalidResourcePoolConfig.Message,
				)
			}
		} else {
			fmt.Fprintf(tabWriter, "Resource Pool %s created at %s\n",
				r.Result.GetValue(), respoolPath)
		}
		tabWriter.Flush()
	}
}

func printResPoolUpdateResponse(r *respool.UpdateResponse, respoolPath string, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.Error != nil {

			if r.Error.NotFound != nil {
				fmt.Fprintf(
					tabWriter,
					"Resource pool not found: %s\n",
					r.Error.NotFound.Message,
				)
			} else if r.Error.InvalidResourcePoolConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid resource pool config: %s\n",
					r.Error.InvalidResourcePoolConfig.Message,
				)
			}
		} else {
			fmt.Fprintf(tabWriter, "Resource Pool %s updated\n",
				respoolPath)
		}
		tabWriter.Flush()
	}
}

func printResPoolDeleteResponse(r *respool.DeleteResponse, respoolPath string, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.Error != nil {
			if r.Error.NotFound != nil {
				fmt.Fprintf(
					tabWriter,
					"ResPool Not Found: %s\n",
					r.Error.NotFound.Message,
				)
			} else if r.Error.IsBusy != nil {
				fmt.Fprintf(
					tabWriter,
					"ResPool is busy: %s\n",
					r.Error.IsBusy.Message,
				)
			} else if r.Error.IsNotLeaf != nil {
				fmt.Fprintf(
					tabWriter,
					"ResPool is not leaf: %s\n",
					r.Error.IsNotLeaf.Message,
				)
			}
		} else {
			fmt.Fprintf(tabWriter, "Resource Pool %s is deleted \n",
				respoolPath)
		}
		tabWriter.Flush()
	}
}
