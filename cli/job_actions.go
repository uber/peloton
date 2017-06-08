package cli

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"code.uber.internal/infra/peloton/.gen/peloton/api/job"
	"code.uber.internal/infra/peloton/.gen/peloton/api/peloton"
	"code.uber.internal/infra/peloton/.gen/peloton/api/respool"

	"gopkg.in/yaml.v2"
)

const (
	// TODO: use something like ColumnBuilder for the header and
	// format string by taking a list of (header, formatString) pairs
	jobListFormatHeader = "Name\tCPU Limit\tMem Limit\tDisk Limit\t" +
		"Instances\tCommand\t\n"
	jobListFormatBody = "%s\t%.1f\t%.0f MB\t%.0f MB\t%d\t%s\t\n"
	labelSeparator    = ","
	keyValSeparator   = ":"
)

// JobCreateAction is the action for creating a job
func (client *Client) JobCreateAction(jobID string, respoolPath string, cfg string) error {
	respoolID, err := client.LookupResourcePoolID(respoolPath)
	if err != nil {
		return err
	}
	if respoolID == nil {
		return fmt.Errorf("unable to find resource pool ID for "+
			":%s", respoolPath)
	}

	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("Unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("Unable to parse file %s: %v", cfg, err)
	}

	// TODO remove this once respool is moved out of jobconfig
	// set the resource pool ID
	jobConfig.RespoolID = respoolID

	var request = &job.CreateRequest{
		Id: &peloton.JobID{
			Value: jobID,
		},
		Config: &jobConfig,
	}
	response, err := client.jobClient.Create(client.ctx, request)
	if err != nil {
		return err
	}
	printJobCreateResponse(response, client.Debug)
	return nil
}

// JobDeleteAction is the action for deleting a job
func (client *Client) JobDeleteAction(jobName string) error {
	var request = &job.DeleteRequest{
		Id: &peloton.JobID{
			Value: jobName,
		},
	}
	response, err := client.jobClient.Delete(client.ctx, request)
	if err != nil {
		return err
	}
	printResponseJSON(response)
	return nil
}

// JobGetAction is the action for getting a job
func (client *Client) JobGetAction(jobName string) error {
	var request = &job.GetRequest{
		Id: &peloton.JobID{
			Value: jobName,
		},
	}
	response, err := client.jobClient.Get(client.ctx, request)
	if err != nil {
		return err
	}
	printJobGetResponse(response, client.Debug)
	return nil
}

// JobQueryAction is the action for getting job ids by labels and respool path
func (client *Client) JobQueryAction(labels string, respoolPath string, keywords string) error {
	var apiLabels []*peloton.Label
	if len(labels) > 0 {
		labelPairs := strings.Split(labels, labelSeparator)
		for _, l := range labelPairs {
			labelVals := strings.Split(l, keyValSeparator)
			if len(labelVals) != 2 {
				fmt.Printf("Invalid label %v", l)
				return errors.New("Invalid label" + l)
			}
			apiLabels = append(apiLabels, &peloton.Label{
				Key:   labelVals[0],
				Value: labelVals[1],
			})
		}
	}
	var respoolID *respool.ResourcePoolID
	var err error
	if len(respoolPath) > 0 {
		respoolID, err = client.LookupResourcePoolID(respoolPath)
		if err != nil {
			return err
		}
	}
	var apiKeywords []string
	for _, k := range strings.Split(keywords, labelSeparator) {
		if k != "" {
			apiKeywords = append(apiKeywords, k)
		}
	}
	var request = &job.QueryRequest{
		RespoolID: respoolID,
		Spec: &job.QuerySpec{
			Labels:   apiLabels,
			Keywords: apiKeywords,
		},
	}
	response, err := client.jobClient.Query(client.ctx, request)
	if err != nil {
		return err
	}
	printJobQueryResponse(response, client.Debug)
	return nil
}

// JobUpdateAction is the action of updating a job
func (client *Client) JobUpdateAction(jobName string, cfg string) error {
	var jobConfig job.JobConfig
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("Unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("Unable to parse file %s: %v", cfg, err)
	}

	var request = &job.UpdateRequest{
		Id: &peloton.JobID{
			Value: jobName,
		},
		Config: &jobConfig,
	}
	response, err := client.jobClient.Update(client.ctx, request)
	if err != nil {
		return err
	}

	printJobUpdateResponse(response, client.Debug)
	return nil
}

func printJobUpdateResponse(r *job.UpdateResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.Error != nil {
			if r.Error.JobNotFound != nil {
				fmt.Fprintf(tabWriter, "Job %s not found: %s\n",
					r.Error.JobNotFound.Id.Value, r.Error.JobNotFound.Message)
			} else if r.Error.InvalidConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid job config: %s\n",
					r.Error.InvalidConfig.Message)
			}
		} else if r.Id != nil {
			fmt.Fprintf(tabWriter, "Job %s updated\n", r.Id.Value)
			fmt.Fprint(tabWriter, "message:", r.Message)
		}
	}
}

func printJobCreateResponse(r *job.CreateResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.Error != nil {
			if r.Error.AlreadyExists != nil {
				fmt.Fprintf(tabWriter, "Job %s already exists: %s\n",
					r.Error.AlreadyExists.Id.Value, r.Error.AlreadyExists.Message)
			} else if r.Error.InvalidConfig != nil {
				fmt.Fprintf(tabWriter, "Invalid job config: %s\n",
					r.Error.InvalidConfig.Message)
			} else if r.Error.InvalidJobId != nil {
				fmt.Fprintf(tabWriter, "Invalid job ID: %v, message: %v\n",
					r.Error.InvalidJobId.Id.Value,
					r.Error.InvalidJobId.Message)
			}
		} else if r.JobId != nil {
			fmt.Fprintf(tabWriter, "Job %s created\n", r.JobId.Value)
		} else {
			fmt.Fprint(tabWriter, "Missing job ID in job create response\n")
		}
		tabWriter.Flush()
	}
}

func printJobDeleteResponse(r *job.DeleteResponse, debug bool) {
	// TODO: when DeleteResponse has useful fields in it, fill me in!
	// Right now, its completely empty
	if debug {
		printResponseJSON(r)
	} else {
		fmt.Fprintf(tabWriter, "Job deleted\n")
		tabWriter.Flush()
	}
}

func printJobGetResponse(r *job.GetResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		c := r.GetJobInfo().GetConfig()
		if c != nil {
			fmt.Fprint(tabWriter, "Unable to get job config\n")
		} else {
			rs := c.DefaultConfig.Resource
			fmt.Fprintf(tabWriter, jobListFormatHeader)
			fmt.Fprintf(tabWriter, jobListFormatBody,
				c.Name, rs.CpuLimit, rs.MemLimitMb, rs.DiskLimitMb,
				c.InstanceCount, c.DefaultConfig.Command)
		}
		tabWriter.Flush()
	}
}

func printJobQueryResponse(r *job.QueryResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetError() != nil {
			fmt.Fprintf(tabWriter, "Error: %v\n", r.GetError().String())
		} else if len(r.GetRecords()) == 0 {
			fmt.Fprint(tabWriter, "No jobs found.\n", r.GetError().String())
		} else {
			for _, jobID := range r.Records {
				fmt.Fprintf(tabWriter, "%s\n", jobID)
			}
		}
		tabWriter.Flush()
	}
}
