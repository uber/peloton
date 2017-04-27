package client

import (
	"fmt"
	"io/ioutil"

	"peloton/api/job"
	"peloton/api/peloton"

	"go.uber.org/yarpc"
	"gopkg.in/yaml.v2"
)

const (
	// TODO: use something like ColumnBuilder for the header and
	// format string by taking a list of (header, formatString) pairs
	jobListFormatHeader = "Name\tCPU Limit\tMem Limit\tDisk Limit\t" +
		"Instances\tCommand\t\n"
	jobListFormatBody = "%s\t%.1f\t%.0f MB\t%.0f MB\t%d\t%s\t\n"
)

// JobCreateAction is the action for creating a job
func (client *Client) JobCreateAction(respoolPath string, cfg string) error {
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

	var response job.CreateResponse
	var request = &job.CreateRequest{
		Config: &jobConfig,
	}
	_, err = client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("JobManager.Create"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printJobCreateResponse(response, client.Debug)
	return nil
}

// JobDeleteAction is the action for deleting a job
func (client *Client) JobDeleteAction(jobName string) error {
	var response job.DeleteResponse
	var request = &job.DeleteRequest{
		Id: &peloton.JobID{
			Value: jobName,
		},
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("JobManager.Delete"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printResponseJSON(response)
	return nil
}

// JobGetAction is the action for getting a job
func (client *Client) JobGetAction(jobName string) error {
	var response job.GetResponse
	var request = &job.GetRequest{
		Id: &peloton.JobID{
			Value: jobName,
		},
	}
	_, err := client.jobClient.Call(
		client.ctx,
		yarpc.NewReqMeta().Procedure("JobManager.Get"),
		request,
		&response,
	)
	if err != nil {
		return err
	}
	printJobGetResponse(response, client.Debug)
	return nil
}

func printJobCreateResponse(r job.CreateResponse, debug bool) {
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
			fmt.Fprintf(tabWriter, "Missing job ID in job create response\n")
		}
		tabWriter.Flush()
	}
}

func printJobDeleteResponse(r job.DeleteResponse, debug bool) {
	// TODO: when DeleteResponse has useful fields in it, fill me in!
	// Right now, its completely empty
	if debug {
		printResponseJSON(r)
	} else {
		fmt.Fprintf(tabWriter, "Job deleted\n")
		tabWriter.Flush()
	}
}

func printJobGetResponse(r job.GetResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetConfig() == nil {
			fmt.Fprint(tabWriter, "Unable to get job config\n")
		} else {
			rs := r.Config.DefaultConfig.Resource
			fmt.Fprintf(tabWriter, jobListFormatHeader)
			fmt.Fprintf(tabWriter, jobListFormatBody,
				r.Config.Name, rs.CpuLimit, rs.MemLimitMb, rs.DiskLimitMb,
				r.Config.InstanceCount, r.Config.DefaultConfig.Command)
		}
		tabWriter.Flush()
	}
}
