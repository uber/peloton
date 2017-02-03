package client

import (
	"fmt"
	"io/ioutil"

	pj "peloton/job"

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
func (client *Client) JobCreateAction(jobName string, cfg string) error {
	var jobConfig pj.JobConfig
	buffer, err := ioutil.ReadFile(cfg)
	if err != nil {
		return fmt.Errorf("Unable to open file %s: %v", cfg, err)
	}
	if err := yaml.Unmarshal(buffer, &jobConfig); err != nil {
		return fmt.Errorf("Unable to parse file %s: %v", cfg, err)
	}

	var response pj.CreateResponse
	var request = &pj.CreateRequest{
		Id: &pj.JobID{
			Value: jobName,
		},
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
	var response pj.DeleteResponse
	var request = &pj.DeleteRequest{
		Id: &pj.JobID{
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
	var response pj.GetResponse
	var request = &pj.GetRequest{
		Id: &pj.JobID{
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

func printJobCreateResponse(r pj.CreateResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.AlreadyExists != nil {
			fmt.Fprintf(tabWriter, "Job %s already exists: %s\n",
				r.AlreadyExists.Id.Value, r.AlreadyExists.Message)
		} else if r.InvalidConfig != nil {
			fmt.Fprintf(tabWriter, "Invalid job config: %s\n",
				r.InvalidConfig.Message)
		} else if r.Result != nil {
			fmt.Fprintf(tabWriter, "Job %s created\n", r.Result.Value)
		} else {
			fmt.Fprintf(tabWriter, "Missing result in job create response\n")
		}
		tabWriter.Flush()
	}
}

func printJobDeleteResponse(r pj.DeleteResponse, debug bool) {
	// TODO: when DeleteResponse has useful fields in it, fill me in!
	// Right now, its completely empty
	if debug {
		printResponseJSON(r)
	} else {
		fmt.Fprintf(tabWriter, "Job deleted\n")
		tabWriter.Flush()
	}
}

func printJobGetResponse(r pj.GetResponse, debug bool) {
	if debug {
		printResponseJSON(r)
	} else {
		if r.GetResult() == nil {
			fmt.Fprintf(tabWriter, "Unable to get job\n")
		} else {
			rs := r.Result.DefaultConfig.Resource
			fmt.Fprintf(tabWriter, jobListFormatHeader)
			fmt.Fprintf(tabWriter, jobListFormatBody,
				r.Result.Name, rs.CpusLimit, rs.MemLimitMb, rs.DiskLimitMb,
				r.Result.InstanceCount, r.Result.DefaultConfig.Command)
		}
		tabWriter.Flush()
	}
}
