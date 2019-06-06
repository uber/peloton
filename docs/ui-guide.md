# Peloton UI Guide
Note: (todo) we will add Peloton-UI readme here.

## Peloton UI Common Errors

In Peloton UI, you can view the status of tasks that run on clusters in `Tasks` page. This section is to give you an explanation on the common error codes that you encounter.

Before we directly explain to you the error codes, first we'll empower you by showing you how to view failure logs, so you can examine the cause of the failure yourself first.

### View Error Logs from UI and CLI
- Error Logs from Peloton UI
To view the error log from Peloton UI, click on the instance id of the failed task (located under the “Instance” column; you see cursor turns into a pointer icon when you hover over); detailed stack backtrace can be found in “stderr” file.

- Error Logs from Peloton CLI
Alternatively, the following Peloton CLI command returns the entire task events sequence:

>peloton -z <zookeeper parameter> task events <job_id> <instance_id>

### Error Codes

| **Error** | **Example** | **Explanation** | **Remediation**  |
| ------ | ------ | ------ | ------ |
| Linux Exit Codes | "Command exited with status code 1/52/137/143, etc." | Anything other than exit code 0 suggests that it’s an application error. Here are some [standard linux codes][linuxCodes] interpretations. | It’s recommended that users first fix the error by examining the error log for the failed task. Errors in this category are usually caused within users’ own code.
| RPC Interruption | "Task stop API request"  | In this category, the task was terminated by API Request. | No further action needed.
| Out Of Memory Issues | "Memory limit exceeded: Requested: 67116MB Maximum Used: 67116MB MEMORY STATISTICS: cache 794624 rss 69537611776 rss_huge 0 mapped_file 323584 dirty 0 writeback 0 pgpgin 18086908 pgpgout 1109758 pgfault 18068513 pgmajfault 1556 inactive_anon 0 active_anon 69537595392 inactive_file 196608 active_file ..." | OOM-killer (Out Of Memory Killer) was triggered by linux kernel, because the kernel over allocated memory, and was critically low on memory. | It’s recommended that users avoid running batch jobs that are of heavy memory usage at peak times.
| Unable to Launch Container | "Failed to launch container: Failed to fetch all URIs for container." | This error generally means that the URI, one that associated with the container of the given task, failed to connect; some of potential causes were network failure, hdfs heavy load, etc. | Recommend users to restart the failed task.
| Executor Abnormality | “Executor terminated.”/ "Abnormal executor termination: unknown container" | Mesos executor terminated abnormally; refer to mesos [page][mesosPage] for more info. | We recommend users first examining logs before seeking help.
| Error Opening Sandbox | "The following errors occurred while fetching sandbox: get slave log failed on host due to: HTTP GET failed ... &{404 Not Found 404 HTTP/1.1 1 1" | Mesos has a garbage collection policy, which is typically duration of 7 days. However, in special cases, when disk availability runs lower than its set limit, it can also trigger gc process. If such timeline is critical to you, you can check the gc policy on specific cluster via the following command: `curl -s <host machine>:<port>/flags jq .flags grep gc` | No further action available.

   [linuxCodes]: <http://tldp.org/LDP/abs/html/exitcodes.html>
   [mesosPage]: <http://mesos.apache.org/documentation/latest/task-state-reasons/>
