/*
Package archiver contains archiver engine interface and implementation.
It provides a mechanism to archive data from primary cassandra storage to
secondary storage like QueryBuilder or ELK using message queue like Kafka.

It is a standalone component of peloton with one running instance.
The archiver config files are used to provide mainly the following information:
	1. archive_interval: time duration specifying how often the archiver thread runs.
	2. max_archive_entries: Max number of entries that can be archived on a single run
	3. archive_age: Minimum age of the jobs to be archived
This may also have the message queue (kafka) config as well.

By default, the archiver engine will function like this:
	1. Archiver thread wakes up every 24 hours
	2. Archiver thread uses peloton client to make JobQuery API request
	   to jobmgr that queries for jobs that have been completed 30 days ago or earlier.
	3. The job config for these jobs will be sent out as json data to Kafka upstream.
	4. Once the jobconfig is sent to secondary storage via Kafka, the archiver will
	   call the JobDelete API for this job_id
	Outside the scope of this code, the data streamed to kafka will be ingested by
	secondary storage like ELK or query builder.
*/
package archiver
