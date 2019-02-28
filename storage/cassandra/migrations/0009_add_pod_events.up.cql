/*
Pod Events represents state changes for each JobID + InstanceID.
Terminology
Run: Represents an execution cycle for a pod from non-terminal states to RUNNING/terminal state
which has same run_id. Here non-terminal state must start with INITIALIZED (Peloton state) & TASK_STAGING (Mesos state).

Examples for a `run` are:
Run 3: INITIALIZED -> PENDING -> LAUNCHED -> RUNNING
Run 2: INITIALIZED -> PENDING -> LAUNCHED -> RUNNING -> FAILED
Run 1: INITIALIZED -> PENDING -> LAUNCHED -> RUNNING -> KILLING -> KILLED

Example queries for this table are:
1) List last X runs for particular instance of a job. (0 < X <= 100)
2) Get sandbox path for a particular run. Each pod state change for a `run` has hostname and mesos agent id.

Key Considerations for schema design.
- JobID + InstanceID as partition key, as each query is pointed at these values.
- Within a partition, runs are sorted in reverse-chronological order (run_id is an incremental value),
  so most recent records represents current `run`.
- Using LIMIT N, we can fetch last pod state change iterations, and using run_id and previous_run_id,
  `runs` will be created.
- Using LeveledCompactionStrategy, such that each partition is in one SSTable.
- To bound number of runs, setting TTL to 90 days and Data Archiver to trucate more than 100 runs, whichever occurs first.
  Here, 90 day TTL is primarily a safeguard and not choosing a smaller value as we do not want to clean up the current run as well.
  However, if the current run has been running for 90 days without any upgrades, storing this debug information is not useful, so setting it to 90 days.
- GC Grace Period is set to 10 days, so recommendation is to run repair in 10 days.
- unchecked_tombstone_compaction is set to true, such that if a pod is crashlooping and too many tombstones
  are generated then they will be compacted.
*/

CREATE TABLE IF NOT EXISTS pod_events (
    job_id uuid,
    instance_id int,
    run_id bigint,
    update_time timeuuid,
    pod_status blob,
    previous_run_id bigint,
    actual_state text,
    goal_state text,
    hostname text,
    agent_id text,
    config_version bigint,
    desired_config_version bigint,
    volumeID text,
    message text,
    reason text,
    PRIMARY KEY ((job_id, instance_id), run_id, update_time)
) WITH CLUSTERING ORDER BY (run_id DESC, update_time DESC)
    AND bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb': '64', 'unchecked_tombstone_compaction': 'true'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 7776000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0;
