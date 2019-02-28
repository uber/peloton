/*
  active_jobs table contains active jobs at any given time and is used for job recovery
  We would use synthetic sharding. We will start with one partition with shard_id = 0,
  and if we find that the partition size is increasing a lot (20k+ active jobs), we will
  come up with a sharding strategy, example: Use the first byte of job_id as shard_id.
 */
CREATE TABLE IF NOT EXISTS active_jobs (
  shard_id          int,
  job_id            uuid,
  PRIMARY KEY (shard_id, job_id)
) WITH bloom_filter_fp_chance = 0.1
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb': '64', 'unchecked_tombstone_compaction': 'true'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0;
