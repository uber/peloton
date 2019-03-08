/*
 * Compaction Strategy changed from SizeTiered to Leveled. Leveled compaction is better for read/write
 * latency for balanced read/write, since it guarantees record to be present in 1 sstable.
 *
 * Reducing sstable size from default 160MB to 64MB.
 * Recommended to watch Cassandra Summit 2016 talk "Myth of Large Partitions": https://www.youtube.com/watch?v=N3mGxgnUiRY
 * Since sstables are immutable, on compaction new sstable is written as well as corresponding summary index
 * files are generated, which creates too many small objects on heap and leads to GC (STW event),
 * causing cascading event on latency.
 * Reason to select 64MB and not other size such as 128MB or 32MB, 128MB can still lead to large partition issue,
 * as well as large partition warning configuration set at Cassandra cluster is 128MB.
 * 32MB sstable size, might be too small which can lead to aggressive compaction and level change.
 * Overall 64MB file size seems a sweet spot.
 *
 * Reverting GC Grace Seconds from 1 day to 10 days (default value), to make sure repair completes
 * in this period and giving us higher consistency guarantee for long running tasks.
 */

ALTER TABLE job_runtime WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER TABLE job_runtime WITH GC_GRACE_SECONDS=864000;

ALTER MATERIALIZED VIEW mv_job_by_state WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER MATERIALIZED VIEW mv_job_by_state WITH GC_GRACE_SECONDS=864000;

ALTER TABLE task_state_changes WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER TABLE task_state_changes WITH GC_GRACE_SECONDS=864000;

ALTER TABLE task_runtime WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64' };
ALTER TABLE task_runtime WITH GC_GRACE_SECONDS=864000;

ALTER MATERIALIZED VIEW mv_task_by_state WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER MATERIALIZED VIEW mv_task_by_state WITH GC_GRACE_SECONDS=864000;

ALTER TABLE task_config WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER TABLE task_config WITH GC_GRACE_SECONDS=864000;

ALTER TABLE job_config WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER TABLE job_config WITH GC_GRACE_SECONDS=864000;
