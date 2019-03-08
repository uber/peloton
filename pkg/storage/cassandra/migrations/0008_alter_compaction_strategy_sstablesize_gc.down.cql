ALTER TABLE job_runtime WITH compaction={'class' :'SizeTieredCompactionStrategy'};
ALTER TABLE job_runtime WITH GC_GRACE_SECONDS=864000;

ALTER MATERIALIZED VIEW mv_job_by_state WITH compaction={'class' :'SizeTieredCompactionStrategy'};
ALTER MATERIALIZED VIEW mv_job_by_state WITH GC_GRACE_SECONDS=864000;

ALTER TABLE task_state_changes WITH compaction={'class' :'LeveledCompactionStrategy'};
ALTER TABLE task_state_changes WITH GC_GRACE_SECONDS=86400;

ALTER TABLE task_runtime WITH compaction={'class' :'SizeTieredCompactionStrategy'};
ALTER TABLE task_runtime WITH GC_GRACE_SECONDS=864000;

ALTER MATERIALIZED VIEW mv_task_by_state WITH compaction={'class' :'SizeTieredCompactionStrategy'};
ALTER MATERIALIZED VIEW mv_task_by_state WITH GC_GRACE_SECONDS=864000;

ALTER TABLE task_config WITH compaction={'class' :'LeveledCompactionStrategy'};
ALTER TABLE task_config WITH GC_GRACE_SECONDS=86400;

ALTER TABLE job_config WITH compaction={'class' :'LeveledCompactionStrategy'};
ALTER TABLE job_config WITH GC_GRACE_SECONDS=86400;
