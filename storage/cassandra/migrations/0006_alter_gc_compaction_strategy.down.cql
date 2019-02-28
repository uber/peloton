ALTER TABLE job_config  WITH compaction = { 'class' :  'SizeTieredCompactionStrategy'  };
ALTER TABLE task_config  WITH compaction = { 'class' :  'SizeTieredCompactionStrategy'  };
ALTER TABLE task_state_changes  WITH compaction = { 'class' :  'SizeTieredCompactionStrategy'  };
ALTER TABLE job_config  WITH GC_GRACE_SECONDS = 864000;
ALTER TABLE task_config  WITH GC_GRACE_SECONDS = 864000;
ALTER TABLE task_state_changes  WITH GC_GRACE_SECONDS = 864000;
