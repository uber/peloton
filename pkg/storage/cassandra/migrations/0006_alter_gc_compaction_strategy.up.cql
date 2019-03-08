ALTER TABLE job_config  WITH compaction = { 'class' :  'LeveledCompactionStrategy'  };
ALTER TABLE task_config  WITH compaction = { 'class' :  'LeveledCompactionStrategy'  };
ALTER TABLE task_state_changes  WITH compaction = { 'class' :  'LeveledCompactionStrategy'  };
ALTER TABLE job_config  WITH GC_GRACE_SECONDS = 86400;
ALTER TABLE task_config  WITH GC_GRACE_SECONDS = 86400;
ALTER TABLE task_state_changes  WITH GC_GRACE_SECONDS = 86400;
