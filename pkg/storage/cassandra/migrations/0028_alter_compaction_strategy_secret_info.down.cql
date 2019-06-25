ALTER TABLE secret_info WITH compaction={'class' :'SizeTieredCompactionStrategy'};
ALTER TABLE secret_info WITH GC_GRACE_SECONDS=864000;