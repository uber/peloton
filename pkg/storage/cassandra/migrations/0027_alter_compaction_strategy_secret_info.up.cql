ALTER TABLE secret_info WITH compaction={'class' :'LeveledCompactionStrategy','sstable_size_in_mb':'64'};
ALTER TABLE secret_info WITH GC_GRACE_SECONDS=864000;