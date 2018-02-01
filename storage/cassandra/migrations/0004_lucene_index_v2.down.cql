DROP index if exists job_index_lucene_v2;
ALTER TABLE job_index DROP (name, job_type, instance_count);
