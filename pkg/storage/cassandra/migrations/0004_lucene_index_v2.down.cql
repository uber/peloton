/*
  Doing alter table inline causes failure when using on cassandra 3.4.0
*/

DROP index if exists job_index_lucene_v2;
ALTER TABLE job_index DROP name;
ALTER TABLE job_index DROP job_type;
ALTER TABLE job_index DROP instance_count;

