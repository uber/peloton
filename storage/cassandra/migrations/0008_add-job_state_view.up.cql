/*
  JobState to job materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_jobs_by_state AS
    SELECT JobID, JobState, CreateTime, UpdateTime FROM job_runtime
    WHERE JobID is not NULL and JobState is not NULL and CreateTime is not NULL and UpdateTime is not NULL
    PRIMARY KEY (JobState, JobID);
