/*
  JobState to job materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_jobs_by_state AS
    SELECT JobID, JobState, Owner, CreateTime FROM jobs
    WHERE Owner is not NULL and JobID is not NULL and JobState is not NULL and CreateTime is not NULL
    PRIMARY KEY (JobState, JobID);
