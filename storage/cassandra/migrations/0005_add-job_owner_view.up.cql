/*
  owner to job materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_jobs_by_owner AS
    SELECT JobID, Owner, CreateTime, JobConfig FROM jobs
    WHERE Owner is not NULL and JobID is not NULL and CreateTime is not NULL and JobConfig is not NULL
    PRIMARY KEY (Owner, JobID);
