ALTER TABLE jobs ADD RespoolID text;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_jobs_by_respool AS
    SELECT JobID, RespoolID, JobConfig FROM jobs
    WHERE JobID is not NULL and RespoolID is not NULL and JobConfig is not NULL
    PRIMARY KEY (respoolID, JobID);
