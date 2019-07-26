CREATE MATERIALIZED VIEW IF NOT EXISTS mv_job_by_state AS
    SELECT job_id, state, update_time FROM job_runtime
    WHERE job_id is not NULL and state is not NULL and update_time is not NULL
    PRIMARY KEY (state, job_id);