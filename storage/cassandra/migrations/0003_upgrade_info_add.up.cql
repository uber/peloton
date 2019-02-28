/*
  update_info table persists the update info for peloton.
 */
CREATE TABLE IF NOT EXISTS update_info (
  update_id         uuid,
  update_config     text,
  state             int,
  instances_total   int,
  instances_done    int,
  instances_current set<int>,
  job_id            uuid,
  job_config        text,
  prev_version      int,
  creation_time     timestamp,
  update_time       timestamp,
  PRIMARY KEY (update_id)
);

/*
  Job to update_info materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_update_by_job AS
    SELECT update_id, job_id, creation_time  FROM update_info
    WHERE update_id is not NULL and job_id is not NULL and creation_time is not NULL
    PRIMARY KEY (update_id, job_id);
