ALTER TABLE update_info ADD job_config_version bigint;
ALTER TABLE update_info ADD job_config_prev_version bigint;
ALTER TABLE update_info ADD update_state text;
ALTER TABLE update_info ADD update_options blob;
ALTER TABLE update_info DROP prev_version;
ALTER TABLE update_info DROP job_config;
ALTER TABLE update_info DROP state;
ALTER TABLE update_info DROP update_config;

DROP MATERIALIZED VIEW IF EXISTS mv_update_by_job;

/*
  Job to update_info materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_updates_by_job AS
    SELECT update_id, job_id, creation_time FROM update_info
    WHERE update_id is not NULL and job_id is not NULL and creation_time is not NULL
    PRIMARY KEY (job_id, update_id);
