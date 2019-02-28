ALTER TABLE update_info DROP job_config_version;
ALTER TABLE update_info DROP job_config_prev_version;
ALTER TABLE update_info DROP update_state;
ALTER TABLE update_info DROP update_options;
ALTER TABLE update_info ADD prev_version int;
ALTER TABLE update_info ADD job_config text;
ALTER TABLE update_info ADD state int;
ALTER TABLE update_info ADD update_config text;

DROP MATERIALIZED VIEW IF EXISTS mv_updates_by_job;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_update_by_job AS
    SELECT update_id, job_id, creation_time  FROM update_info
    WHERE update_id is not NULL and job_id is not NULL and creation_time is not NULL
    PRIMARY KEY (update_id, job_id);
