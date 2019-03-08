CREATE MATERIALIZED VIEW IF NOT EXISTS mv_volume_by_job AS
    SELECT job_id, volume_id FROM persistent_volumes
    WHERE job_id is not NULL and volume_id is not NULL
    PRIMARY KEY (job_id, volume_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_volume_by_state AS
    SELECT state, volume_id FROM persistent_volumes
    WHERE state is not NULL and volume_id is not NULL
    PRIMARY KEY (state, volume_id);
