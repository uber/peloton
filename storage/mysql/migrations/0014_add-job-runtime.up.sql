/*
  Add job runtime info as a field in jobs table
 */
CREATE TABLE job_runtime (
  `row_key`         VARCHAR(64) NOT NULL PRIMARY KEY,
  `runtime`         JSON NOT NULL,
  `job_state`       VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(runtime, '$.state'))),
  `create_time`     DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_state ON job_runtime (job_state);
