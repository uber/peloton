/*
  Tasks table persists the tasks for peloton
 */
CREATE TABLE tasks (
  `added_id`        int(11) NOT NULL AUTO_INCREMENT,
  `row_key`         VARCHAR(64) NOT NULL PRIMARY KEY,
  `col_key`         VARCHAR(64) NOT NULL,
  `ref_key`         INTEGER NOT NULL,
  `body`            JSON NOT NULL,
  `created_by`      VARCHAR(64) NOT NULL ,
  `create_time`     DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `job_id`          VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.jobId.value'))),
  `task_state`      VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.runtime.state'))),
  `task_host`       VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.runtime.host'))),
  `flags`           INTEGER DEFAULT 0,
   KEY `added_id` (`added_id`)
);

