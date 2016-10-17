/*
  Tasks table persists the tasks for peloton
 */
CREATE TABLE frameworks(
  `framework_name`  VARCHAR(64) NOT NULL PRIMARY KEY,
  `framework_id`    VARCHAR(128),
  `mesos_stream_id` VARCHAR(128),
  `update_time`     TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_host`     VARCHAR(64)
);
