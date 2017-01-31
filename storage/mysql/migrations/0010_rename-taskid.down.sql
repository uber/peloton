/*
  Rename task_id field in tasks table to mesos_task_id
 */
ALTER TABLE tasks CHANGE `mesos_task_id` task_id  VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.runtime.taskId.value')));

