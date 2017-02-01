/*
  Tasks table persists the tasks for peloton
 */
ALTER TABLE tasks CHANGE `task_id` mesos_task_id VARCHAR(128) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.runtime.taskId.value')));

