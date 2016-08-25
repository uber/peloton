/*
  Tasks table persists the tasks for peloton
 */
CREATE INDEX idx_job_to_task ON tasks (job_id, task_state);
