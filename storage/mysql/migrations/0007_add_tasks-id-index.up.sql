/*
  create index on tasks on job_id, instance_id
 */
CREATE INDEX idx_job_instance_id_to_task ON tasks (job_id, instance_id);
