/*
  task state materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_task_by_job_state AS
    SELECT JobID, TaskID, TaskState, TaskHost, TaskInfo FROM tasks
    WHERE JobID is not NULL and TaskID is not NULL and TaskState is not NULL and TaskInfo is not NULL
    PRIMARY KEY (JobID, TaskState, TaskID);
