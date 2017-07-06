/*
  task host materialized view
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_task_by_host AS
    SELECT JobID, TaskID, TaskState, TaskHost FROM tasks
    WHERE JobID is not NULL and InstanceID is not NULL and TaskHost is not NULL
    PRIMARY KEY (TaskHost, JobID, InstanceID);
