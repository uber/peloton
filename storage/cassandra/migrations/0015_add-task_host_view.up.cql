/*
 Drop the old view ...
 */
DROP MATERIALIZED VIEW IF EXISTS mv_task_by_host;
/*
  ... and create a new version of the mv_task_by_host view with a TaskInfo field
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_task_by_host AS
    SELECT JobID, TaskID, TaskInfo, TaskState, TaskHost FROM tasks
    WHERE JobID is not NULL and TaskID is not NULL and TaskHost is not NULL
    PRIMARY KEY (TaskHost, JobID, TaskID);
