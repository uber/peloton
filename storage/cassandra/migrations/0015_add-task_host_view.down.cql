/*
 Drop the new view ...
 */
DROP MATERIALIZED VIEW IF EXISTS mv_task_by_host;
/*
  ... and recreate the old task host materialized view.
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_task_by_host AS
    SELECT JobID, TaskID, TaskState, TaskHost FROM tasks
    WHERE JobID is not NULL and TaskID is not NULL and TaskHost is not NULL
    PRIMARY KEY (TaskHost, JobID, TaskID);
