/*
  tasks table persists the tasks for peloton
 */
CREATE TABLE IF NOT EXISTS tasks (
  TaskID         text,
  JobID          text,
  InstanceID     int,
  TaskInfo       text,
  TaskHost       text,
  TaskState      text,
  CreateTime     timestamp,
  UpdateTime     timestamp,
  PRIMARY KEY (JobID, InstanceID)
);

