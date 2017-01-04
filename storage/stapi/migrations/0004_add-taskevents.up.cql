/*
  taskevents table persists the task events for peloton
 */
CREATE TABLE IF NOT EXISTS task_events (
  TaskID text,
  Events list<text>,
  PRIMARY KEY (TaskID)
);

