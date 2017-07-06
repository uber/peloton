/*
  task_state_changes table persists the task state change for peloton
 */
CREATE TABLE IF NOT EXISTS task_state_changes (
  TaskID text,
  Events list<text>,
  PRIMARY KEY (TaskID)
);

