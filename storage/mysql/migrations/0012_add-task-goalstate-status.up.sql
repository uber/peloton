/*
  Tasks table persists the tasks for peloton
 */
ALTER TABLE tasks ADD task_goal_state VARCHAR(64) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(body, '$.runtime.goalState')));
