/*
  Tasks table persists the tasks for peloton
 */
CREATE INDEX idx_owning_team_to_job ON jobs (owning_team);
