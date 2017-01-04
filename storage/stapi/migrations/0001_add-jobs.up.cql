/*
  jobs table persists the jobs for peloton
 */
CREATE TABLE IF NOT EXISTS jobs (
  JobID         text,
  JobConfig     text,
  Owner         text,
  Labels        text,
  JobState      text,
  CreateTime    timestamp,
  CompleteTime  timestamp,
  PRIMARY KEY (JobId)
);
