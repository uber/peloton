/*
  jobs table persists the jobs for peloton
 */
CREATE TABLE IF NOT EXISTS jobs (
  JobID         text,
  JobConfig     text,
  Owner         text,
  Labels        text,
  CreateTime    timestamp,
  UpdateTime    timestamp,
  PRIMARY KEY (JobId)
);

/*
  job_runtime table persists the job runtime for peloton
 */
CREATE TABLE IF NOT EXISTS job_runtime (
  JobID         text,
  JobRuntime    text,
  JobState      text,
  CreateTime    timestamp,
  UpdateTime    timestamp,
  PRIMARY KEY (JobId)
);
