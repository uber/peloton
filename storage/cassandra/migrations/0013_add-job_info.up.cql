/*
  job_index table merges the jobs info and runtime info. It would be used for supporting all
  queries.
 */
CREATE TABLE IF NOT EXISTS job_index (
  JobID         text,
  JobConfig     text,
  JobRuntime    text,
  JobState      text,
  RespoolID     text,
  Owner         text,
  Labels        text,
  CreateTime    timestamp,
  UpdateTime    timestamp,
  CompleteTime  timestamp,
  StartTime     timestamp,
  PRIMARY KEY (JobId)
);

/*
  create lucene index in cassandra store for job_index table
 */
CREATE CUSTOM INDEX IF NOT EXISTS job_index_lucene ON job_index ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '10',
   'schema': '{
      fields: {
         owner: {type: "string"},
         labels: {type: "text", analyzer: "english"},
         jobconfig:{type: "text", analyzer: "english"},
         createtime: {type: "date", pattern: "yyyy/MM/dd"},
         respoolid: {type: "string"},
         jobstate: {type: "string"},
         updatetime: {type: "date", pattern: "yyyy/MM/dd"},
         starttime: {type:"date", pattern: "yyyy/MM/dd"},
         completetime: {type: "date", pattern: "yyyy/MM/dd"}
      }
   }'
};