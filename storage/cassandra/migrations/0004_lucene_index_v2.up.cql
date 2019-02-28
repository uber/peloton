/*
  Doing alter table inline causes failure when using on cassandra 3.4.0
*/

ALTER TABLE job_index ADD name text;
ALTER TABLE job_index ADD job_type int;
ALTER TABLE job_index ADD instance_count int;

/*
  create v2 lucene index in cassandra store for job_index table
 */
CREATE CUSTOM INDEX IF NOT EXISTS job_index_lucene_v2 ON job_index ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '10',
   'schema': '{
      fields: {
         owner: {type: "string"},
         name: {type: "string"},
         job_type: {type: "integer"},
         instance_count: {type: "integer"},
         runtime_info: {type: "text", analyzer: "english"},
         labels: {type: "text", analyzer: "english"},
         config:{type: "text", analyzer: "english"},
         creation_time: {type: "date", pattern: "yyyyMMddHHmmss"},
         respool_id: {type: "string"},
         state: {type: "string"},
         update_time: {type: "date", pattern: "yyyyMMddHHmmss"},
         start_time: {type:"date", pattern: "yyyyMMddHHmmss"},
         completion_time: {type: "date", pattern: "yyyyMMddHHmmss"}
      }
   }'
};
