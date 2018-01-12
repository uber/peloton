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
