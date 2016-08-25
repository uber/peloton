/*
  Tasks table persists the tasks for peloton
 */
CREATE FULLTEXT INDEX idx_labels_jobs ON jobs (labels_summary);
