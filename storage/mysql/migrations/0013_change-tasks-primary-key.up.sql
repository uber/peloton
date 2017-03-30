ALTER TABLE jobs DROP PRIMARY KEY, ADD PRIMARY KEY(added_id);
ALTER TABLE jobs ADD CONSTRAINT uc_key UNIQUE (row_key, col_key, ref_key);
ALTER TABLE tasks DROP PRIMARY KEY, ADD PRIMARY KEY(added_id);
ALTER TABLE tasks ADD CONSTRAINT uc_key UNIQUE (row_key, col_key, ref_key);
