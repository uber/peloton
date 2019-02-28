ALTER TABLE update_info ADD update_type text;
ALTER TABLE update_info ADD instances_added set<int>;
ALTER TABLE update_info ADD instances_updated set<int>;
ALTER TABLE update_info ADD instances_removed set<int>;
