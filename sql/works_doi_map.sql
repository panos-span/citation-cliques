-- Create a mapping table for normalized DOIs to speed up joins
-- This avoids expensive string manipulation during joins in abm_micro_edges_fast.sql

-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_we_doi ON works_enhanced(doi);

CREATE TABLE rolap.works_doi_map AS
SELECT
  work_id,
  LOWER(REPLACE(REPLACE(doi,'https://doi.org/',''),'http://doi.org/','')) AS doi_norm
FROM rolap.works_enhanced
WHERE doi IS NOT NULL;
