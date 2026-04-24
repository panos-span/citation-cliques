-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_rw_work_id ON relevant_works(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_wdm_doi_norm ON works_doi_map(doi_norm);

-- Optimized: Filter references by relevant works BEFORE string manipulation
CREATE TABLE rolap.resolved_refs AS
WITH relevant_refs AS (
    SELECT wr.work_id, wr.doi
    FROM work_references wr
    JOIN rolap.relevant_works rw ON rw.work_id = wr.work_id
    WHERE wr.doi IS NOT NULL
)
SELECT rr.work_id AS citing_work_id, wdm.work_id AS cited_work_id
FROM relevant_refs rr
JOIN rolap.works_doi_map wdm ON wdm.doi_norm = LOWER(REPLACE(REPLACE(rr.doi,'https://doi.org/',''),'http://doi.org/',''));