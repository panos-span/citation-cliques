CREATE INDEX IF NOT EXISTS idx_wr_work         ON work_references(work_id);
CREATE INDEX IF NOT EXISTS idx_wr_doi          ON work_references(doi);
CREATE INDEX IF NOT EXISTS rolap.idx_rr_cited_work_id ON resolved_refs(cited_work_id);

-- Optimized: Use JOIN instead of IN for better performance
CREATE TABLE rolap.work_authors_unique AS
SELECT DISTINCT wa.work_id, wa.orcid 
FROM work_authors wa
JOIN rolap.resolved_refs rr ON wa.work_id = rr.cited_work_id;