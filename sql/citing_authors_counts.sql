CREATE INDEX IF NOT EXISTS rolap.idx_wau_work  ON work_authors_unique(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_wau_orcid ON work_authors_unique(orcid);

-- Optimized: Count ALL authors on the citing papers, not just the matched ones.
-- This ensures the citation weight (1/N_citing) is correct.
CREATE TABLE rolap.citing_authors_counts AS
SELECT wa.work_id, COUNT(*) AS n_citing_authors
FROM work_authors wa
WHERE wa.work_id IN (SELECT work_id FROM rolap.relevant_works)
GROUP BY wa.work_id;