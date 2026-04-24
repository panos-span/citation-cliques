-- Step 1 of Author Profiles: Pre-calculate author publications and scores
-- Filters by rolap.filtered_authors

-- Required indexes on source tables
CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);
CREATE INDEX IF NOT EXISTS idx_wa_work_id_orcid ON work_authors(work_id, orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_we_work_id ON works_enhanced(work_id);

CREATE TABLE rolap.author_pubs_precalc AS
SELECT wa.orcid, we.subject, we.eigenfactor_score
FROM work_authors wa
JOIN rolap.works_enhanced we ON wa.work_id = we.work_id
JOIN rolap.filtered_authors fa ON wa.orcid = fa.orcid
WHERE wa.orcid IS NOT NULL;
