-- Combined step: Pre-calculate author publications, scores, and citations
-- Replaces author_pubs_precalc and author_paper_subject_citations
-- Filters by rolap.filtered_authors

-- Required indexes on source tables
CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);
CREATE INDEX IF NOT EXISTS idx_wa_work_id_orcid ON work_authors(work_id, orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_we_work_id ON works_enhanced(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_wc_doi ON work_citations(doi);

CREATE TABLE rolap.author_works_master AS
SELECT
    wa.orcid,
    we.subject,
    we.eigenfactor_score,
    COALESCE(wc.citations_number, 0) as citations
FROM work_authors wa
JOIN rolap.works_enhanced we ON wa.work_id = we.work_id
JOIN rolap.filtered_authors fa ON wa.orcid = fa.orcid
LEFT JOIN rolap.work_citations wc ON we.doi = wc.doi
WHERE wa.orcid IS NOT NULL AND we.subject IS NOT NULL;

-- Create indexes for downstream steps
CREATE INDEX IF NOT EXISTS rolap.idx_awm_subject ON author_works_master(subject);
CREATE INDEX IF NOT EXISTS rolap.idx_awm_orcid_subject ON author_works_master(orcid, subject);
