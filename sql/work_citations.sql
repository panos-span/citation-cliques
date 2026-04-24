-- Count number of citations to each work
-- Optimized: Only count citations for works by filtered authors
CREATE INDEX IF NOT EXISTS work_references_doi_idx ON work_references(doi);
CREATE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);
CREATE INDEX IF NOT EXISTS idx_wa_orcid ON work_authors(orcid);
CREATE INDEX IF NOT EXISTS idx_wa_work_id ON work_authors(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_we_work_id ON works_enhanced(work_id);

CREATE TABLE rolap.work_citations AS
SELECT wr.doi, COUNT(*) AS citations_number
FROM work_references wr
WHERE wr.doi IN (
    SELECT DISTINCT we.doi
    FROM rolap.works_enhanced we
    JOIN work_authors wa ON wa.work_id = we.work_id
    JOIN rolap.filtered_authors fa ON fa.orcid = wa.orcid
)
GROUP BY wr.doi;
