-- Works (and their authors) for matched authors only; limited to works_enhanced scope.

CREATE INDEX IF NOT EXISTS rolap.idx_ma_orcid ON matched_authors(orcid);

CREATE TABLE rolap.relevant_works AS
SELECT DISTINCT wa.work_id, wa.orcid, we.doi, we.published_year
FROM work_authors wa
JOIN rolap.matched_authors ma ON ma.orcid = wa.orcid
JOIN rolap.works_enhanced we ON we.work_id = wa.work_id;