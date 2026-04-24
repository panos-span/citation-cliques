CREATE INDEX IF NOT EXISTS idx_wa_work_id_orcid ON work_authors(work_id, orcid);
CREATE INDEX IF NOT EXISTS idx_works_id          ON works(id);
CREATE INDEX IF NOT EXISTS rolap.idx_ma_orcid    ON matched_authors(orcid);

CREATE TABLE rolap.coauthor_links AS
SELECT
  CASE WHEN wa1.orcid < wa2.orcid THEN wa1.orcid ELSE wa2.orcid END AS orcid1,
  CASE WHEN wa1.orcid < wa2.orcid THEN wa2.orcid ELSE wa1.orcid END AS orcid2,
  MIN(w.published_year) AS first_collaboration_year
FROM rolap.matched_authors ma
JOIN work_authors wa1 ON wa1.orcid = ma.orcid
JOIN work_authors wa2 ON wa2.work_id = wa1.work_id
JOIN works w ON w.id = wa1.work_id
WHERE wa1.orcid <> wa2.orcid
GROUP BY 1, 2;