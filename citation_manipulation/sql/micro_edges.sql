-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_rr_citing ON resolved_refs(citing_work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_rr_cited  ON resolved_refs(cited_work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_rw_work_id ON relevant_works(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_wau_work_id ON work_authors_unique(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_cac_work_id ON citing_authors_counts(work_id);
CREATE INDEX IF NOT EXISTS rolap.idx_zac_cited_work_id ON cited_authors_counts(cited_work_id);

CREATE TABLE rolap.micro_edges AS
SELECT
  rw.orcid                           AS citing_orcid,
  wau2.orcid                         AS cited_orcid,
  rw.published_year                  AS citation_year,
  1.0 / (cac.n_citing_authors * zac.n_cited_authors) AS w
FROM rolap.resolved_refs rr
JOIN rolap.relevant_works      rw   ON rw.work_id = rr.citing_work_id
JOIN rolap.work_authors_unique wau2 ON wau2.work_id = rr.cited_work_id
JOIN rolap.citing_authors_counts cac ON cac.work_id = rr.citing_work_id
JOIN rolap.cited_authors_counts  zac ON zac.cited_work_id = rr.cited_work_id
WHERE rw.orcid <> wau2.orcid;  -- drop self at expansion time; faster later