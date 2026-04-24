-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cpdt_pair ON cnf_pair_dir_totals(o1, o2);
CREATE INDEX IF NOT EXISTS rolap.idx_cprt_pair ON cnf_pair_recip_totals(o1, o2);

CREATE TABLE rolap.citation_anomalies_enriched AS
SELECT
  d.citing_orcid,
  d.cited_orcid,
  d.total_citations,
  d.active_years,
  d.o1, d.o2,
  CASE WHEN d.citing_orcid = d.o1 THEN r.w_o2_to_o1 ELSE r.w_o1_to_o2 END AS reciprocal_citations
FROM rolap.cnf_pair_dir_totals       AS d
JOIN rolap.cnf_pair_recip_totals     AS r
  ON r.o1 = d.o1 AND r.o2 = d.o2;
