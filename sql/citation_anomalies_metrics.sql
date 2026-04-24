-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cae_pair ON citation_anomalies_enriched(citing_orcid, cited_orcid);

CREATE TABLE rolap.citation_anomalies_metrics AS
SELECT
  e.citing_orcid,
  e.cited_orcid,
  e.total_citations,
  CAST(e.reciprocal_citations AS REAL) / NULLIF(e.total_citations, 0) AS reciprocity_ratio,
  1.0 - (CAST(MIN(e.total_citations, e.reciprocal_citations) AS REAL)
         / NULLIF(MAX(e.total_citations, e.reciprocal_citations), 1))  AS asymmetry_score,
  CAST(e.total_citations AS REAL) / NULLIF(e.active_years, 0)          AS citation_velocity
FROM rolap.citation_anomalies_enriched e;
