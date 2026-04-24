-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_me_pair_year ON micro_edges(citing_orcid, cited_orcid, citation_year);

CREATE TABLE rolap.citation_network_agg AS
SELECT
  citing_orcid,
  cited_orcid,
  citation_year,
  COUNT(*) AS citation_count_raw,
  SUM(w)  AS citation_weight
FROM rolap.micro_edges
GROUP BY citing_orcid, cited_orcid, citation_year;
