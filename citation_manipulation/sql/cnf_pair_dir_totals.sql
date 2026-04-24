-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cpy_pair_unordered ON cnf_pair_year(o1, o2);
CREATE INDEX IF NOT EXISTS rolap.idx_cpy_pair_dir ON cnf_pair_year(citing_orcid, cited_orcid);

CREATE TABLE rolap.cnf_pair_dir_totals AS
SELECT
  citing_orcid,
  cited_orcid,
  o1, o2,
  SUM(w_year)                           AS total_citations,
  MIN(citation_year)                    AS first_citation_year,
  MAX(citation_year)                    AS last_citation_year,
  COUNT(DISTINCT citation_year)         AS active_years
FROM rolap.cnf_pair_year
GROUP BY citing_orcid, cited_orcid, o1, o2;