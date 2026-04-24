-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cnf_citing ON citation_network_final(citing_orcid);

CREATE TABLE rolap.author_behavior_metrics_ram AS
SELECT
  cnf.citing_orcid         AS orcid,
  SUM(cnf.citation_weight) AS total_outgoing_citations,
  SUM(CASE WHEN cnf.is_self_citation = 1 THEN cnf.citation_weight ELSE 0 END) AS self_w,
  SUM(CASE WHEN cnf.is_self_citation = 0 THEN cnf.citation_weight ELSE 0 END) AS nonself_w,
  SUM(CASE WHEN cnf.is_self_citation = 0 AND cnf.is_coauthor_citation = 1
           THEN cnf.citation_weight ELSE 0 END) AS coauthor_nonself_w
FROM rolap.citation_network_final AS cnf
WHERE cnf.citation_year BETWEEN 2020 AND 2024
GROUP BY cnf.citing_orcid;
