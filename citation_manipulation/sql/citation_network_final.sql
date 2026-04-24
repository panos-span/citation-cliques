-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cna_pair ON citation_network_agg(citing_orcid, cited_orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_col_orcid_pair ON coauthor_links(orcid1, orcid2);

CREATE TABLE rolap.citation_network_final AS
SELECT
  a.citing_orcid,
  a.cited_orcid,
  a.citation_year,
  a.citation_count_raw,
  a.citation_weight,
  0 AS is_self_citation,
  CASE WHEN cl.first_collaboration_year IS NOT NULL
        AND cl.first_collaboration_year <= a.citation_year
       THEN 1 ELSE 0 END AS is_coauthor_citation
FROM rolap.citation_network_agg a
LEFT JOIN rolap.coauthor_links cl
  ON (CASE WHEN a.citing_orcid < a.cited_orcid THEN a.citing_orcid ELSE a.cited_orcid END) = cl.orcid1
 AND (CASE WHEN a.citing_orcid < a.cited_orcid THEN a.cited_orcid ELSE a.citing_orcid END) = cl.orcid2;
