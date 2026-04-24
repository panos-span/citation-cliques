CREATE TABLE rolap.control_authors_bucketed AS
SELECT
  orcid,
  subject,
  h5_index,
  CAST(h5_index / 3 AS INTEGER) AS h5_bucket
FROM rolap.authors_enriched
WHERE author_tier = 'Top Tier';
