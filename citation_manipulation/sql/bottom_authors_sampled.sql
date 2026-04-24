CREATE TABLE rolap.bottom_authors_sampled AS
SELECT orcid, subject, h5_index,
       CAST(h5_index / 3 AS INTEGER) AS h5_bucket
FROM (
  SELECT
    ae.orcid,
    ae.subject,
    ae.h5_index,
    ROW_NUMBER() OVER (
      PARTITION BY ae.subject
      ORDER BY substr(
        CAST(REPLACE(ae.orcid, '-', '') AS TEXT) * 0.54534238371923827955579364758491,
        length(CAST(REPLACE(ae.orcid, '-', '') AS TEXT)) + 2
      )
    ) AS sample_rank,
    cc.n_ctrl
  FROM rolap.authors_enriched ae
  JOIN rolap.ctrl_counts cc ON cc.subject = ae.subject
  WHERE ae.author_tier = 'Bottom Tier'
)
WHERE sample_rank <= n_ctrl;
