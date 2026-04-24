CREATE TABLE rolap.author_matched_candidates AS
WITH
authors_enriched AS (
  SELECT
    ap.orcid,
    ap.subject,
    ap.author_tier,
    COALESCE(ashi.h5_index, 0) AS h5_index
  FROM rolap.author_profiles ap
  JOIN rolap.author_subject_h5_index ashi
    ON ap.orcid = ashi.orcid AND ap.subject = ashi.subject
  WHERE ap.author_tier IN ('Bottom Tier','Top Tier')
),
bottom_authors_sampled AS (
  SELECT ae.orcid, ae.subject, ae.h5_index,
         CAST(ae.h5_index / 3 AS INTEGER) AS h5_bucket
  FROM (
    SELECT
      orcid, subject, h5_index,
      ROW_NUMBER() OVER (
        PARTITION BY subject
        ORDER BY substr(
          CAST(REPLACE(orcid, '-', '') AS TEXT) * 0.54534238371923827955579364758491,
          length(CAST(REPLACE(orcid, '-', '') AS TEXT)) + 2
        )
      ) AS sample_rank
    FROM authors_enriched
    WHERE author_tier = 'Bottom Tier' AND h5_index > 0
  ) ae
  JOIN rolap.ctrl_counts cc ON cc.subject = ae.subject
  WHERE sample_rank <= cc.n_ctrl
),
control_authors_bucketed AS (
  SELECT
    orcid, subject, h5_index,
    CAST(h5_index / 3 AS INTEGER) AS h5_bucket
  FROM authors_enriched
  WHERE author_tier = 'Top Tier' AND h5_index > 0
)
SELECT
  b.orcid  AS case_orcid,
  c.orcid  AS control_orcid,
  b.subject AS subject,
  ABS(b.h5_index - c.h5_index) AS score,
  -- deterministic tie-breakers (no RANDOM())
  substr(
    CAST(REPLACE(c.orcid, '-', '') AS TEXT) * 0.54534238371923827955579364758491,
    length(CAST(REPLACE(c.orcid, '-', '') AS TEXT)) + 2
  ) AS tie_ctrl,
  substr(
    CAST(REPLACE(b.orcid, '-', '') AS TEXT) * 0.54534238371923827955579364758491,
    length(CAST(REPLACE(b.orcid, '-', '') AS TEXT)) + 2
  ) AS tie_case
FROM bottom_authors_sampled b
JOIN control_authors_bucketed c
  ON b.subject = c.subject
 AND c.h5_bucket BETWEEN (b.h5_bucket - 1) AND (b.h5_bucket + 1);
