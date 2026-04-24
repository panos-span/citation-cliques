-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_ashi_orcid_subject ON author_subject_h5_index(orcid, subject);

CREATE TABLE rolap.author_primary_subject_ranked AS
SELECT
  ashi.orcid,
  ashi.subject,
  ashi.h5_index,
  ROW_NUMBER() OVER (
    PARTITION BY ashi.orcid
    ORDER BY ashi.h5_index DESC, ashi.subject
  ) AS rn
FROM rolap.author_subject_h5_index AS ashi;
