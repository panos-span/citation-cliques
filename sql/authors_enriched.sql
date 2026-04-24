CREATE TABLE rolap.authors_enriched AS
SELECT
  ap.orcid,
  ap.subject,
  ap.author_tier,
  COALESCE(ashi.h5_index, 0) AS h5_index
FROM rolap.author_profiles ap
JOIN rolap.author_subject_h5_index ashi
  ON ap.orcid = ashi.orcid AND ap.subject = ashi.subject
WHERE ap.author_tier IN ('Bottom Tier', 'Top Tier')
  AND COALESCE(ashi.h5_index,0) > 0;
