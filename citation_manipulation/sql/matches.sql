SELECT
  COUNT(DISTINCT case_orcid)    AS uniq_cases,
  COUNT(DISTINCT control_orcid) AS uniq_controls,
  COUNT(*)                      AS total_pairs
FROM author_matched_pairs;