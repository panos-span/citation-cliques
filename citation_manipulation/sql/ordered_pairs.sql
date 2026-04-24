CREATE TABLE rolap.ordered_pairs AS
SELECT
  case_orcid, control_orcid, subject,
  ROW_NUMBER() OVER (
    PARTITION BY subject
    ORDER BY score ASC, tie_case ASC, tie_ctrl ASC, case_orcid, control_orcid
  ) AS rn
FROM rolap.author_matched_candidates;
