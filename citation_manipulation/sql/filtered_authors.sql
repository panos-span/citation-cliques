-- Filter authors to a manageable subset to speed up the pipeline.
-- Currently set to ~10% of authors based on the last digit of their ORCID.
-- Adjust the condition to increase/decrease the sample size.

CREATE TABLE rolap.filtered_authors AS
SELECT DISTINCT orcid
FROM work_authors
WHERE substr(orcid, -1) = '0'; -- Selects approx 10% of authors

CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_fa_orcid ON filtered_authors(orcid);
