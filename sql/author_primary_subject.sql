-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_apsr_rn ON author_primary_subject_ranked(rn);

CREATE TABLE rolap.author_primary_subject AS
SELECT orcid, subject
FROM rolap.author_primary_subject_ranked
WHERE rn = 1;
