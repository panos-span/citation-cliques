-- Step 2 of H5 Index: Rank papers within subject

-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_awm_orcid_subject ON author_works_master(orcid, subject);

CREATE TABLE rolap.ranked_author_papers AS
SELECT
    orcid,
    subject,
    citations,
    ROW_NUMBER() OVER (PARTITION BY orcid, subject ORDER BY citations DESC) as paper_rank
FROM rolap.author_works_master;
