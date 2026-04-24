-- Step 3 of H5 Index: Calculate H5 Index
-- Creates rolap.author_subject_h5_index

-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_rap_orcid_subject ON ranked_author_papers(orcid, subject);

CREATE TABLE rolap.author_subject_h5_index AS
SELECT
    orcid,
    subject,
    COALESCE(MAX(paper_rank), 0) as h5_index
FROM rolap.ranked_author_papers
WHERE paper_rank <= citations
GROUP BY orcid, subject;
