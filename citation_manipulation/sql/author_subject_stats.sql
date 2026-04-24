-- Step 2 of Author Profiles: Calculate subject statistics
-- Depends on rolap.author_works_master and rolap.eigenfactor_percentiles

-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_awm_subject ON author_works_master(subject);
CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_ep_subject ON eigenfactor_percentiles(subject);

CREATE TABLE rolap.author_subject_stats AS
SELECT
    orcid, ep.subject,
    COUNT(*) as papers_in_subject,
    AVG(eigenfactor_score) as avg_eigenfactor,
    SUM(CASE WHEN eigenfactor_score <= ep.p25 THEN 1 ELSE 0 END) as bottom_tier_papers,
    SUM(CASE WHEN eigenfactor_score >= ep.p75 THEN 1 ELSE 0 END) as top_tier_papers
FROM rolap.author_works_master ap
JOIN rolap.eigenfactor_percentiles ep ON ap.subject = ep.subject
GROUP BY orcid, ep.subject;
