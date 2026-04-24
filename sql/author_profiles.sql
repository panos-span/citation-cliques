-- Step 3 of Author Profiles: Final Classification
-- Creates rolap.author_profiles

-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_ass_orcid_subject ON author_subject_stats(orcid, subject);

CREATE TABLE rolap.author_profiles AS
SELECT
    orcid, subject, papers_in_subject, avg_eigenfactor,
    CASE
        WHEN CAST(bottom_tier_papers AS REAL) / papers_in_subject >= 0.7 AND papers_in_subject >= 3 THEN 'Bottom Tier'
        WHEN CAST(top_tier_papers AS REAL) / papers_in_subject >= 0.7 AND papers_in_subject >= 3 THEN 'Top Tier'
        WHEN papers_in_subject >= 3 THEN 'Mixed Tier'
        ELSE 'Insufficient Data'
    END as author_tier
FROM rolap.author_subject_stats;
