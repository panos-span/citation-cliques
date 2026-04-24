-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cab_subject ON control_authors_bucketed(subject);

CREATE TABLE rolap.ctrl_counts AS
SELECT subject, COUNT(DISTINCT orcid) AS n_ctrl
FROM rolap.control_authors_bucketed
GROUP BY subject;
