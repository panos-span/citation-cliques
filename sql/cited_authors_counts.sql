CREATE TABLE rolap.cited_authors_counts AS
SELECT work_id AS cited_work_id, COUNT(*) AS n_cited_authors
FROM rolap.work_authors_unique
GROUP BY work_id;