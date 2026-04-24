-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_abmr_orcid ON author_behavior_metrics_ram(orcid);
CREATE UNIQUE INDEX IF NOT EXISTS rolap.idx_author_primary_orcid ON author_primary_subject(orcid);

CREATE TABLE rolap.author_behavior_metrics AS
SELECT
  ram.orcid,
  aps.subject,
  ram.total_outgoing_citations,
  COALESCE(ram.self_w / NULLIF(ram.total_outgoing_citations, 0), 0.0) AS self_citation_rate,
  COALESCE(ram.coauthor_nonself_w / NULLIF(ram.nonself_w, 0), 0.0)     AS coauthor_citation_rate
FROM rolap.author_behavior_metrics_ram ram
JOIN rolap.author_primary_subject AS aps ON aps.orcid = ram.orcid;
