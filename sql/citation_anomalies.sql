-- Required indexes on source tables
CREATE INDEX IF NOT EXISTS rolap.idx_cam_pair ON citation_anomalies_metrics(citing_orcid, cited_orcid);
CREATE INDEX IF NOT EXISTS rolap.idx_cpb_pair ON cnf_pair_bursts(citing_orcid, cited_orcid);

CREATE TABLE rolap.citation_anomalies AS
SELECT
  fm.citing_orcid AS orcid,
  AVG(fm.asymmetry_score)   AS avg_asymmetry,
  MAX(fm.asymmetry_score)   AS max_asymmetry,
  AVG(fm.citation_velocity) AS avg_velocity,
  MAX(cb.max_burst)         AS max_burst
FROM rolap.citation_anomalies_metrics fm
LEFT JOIN rolap.cnf_pair_bursts cb
  ON cb.citing_orcid = fm.citing_orcid AND cb.cited_orcid = fm.cited_orcid
GROUP BY fm.citing_orcid;
