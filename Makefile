# Makefile for the citation-cliques replication package.
#
# This Makefile drives the self-contained SQLite pipeline that produces the
# author-level citation features consumed by the analysis scripts in python/.
#
# Prerequisites:
#   - sqlite3 (>= 3.38)
#   - uv (https://docs.astral.sh/uv/) with Python >= 3.11
#   - rolap.db and impact.db downloaded from the Zenodo replication archive
#     and placed in the repository root (see README.md).
#
# Common targets:
#   make env         Sync the Python environment with uv.
#   make pipeline    Run every SQL step in order against rolap.db.
#   make analysis    Run the main statistical and figure-generation script.
#   make outliers    Run the hybrid Isolation Forest outlier analysis.
#   make tests       Run the RDBUnit SQL fixtures in tests/.
#   make clean       Remove cached derived tables (keeps raw DBs).
#   make distclean   Also remove databases and generated outputs.

DB          ?= rolap.db
IMPACT_DB   ?= impact.db
SQL_DIR     := sql
PY_DIR      := python
TESTS_DIR   := tests

SQLITE      ?= sqlite3
UV          ?= uv

# Ordered list of SQL steps. Order matters: each step may depend on tables
# produced by earlier steps. The sequence mirrors the documented DAG.
SQL_STEPS := \
  00_prepare_base_tables.sql \
  works_doi_map.sql \
  works_enhanced.sql \
  work_citations.sql \
  eigenfactor_percentiles.sql \
  author_works_master.sql \
  author_subject_stats.sql \
  author_profiles.sql \
  ranked_author_papers.sql \
  author_paper_subject_citations.sql \
  author_subject_h5_index.sql \
  author_pubs_precalc.sql \
  authors_enriched.sql \
  filtered_authors.sql \
  control_authors_bucketed.sql \
  ctrl_counts.sql \
  bottom_authors_sampled.sql \
  author_matched_candidates.sql \
  ordered_pairs.sql \
  author_matched_pairs.sql \
  matched_authors.sql \
  author_primary_subject_ranked.sql \
  author_primary_subject.sql \
  work_authors_unique.sql \
  coauthor_links.sql \
  resolved_refs.sql \
  relevant_works.sql \
  citing_authors_counts.sql \
  cited_authors_counts.sql \
  micro_edges.sql \
  citation_network_agg.sql \
  cnf_pair_year.sql \
  cnf_pair_dir_totals.sql \
  cnf_pair_recip_totals.sql \
  cnf_pair_bursts.sql \
  citation_network_final.sql \
  abm_author_filter.sql \
  abm_wr_dedup.sql \
  abm_relevant_citing_works.sql \
  abm_citing_authors_count.sql \
  abm_cited_authors_count.sql \
  abm_micro_edges_fast.sql \
  abm_pair_year_fast.sql \
  abm_pair_year_co_fast.sql \
  author_behavior_metrics_ram.sql \
  author_behavior_metrics.sql \
  author_venue_metrics.sql \
  citation_anomalies_enriched.sql \
  citation_anomalies_metrics.sql \
  citation_anomalies.sql \
  matched_pair_comparison.sql \
  matches.sql \
  hypothesis_test_summary.sql \
  report_hypothesis.sql \
  author_features_final.sql

.PHONY: env pipeline analysis outliers tests clean distclean help

help:
	@sed -n '1,/^$$/p' $(MAKEFILE_LIST) | sed 's/^# \{0,1\}//'

env:
	$(UV) sync

$(DB):
	@echo "error: $(DB) not found. Download the databases from Zenodo (see README.md)." >&2
	@exit 1

pipeline: $(DB)
	@set -e; \
	for step in $(SQL_STEPS); do \
	  echo "== $$step =="; \
	  $(SQLITE) "$(DB)" < "$(SQL_DIR)/$$step"; \
	done

analysis:
	$(UV) run $(PY_DIR)/citation_analysis.py

outliers:
	$(UV) run $(PY_DIR)/hybrid_outlier_analysis.py

investigate:
	$(UV) run $(PY_DIR)/investigate_authors.py

figures-dag:
	$(UV) run $(PY_DIR)/diagrams.py

tests:
	@command -v rdbunit >/dev/null 2>&1 || { \
	  echo "error: rdbunit not found. Install from https://github.com/dspinellis/rdbunit." >&2; exit 1; }
	@set -e; for t in $(TESTS_DIR)/*.rdbu; do \
	  echo "== $$t =="; rdbunit "$$t" | $(SQLITE); \
	done

clean:
	rm -f main.aux main.log main.out main.bcf main.run.xml

distclean: clean
	rm -f $(DB) $(IMPACT_DB)
	rm -rf analysis_results/ publication_figures/
