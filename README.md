# citation-cliques

Replication package for *Citation Cliques in Low Impact Journals* (Spanakis,
Alexandrou & Spinellis, 2026). The repository contains the SQL pipeline, Python
analysis scripts, and RDBUnit test fixtures needed to reproduce every figure
and table in the paper from the accompanying SQLite databases.

The manuscript source is maintained separately at
[`gregalexan/citation-cliques-latex`](https://github.com/gregalexan/citation-cliques-latex).

## Repository layout

```
.
├── sql/        SQLite pipeline scripts (run in the order defined in the Makefile)
├── python/     Analysis, matching, figure, and diagnostic scripts
├── tests/      RDBUnit fixtures for the critical intermediate tables
├── data/       Small input CSVs (ISSN → subject predictions, eigenfactor scores)
├── Makefile    Canonical driver: `make pipeline`, `make analysis`, `make tests`
├── pyproject.toml / uv.lock   Pinned Python environment (managed with uv)
└── CITATION.cff, LICENSE      Citation metadata and MIT licence
```

## Data

The raw databases (`rolap.db`, `impact.db`) are **not** in this repository
because of their size (~15 GB and ~119 GB respectively). Download them from
the companion Zenodo archive and place them at the repository root:

> TODO: insert Zenodo DOI here once the first GitHub release is archived.

The databases are derived from Crossref (2020–2024) via
[`alexandria3k`](https://github.com/dspinellis/alexandria3k); the ingestion
step is documented in the paper's Methodology section and the data-derivation
appendix.

## Quick start

```bash
# 1. Install dependencies (requires uv: https://docs.astral.sh/uv/)
make env

# 2. Place rolap.db and impact.db at the repository root (from Zenodo).

# 3. Run the SQL pipeline (populates ~50 derived tables in rolap.db)
make pipeline

# 4. Generate figures, tables, and statistical tests
make analysis
make outliers
```

## Pipeline overview

The SQL steps build, in order: subject-aware eigenfactor percentiles →
author portfolios and tier assignments → matched Case/Control pairs →
directed author-to-author citation networks with cohesion and pairwise
anomaly metrics → aggregated hypothesis-test summaries and the final
author-feature table consumed by the Python analysis.

The full DAG is defined in the `SQL_STEPS` variable in the [Makefile](Makefile).
See also `python/diagrams.py` for the diagrammatic rendering used in the paper.

## Python scripts

| Script | Purpose |
|---|---|
| `citation_analysis.py` | Main entry point: matched-pair statistics, all paper figures and tables. |
| `hybrid_outlier_analysis.py` | Isolation Forest + Cohesion-Composite outlier pipeline. |
| `investigate_authors.py` | Audit and outlier-case drill-down tables (Appendix). |
| `match_authors.py` | Greedy h5-aware matching of Case to Control authors. |
| `eigenfactor.py` | Per-subject Eigenfactor computation via power iteration. |
| `insert_predictions.py` | Load ISSN → subject predictions into `impact.db`. |
| `diagrams.py` | Generates the analysis-DAG figure. |
| `get_tables.py` | Small introspection helper (list tables and row counts). |

## Tests

RDBUnit fixtures in `tests/` verify the core derived tables (e.g.
`author_profiles`, `author_matched_pairs`, `citation_network_final`,
`author_behavior_metrics`). Run them with:

```bash
make tests
```

This target requires [`rdbunit`](https://github.com/dspinellis/rdbunit) to
be on `PATH`.

## Citation

If you use this software, please cite the paper and this replication
package; the machine-readable metadata is in [`CITATION.cff`](CITATION.cff).

## Licence

Source code is released under the [MIT Licence](LICENSE). The derived data
tables published on Zenodo are released under CC BY 4.0.
