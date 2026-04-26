# citation-cliques

[![DOI](https://zenodo.org/badge/1220135353.svg)](https://doi.org/10.5281/zenodo.19786936)

Replication package for *Citation Cliques in Low Impact Journals* (Spanakis,
Alexandrou & Spinellis, 2026). The repository contains the SQL pipeline, Python
analysis scripts, and RDBUnit test fixtures needed to reproduce every figure
and table in the paper from the Crossref-derived SQLite databases.

The manuscript source is maintained separately at
[`gregalexan/citation-cliques-latex`](https://github.com/gregalexan/citation-cliques-latex).

## Repository layout

```
.
├── citation_manipulation/   Pipeline root (runs the Makefile from here)
│   ├── Makefile             Top-level driver: populate, pipeline, analysis
│   ├── sql/                 SQLite pipeline scripts (ordered in the Makefile)
│   ├── python/              Analysis, matching, figure, and diagnostic scripts
│   ├── tests/               RDBUnit fixtures for the critical derived tables
│   ├── data/                Small input CSVs (ISSN subjects, eigenfactor scores)
│   └── pyproject.toml / uv.lock   Pinned Python environment (managed with uv)
├── common/                  Shared Makefile rules for alexandria3k pipelines
└── CITATION.cff, LICENSE, README.md
```

The `citation_manipulation/Makefile` includes `../common/Makefile`, which in
turn pulls in [`simple-rolap`](https://github.com/dspinellis/simple-rolap)
(cloned automatically on first invocation).

## Data

The raw databases (`rolap.db` and `impact.db`, approximately 15 GB and
119 GB respectively) are **not** stored in this repository. They are
regenerated from the public [Crossref](https://www.crossref.org/)
public data file using
[`alexandria3k`](https://github.com/dspinellis/alexandria3k), driven by
the `populate` target in the Makefile.

> **Expect 1–2 days of wall-clock time** for the full download,
> ingestion, and downstream derivation on a modern workstation. Crossref
> is several hundred GB compressed, ingestion is I/O-bound, and the SQL
> pipeline adds several additional hours of processing. At least 300 GB
> of free disk headroom is recommended.

The Crossref archive itself is not redistributed here; by default the
Makefile looks for it at `/home/repo/Crossref-2025` (override via the
`CROSSREF_DIR` variable — see `common/Makefile` for the full list of
tunable paths). The two small CSVs that seed subject classification and
Eigenfactor scores are included in
[`citation_manipulation/data/`](citation_manipulation/data).

## Quick start

```bash
# 0. Install dependencies: uv (https://docs.astral.sh/uv/),
#    sqlite3 >= 3.38, a3k (https://github.com/dspinellis/alexandria3k),
#    and rdbunit for the test suite (optional).

cd citation_manipulation

# 1. Sync the Python environment
uv sync

# 2. Populate the raw database from Crossref (long-running: 1–2 days).
#    Override CROSSREF_DIR if your Crossref dump lives elsewhere.
make populate CROSSREF_DIR=/path/to/Crossref-2025

# 3. Run the SQL pipeline (derived tables, matched pairs, networks)
make pipeline

# 4. Run the analysis and regenerate figures and tables
make analysis
```

## Pipeline overview

The SQL steps build, in order: subject-aware Eigenfactor percentiles →
author portfolios and tier assignments → matched Case/Control pairs →
directed author-to-author citation networks with cohesion and pairwise
anomaly metrics → aggregated hypothesis-test summaries and the final
author-feature table consumed by the Python analysis.

See `citation_manipulation/python/diagrams.py` for the DAG figure used in
the paper.

## Python scripts

| Script | Purpose |
|---|---|
| `citation_analysis.py` | Main entry point: matched-pair statistics and every paper figure and table. |
| `hybrid_outlier_analysis.py` | Isolation Forest + Cohesion-Composite outlier pipeline. |
| `investigate_authors.py` | Audit and outlier-case drill-down tables (Appendix). |
| `match_authors.py` | Greedy h5-aware matching of Case to Control authors. |
| `eigenfactor.py` | Per-subject Eigenfactor computation via power iteration. |
| `insert_predictions.py` | Load ISSN → subject predictions into `impact.db`. |
| `diagrams.py` | Generates the analysis-DAG figure. |
| `get_tables.py` | Small introspection helper (list tables and row counts). |

## Tests

RDBUnit fixtures in `citation_manipulation/tests/` verify the core derived
tables (`author_profiles`, `author_matched_pairs`, `citation_network_final`,
`author_behavior_metrics`, and others). They require
[`rdbunit`](https://github.com/dspinellis/rdbunit) on `PATH`.

## Citation

If you use this software, please cite the paper and this replication
package; the machine-readable metadata is in [`CITATION.cff`](CITATION.cff).

## Licence

Source code is released under the [MIT Licence](LICENSE). Derived data
tables published alongside this repository are released under CC BY 4.0.
