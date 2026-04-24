# `data/` — small inputs

| File | Purpose |
|---|---|
| `eigenfactor_scores_optimized.csv` | Precomputed per-journal Eigenfactor scores (2020–2024) produced by `python/eigenfactor.py`. Loaded into `impact.db` as `eigenfactor_scores`. |
| `predictions_final.csv` | ISSN → broad-subject assignments produced by the LLM classifier described in the paper. Loaded into `impact.db` as `issn_subjects` via `python/insert_predictions.py`. |

The larger databases (`rolap.db`, `impact.db`) are archived on Zenodo — see
the top-level [`README.md`](../README.md) for the download link and
placement instructions.
