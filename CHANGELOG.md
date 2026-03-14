# Changelog

## 0.3.0 - 2026-03-15

- Added `validate-profile` to compare baseline and profiled analysis, classify the profile as improved/stable/review/regressed, and emit validation reports.
- Added per-run history snapshots under `analysis/history/` with `latest.json`, `index.json`, and `run_snapshot.json`.
- Added `--snapshot-label` to `analyze` so repeated company runs can be tracked and compared over time.
- Added tests for validation classification and run-history persistence.

## 0.2.0 - 2026-03-15

- Added a Python CLI for analyzing legacy SQL XML mappings and generating JSON plus Markdown artifacts.
- Added tolerant reference resolution, diagnostics, and Oracle/Delphi domain lint checks.
- Added learning commands to observe XML structure, infer rule profiles, and freeze reusable profiles.
- Added self-healing support for external XML alias mapping, cross-folder source-scoped mapping, wrapped reference tokens, and ignored top-level custom tags.
- Added applied-rule auditing and baseline-vs-profiled delta reports for profile-driven analysis.
