# Changelog

## 0.8.0 - 2026-03-15

- Added `propose-rules` to collect accepted reviewed weak-LLM patch candidates and build a merged candidate profile.
- Added `apply-profile-patch` so proposal bundles can be merged into a base profile without editing JSON by hand.
- Added `simulate-profile` to validate a candidate profile against the XML corpus before promotion.
- Added tests covering candidate profile generation, patch application, and simulation improvements.

## 0.7.0 - 2026-03-15

- Added staged weak-LLM prompt packs so each failure cluster now emits `classify`, `propose`, and `verify` prompts instead of a single monolithic ask.
- Added `review-llm-response` to validate weak-model JSON answers, repair common formatting failures, and generate follow-up prompts for the next stage.
- Added safe profile patch candidate generation for supported reusable rule types such as XML alias mappings, token wrappers, target default order, and ignored tags.
- Added tests covering staged prompt-pack output and LLM response review workflows.

## 0.6.0 - 2026-03-15

- Added failure clustering so repeated diagnostics are grouped into reusable issue families.
- Added weak-LLM prompt pack generation with single-task prompts, compact evidence, and fixed JSON answer schemas.
- Added `prepare-prompt` so a specific failure cluster can be repackaged for a chosen token budget and model profile.
- Added tests covering failure cluster generation and prompt-pack output.

## 0.5.0 - 2026-03-15

- Added trend-aware executive reporting that reads repeated run snapshots and summarizes whether quality is improving, stable, or regressing.
- Added CSV exports for complexity hotspots, value hotspots, diagnostic hotspots, and history trend data for spreadsheet workflows.
- Enhanced the static dashboard with progress trend sparklines and recent snapshot comparisons.
- Added tests covering trend summaries and executive CSV output.

## 0.4.0 - 2026-03-15

- Added executive summary artifacts in JSON and Markdown plus a static HTML dashboard for management reporting.
- Added complexity and value hotspot ranking so large SQL XML projects can be summarized for stakeholders.
- Added `serve-report` to host the generated dashboard locally through a lightweight stdlib web server.
- Added tests covering executive summary and dashboard generation.

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
