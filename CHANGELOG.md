# Changelog

## 1.2.0 - 2026-03-16

- Added `prepare-java-bff` to analyze SQL XML and emit weak-model-friendly Java Spring Boot BFF artifacts for Oracle 19c.
- Added `analysis/java_bff/overview.*`, `chunk_manifest.*`, `implementation_cards/*`, `sql_chunks/*`, `bundles/*`, and `phase_packs/*`.
- Added `qwen3-128k-java-bff` prompt budgeting so large SQL is split into repository chunk prompts that stay below weak-model limits.
- Added phased Java BFF prompt packs for `plan`, `repository chunk`, `repository merge`, `bff assembly`, and `verify`.
- Expanded the artifact catalog with Java BFF contracts for chunked implementation logic generation.
- Added tests covering Java BFF bundle generation and SQL chunk splitting for large queries.

## 1.1.0 - 2026-03-16

- Added autonomous agent loop commands: `run-agent-loop`, `resume-agent-loop`, and `inspect-agent-loop`.
- Added `compile-context` plus phase-specific context packs for weak 128k-token models such as Qwen3.
- Added prompt profiles for `qwen3-128k-classify`, `qwen3-128k-propose`, `qwen3-128k-verify`, and `qwen3-128k-autonomous`.
- Added provider and Cline-bridge runner abstractions plus normalized `analysis/agent_runs/*.result.json` task outputs.
- Added loop artifacts under `analysis/agent_loop/` including resumable state, phase history, and completion reports.
- Expanded the executive dashboard and artifact catalog to cover autonomous loop execution and context-pack artifacts.
- Added tests covering loop state, phase transitions, context compilation, and autonomous loop execution.

## 1.0.0 - 2026-03-15

- Added `rollback-profile` so promoted profiles can safely revert to their parent or an explicit target profile after regressions.
- Added lifecycle sidecar artifacts (`*.history.json` and `*.history.md`) for promoted and rolled-back profiles.
- Added rollback recommendations to `grade-profile` output when a regressed profile still has a recoverable parent profile.
- Added `analysis/evolution_summary.*`, `analysis/prompt_scoreboard.*`, `analysis/evolution_console.html`, `analysis/llm_effectiveness.csv`, and `analysis/profile_lifecycle.csv`.
- Added `analysis/llm_runs/index.json` so repeated provider runs can be aggregated without rescanning every run directory.
- Added `analysis/schema/artifact_catalog.*` so humans, automation, and weaker LLMs can rely on stable artifact contracts.
- Expanded the executive dashboard with LLM evolution, provider scoreboard, and profile lifecycle sections.
- Added tests covering rollback behavior, lifecycle history artifacts, and automatic evolution report generation.

## 0.10.0 - 2026-03-15

- Added `invoke-llm` for direct OpenAI-compatible `/chat/completions` provider integration using staged prompt packs.
- Added configurable provider token limits, temperature, timeout, API key env lookup, and JSON provider config support.
- Added saved `analysis/llm_runs/` artifacts for request payloads, raw responses, extracted text, and run summaries.
- Relaxed SQL XML parameter linting so parameter names no longer need `:` and `data_type` is no longer type-validated.
- Added tests covering direct provider invocation with configurable token limits and automatic review.

## 0.9.0 - 2026-03-15

- Added lifecycle metadata to profiles, including status, parent profile, display name, and validation history.
- Added `grade-profile` to turn simulation or validation results into a lifecycle recommendation such as `trial` or `trusted`.
- Added `promote-profile` to stamp lifecycle state and validation history into a new promoted profile JSON.
- Added tests covering candidate-to-trial promotion and repeated-improvement promotion to trusted.

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
