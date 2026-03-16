# Changelog

## 1.23.0 - 2026-03-16

- Added `retry-from-doctor`, which turns the latest doctor diagnosis into a retry-ready plan with generated repair handoff packs and adaptive prompt variants.
- Added handoff lifecycle tracking under `analysis/handoff/*/lifecycle.json` so operators can see when a prompt pack is new, used, repaired, or resolved.
- Extended `watch-and-review` so it can update handoff lifecycle state and emit adaptive retry artifacts automatically after a rejected response.
- Added doctor response scoreboards, Java phase-queue summaries, history trends, and latest-review retry targeting for faster operator decisions.
- Expanded company weak-model prompt profiles with minimal good/bad examples so Qwen3-class models get tighter output guidance.
- Extended Java starter output with `dto_contract.json`, `quality_gate.json`, and `delivery_summary.json|.md`, plus a more useful Oracle SQL resource assembler.
- Added `analysis/bundle_explorer.html` and enriched `analysis/operator_console.html` with queue, scoreboard, handoff-state, and retry visibility.
- Added tests covering doctor retry planning, handoff lifecycle updates, stronger company prompt rendering, and the new starter quality/delivery artifacts.

## 1.19.0 - 2026-03-16

- Added `doctor-run` so an analysis output can now be diagnosed into provider, loop, failure, and next-command recommendations without manual artifact hunting.
- Added `watch-and-review` so saved Cline or provider responses can be watched, reviewed automatically, and turned into repair handoff packs when needed.
- Added `compile-adaptive-context` and `shrink-prompt` so weaker company models can be retried with smaller prompt/context variants instead of reassembling prompts by hand.
- Added `analysis/operator_console.html`, a single-page operator view for loop state, doctor guidance, recent handoff packs, and recommended commands.
- Extended Java BFF skeleton generation to emit a starter Spring Boot project scaffold with `pom.xml`, `application.yml`, SQL resource placeholders, verification checklists, and merge-guard artifacts.
- Extended Java BFF loop completion so starter-project manifests, verification checklists, and merge-guard artifacts are part of the ready-to-handoff output set.
- Added tests covering doctor-run, adaptive prompt generation, watch-and-review, and starter project artifacts.

## 1.15.0 - 2026-03-16

- Added weak-model response normalization for generic and Java BFF reviews, including fenced JSON cleanup, noisy wrapper trimming, Python-literal parsing, Cline JSON event extraction, and saved normalization artifacts.
- Added `explain-failure` plus `analysis/failure_explanations/*` so stalled loops, rejected reviews, and provider failures now generate actionable troubleshooting cards, recommended commands, and copy-ready company-LLM prompts.
- Added company-specific prompt-pack and handoff commands: `emit-company-prompt`, `repair-company-prompt`, and `export-vscode-cline-pack`.
- Added `analysis/handoff/*` packs with `prompt.txt`, `schema.json`, `response_template.json`, operator notes, and pack metadata for Cline CLI or the VS Code Cline extension.
- Added `analysis/prompt_lab.html` and `analysis/failure_console.html` so operators can browse prompt/context assets and troubleshooting summaries in a browser.
- Expanded the artifact catalog and README with the new failure-explanation, handoff-pack, normalization, and web-console artifacts.
- Added tests covering response normalization, failure explanation generation, company prompt rendering, and handoff pack export.

## 1.11.0 - 2026-03-16

- Added `docs/QUICKSTART_CLINE.md`, a task-focused quick start guide for combining analyzer artifacts with Cline CLI or the VS Code Cline extension.
- Added a top-level README quick start entry so the Cline-oriented workflow is easier to discover.
- Documented the fastest Java BFF and generic analyzer paths for weak 128k-token models such as Qwen3, including when to use the automatic bridge loop versus manual phase/context review.

## 1.10.0 - 2026-03-16

- Added a reusable `tools/cline_bridge.py` wrapper plus `src/legacy_sql_xml_analyzer/cline_bridge.py` so generic analyzer loops and Java BFF loops can hand off phase tasks to the installed Cline CLI through a stable file-based contract.
- Added built-in Cline execution profiles (`cline-json`, `cline-json-yolo`, `cline-text`, `cline-text-yolo`) so the bridge can invoke Cline without hand-written shell templates.
- Added automatic parsing of Cline `--json` event-line output so bridge results now capture the final assistant response instead of raw progress logs.
- Added top-level CLI flags such as `--cline-bridge-profile`, `--cline-cwd`, `--cline-model`, and related `--cline-*` options on run/resume loop commands so users can generate the bridge command automatically without manually composing `--cline-bridge-command`.
- Expanded README guidance for the fast Qwen3/Cline workflow and added tests covering bridge execution, Cline JSON extraction, and auto-generated loop bridge commands.

## 1.9.1 - 2026-03-16

- Added explicit `stream: false` to OpenAI-compatible chat/completions requests.
- Added SSE (`text/event-stream`) fallback reconstruction so providers that still stream chat completions can be consumed as normal completion payloads.
- Improved non-JSON provider diagnostics with content-type-aware hints and response previews for HTML gateway pages, SSE streams, and vendor-specific plaintext envelopes.
- Added tests covering SSE reconstruction and stronger non-JSON provider diagnostics.

## 1.9.0 - 2026-03-16

- Added a shared CLI console/reporting layer with global `--verbose` and `--no-progress` flags across the command surface.
- Added live progress reporting for the autonomous analyzer loop and Java BFF loop so phase, prompt, attempt, and stop-state transitions are visible while the command runs.
- Added formatted CLI failure blocks with actionable hints, related artifact paths, and optional traceback output in verbose mode.
- Added `validate-provider` artifacts for provider connectivity and compatibility debugging, including saved request/response/debug payloads and failure categorization.
- Added tests covering console formatting and provider validation success/failure debug flows.

## 1.8.0 - 2026-03-16

- Added `compile-java-bff-context` plus `analysis/java_bff/context_packs/*/*.json|*.md|*.txt` so Java Spring Boot BFF phase prompts are recompiled into token-budgeted context packs before weak-model execution.
- Tightened Java BFF review validation to cross-check query ids, chunk ids, repository method names, SQL parameter bindings, accepted upstream phase outputs, token-budget readiness, and guess-risk handling.
- Added Java BFF Cline-bridge task contracts under `analysis/java_bff/tasks/*/*.json` and normalized bridge/provider task results under `analysis/java_bff/agent_runs/*/*.result.json`.
- Expanded Java BFF loop completion tracking so context packs and Cline-bridge task/result artifacts are part of the required artifact contract.
- Expanded the executive dashboard and artifact catalog with Java BFF context-pack, task, and task-result coverage for management reporting and operator visibility.
- Added tests covering Java BFF context compilation, stricter weak-model review guardrails, verify-phase readiness checks, and Cline-bridge task contract generation.

## 1.5.0 - 2026-03-16

- Added Java BFF phase-runtime commands for weak-model workflows: `invoke-java-bff`, `review-java-bff-response`, `merge-java-bff-phases`, and `generate-java-bff-skeleton`.
- Added resumable Java BFF autonomous loop commands: `run-java-bff-loop`, `resume-java-bff-loop`, and `inspect-java-bff-loop`.
- Added Java BFF phase review, merged implementation-plan, skeleton manifest, and skeleton handoff README artifacts.
- Added Java Spring Boot skeleton generation for repository, service, controller, and DTO files driven by accepted phase outputs.
- Added Java BFF loop completion tracking so the loop stops only after required review, merge, and skeleton artifacts are complete or a clear stop condition is reached.
- Expanded dashboard and artifact catalog coverage for Java BFF reviews, merged plans, skeleton bundles, and loop state.
- Added tests covering Java BFF review/merge/skeleton workflow and autonomous loop completion with a fake runner.

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
