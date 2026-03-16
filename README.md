# Legacy SQL XML Analyzer

Analyze legacy SQL XML mapping files, resolve cross-query references, lint Delphi and Oracle conventions, and emit artifacts that are usable by humans and 128k-token LLM workflows.

The tool also supports a self-calibration flow for environments where real XML samples cannot leave the company boundary: observe real XML shapes, infer a reusable rule profile, freeze it, and then analyze with that profile.

Current local release: `v1.11.0`

Quick start guides:

- [Cline CLI / VS Code Cline Quick Start](./docs/QUICKSTART_CLINE.md)

Most CLI commands also support:

- `--verbose`: print extra debug detail and traceback information on failure
- `--no-progress`: suppress live progress lines when you want quieter logs

## Usage

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer analyze --input ./xml --output ./analysis
```

## Learning Workflow

Observe real XML files and capture structural patterns:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer learn --input ./xml --output ./artifacts
```

Infer rule candidates from the observations:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer infer-rules --input ./artifacts/learning/observations.json --output ./artifacts
```

Freeze the inferred rules into a reusable profile:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer freeze-profile --input ./artifacts/learning/rule_candidates.json --output ./profiles/company_profile.json
```

Analyze with the frozen profile:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer analyze --input ./xml --output ./analysis --profile ./profiles/company_profile.json
```

When `analyze` runs with `--profile`, it also emits:

- `analysis/applied_rules.json`: the active profile defaults, retained rules, and runtime usage counters
- `analysis/fix_delta.json`: baseline-vs-profiled comparison for resolved queries and diagnostics
- `analysis/fix_delta.md`: a compact human-readable delta summary

When `analyze` runs with `--snapshot-label`, it also persists run history:

- `analysis/run_snapshot.json`: the current run summary
- `analysis/history/latest.json`: the latest run in this output directory
- `analysis/history/index.json`: accumulated run history for repeated executions
- `analysis/executive_summary.json`: machine-readable management summary, complexity hotspots, and value hotspots
- `analysis/executive_summary.md`: concise report for status updates and leadership reviews
- `analysis/dashboard.html`: static web dashboard for browsing results in a browser
- `analysis/evolution_summary.json`: machine-readable weak-LLM evolution and prompt effectiveness summary
- `analysis/evolution_summary.md`: concise operator summary for weak-LLM throughput and repair loops
- `analysis/evolution_console.html`: static web console for LLM runs, review outcomes, and repair hotspots
- `analysis/executive_complexity.csv`: spreadsheet-ready complexity hotspot export
- `analysis/executive_value.csv`: spreadsheet-ready value hotspot export
- `analysis/executive_diagnostics.csv`: spreadsheet-ready diagnostic hotspot export
- `analysis/executive_trend.csv`: spreadsheet-ready run trend export
- `analysis/llm_effectiveness.csv`: spreadsheet-ready provider/stage effectiveness export
- `analysis/profile_lifecycle.csv`: spreadsheet-ready lifecycle event export for the active profile
- `analysis/prompt_scoreboard.json`: machine-readable provider/stage prompt scoreboard
- `analysis/prompt_scoreboard.csv`: spreadsheet-ready provider/stage prompt scoreboard
- `analysis/schema/artifact_catalog.json`: stable machine-readable artifact contract catalog
- `analysis/schema/artifact_catalog.md`: human-readable artifact contract catalog
- `analysis/failure_clusters.json`: grouped diagnostic families for repeated issues
- `analysis/failure_clusters.md`: human-readable failure family summary
- `analysis/prompt_packs/*.txt`: staged weak-LLM prompt packs (`classify`, `propose`, `verify`) plus a backward-compatible propose alias
- `analysis/prompt_packs/*.json`: prompt metadata, stage schemas, and bundle metadata
- `analysis/llm_reviews/*.json`: reviewed weak-LLM responses, patch candidates, and follow-up prompt metadata
- `analysis/llm_reviews/*.md`: human-readable review summaries for weak-LLM responses
- `analysis/llm_runs/*/request.json`: sanitized OpenAI-compatible request payloads
- `analysis/llm_runs/*/response.json`: raw provider responses
- `analysis/llm_runs/*/response.txt`: extracted assistant text responses
- `analysis/llm_runs/*/run_summary.json`: saved provider, model, token-limit, and usage metadata
- `analysis/llm_runs/index.json`: aggregated weak-LLM run history used by the evolution scoreboard
- `analysis/proposals/rule_proposals.json`: accepted patch candidates collected from reviewed weak-LLM answers
- `analysis/proposals/candidate_profile.json`: merged candidate profile generated from accepted patch candidates
- `grade/profile_grade.json`: lifecycle grading for a candidate or promoted profile
- `grade/profile_grade.md`: human-readable lifecycle grading summary

Validate whether a frozen profile is actually helping:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer validate-profile --input ./xml --output ./validation --profile ./profiles/company_profile.json
```

`validate-profile` emits:

- `validation/profile_validation.json`: machine-readable assessment, deltas, and rule usage
- `validation/profile_validation.md`: human-readable summary with recommendation

Serve the generated dashboard locally:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer serve-report --root ./analysis-output --port 8000
```

Generate or regenerate a prompt pack for a specific failure cluster:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer prepare-prompt --analysis-root ./analysis-output --cluster reference_target_missing --budget 32k --model weak-128k
```

Review a weak-LLM response and convert valid profile-rule proposals into a draft patch candidate:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer review-llm-response --analysis-root ./analysis-output --cluster reference_target_missing --response ./llm-response.json --stage propose --budget 32k --model weak-128k
```

`prepare-prompt` now emits three prompt stages for each cluster:

- `classify`: tell the weak model to identify the failure family and missing evidence only
- `propose`: ask for the smallest safe rule or XML/SQL fix
- `verify`: re-check a proposal against analyzer constraints before you trust it

`review-llm-response` accepts imperfect weak-model output and will:

- strip common markdown code fences around JSON answers
- validate the reply against the expected stage schema
- generate a repair prompt when the JSON or fields are invalid
- generate a follow-up prompt for the next stage when the answer is usable
- emit a profile patch candidate for safe rule types such as XML alias mappings or token patterns

Invoke an OpenAI-compatible provider directly from a staged prompt pack:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer invoke-llm --analysis-root ./analysis-output --cluster reference_target_missing --stage propose --budget 32k --provider-base-url https://provider.example.com/v1 --provider-model your-model --provider-api-key-env OPENAI_API_KEY --token-limit 2048 --review
```

You can also keep provider settings in a JSON file:

```json
{
  "name": "company-weak-llm",
  "base_url": "https://provider.example.com/v1",
  "model": "your-model",
  "api_key_env": "OPENAI_API_KEY",
  "token_limit": 2048,
  "temperature": 0.0,
  "timeout_seconds": 60.0
}
```

Then invoke it with:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer invoke-llm --analysis-root ./analysis-output --cluster reference_target_missing --provider-config ./provider.json --review
```

`invoke-llm` is designed for OpenAI-compatible `/chat/completions` providers and saves:

- the sanitized request payload, without embedding the API key
- the raw JSON provider response
- the extracted assistant text
- token-limit and usage metadata for later debugging
- optional automatic review output when `--review` is enabled

Validate an OpenAI-compatible provider before you run weak-model workflows:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer validate-provider --output ./analysis-output --provider-config ./provider.json
```

`validate-provider` sends a short probe request and saves:

- `analysis/provider_validation/*/summary.json`: pass/fail result, checks, and troubleshooting hints
- `analysis/provider_validation/*/debug.json`: config snapshot, normalized URL, prompt stats, and failure classification
- `analysis/provider_validation/*/request.json`: sanitized request payload without the API key
- `analysis/provider_validation/*/response.json`: raw provider JSON when the request succeeds
- `analysis/provider_validation/*/response.txt`: extracted assistant text when the response shape is compatible

This is useful when a company provider is only partially OpenAI-compatible or when you need to isolate whether the problem is API key, endpoint path, network reachability, response JSON shape, or JSON-only instruction following.

Compile a phase-specific context pack for weak 128k-token models such as Qwen3:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer compile-context --analysis-root ./analysis-output --cluster reference_target_missing --phase propose --prompt-profile qwen3-128k-autonomous
```

Run the autonomous multi-phase agent loop until required artifacts are complete or a stop condition is reached:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-agent-loop --input ./xml --output ./analysis-output --runner-mode provider --provider-config ./provider.json --prompt-profile qwen3-128k-autonomous
```

Resume or inspect a previously started loop:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-agent-loop --output ./analysis-output
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer inspect-agent-loop --output ./analysis-output
```

The autonomous loop adds:

- `analysis/context_packs/*.json|*.md|*.txt`: compact phase-specific context packs tuned for weak 128k-token models
- `analysis/agent_runs/*.result.json`: normalized provider or bridge task results
- `analysis/agent_tasks/*.json`: queued Cline-bridge tasks for external subagent execution
- `analysis/agent_loop/loop_state.json`: resumable loop state, token budget, and progress metadata
- `analysis/agent_loop/phase_history.json`: per-phase execution history
- `analysis/agent_loop/completion_report.json`: terminal status with missing required artifacts

The built-in `qwen3-128k-*` prompt profiles are intentionally conservative:

- each phase handles one cluster at a time
- `classify`, `propose`, and `verify` use separate context packs
- `verify` is not allowed to invent a new rule
- `insufficient_evidence` is treated as a valid weak-model answer
- output headroom is reserved so 128k-token models do not run out of completion budget

If you want Cline CLI to act as the executor instead of calling the provider directly, use the file-based bridge contract:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-agent-loop \
  --input ./xml \
  --output ./analysis-output \
  --runner-mode cline_bridge \
  --cline-bridge-profile cline-json \
  --cline-cwd /path/to/your/workspace
```

The main CLI will generate the underlying `tools/cline_bridge.py` command for you. If you still want full manual control, `--cline-bridge-command` remains available.

The generic loop writes:

- `analysis/agent_tasks/*.json`: pending tasks for Cline
- `analysis/agent_runs/*.result.json`: result contract that the bridge must write back

`tools/cline_bridge.py` supports three execution styles:

- `--command-profile`: built-in profiles for the installed `cline` CLI. Start with `cline-json`, or use `cline-json-yolo` if you explicitly want `--yolo`
- `--stdin-command`: send the compiled prompt to the external command over stdin and capture stdout
- `--command-template`: run a shell template with placeholders like `{prompt_file}`, `{response_file}`, and `{task_file}`

The built-in profiles understand the current Cline CLI shape and avoid hand-writing shell commands every time:

- `cline-json`
- `cline-json-yolo`
- `cline-text`
- `cline-text-yolo`

You can also tune the built-in profile with:

- `--cline-command`: alternate executable, for example `npx cline` or a wrapper script
- `--cline-cwd`: workspace passed to `cline task --cwd`
- `--cline-model`
- `--cline-config`
- `--cline-extra-args`
- `--cline-timeout`
- `--cline-verbose-output`
- `--cline-double-check-completion`

The same `--cline-*` knobs are also available directly on `run-agent-loop`, `resume-agent-loop`, `run-java-bff-loop`, and `resume-java-bff-loop` whenever you use `--runner-mode cline_bridge --cline-bridge-profile ...`.

When you use `cline-json` or `cline-json-yolo`, the bridge parses Cline's JSON event lines and extracts the final assistant response automatically before writing `*.result.json`.

For generic tasks the bridge materializes:

- `<analysis>/agent_runs/<task_id>.prompt.txt`
- `<analysis>/agent_runs/<task_id>.response.txt`
- `<analysis>/agent_runs/<task_id>.result.json`

Prepare a Java Spring Boot BFF artifact pack for weak models such as Qwen3, with Oracle 19c SQL logic split into token-safe chunks:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer prepare-java-bff --input ./xml --output ./analysis-output --prompt-profile qwen3-128k-java-bff
```

Focus the pack on a single entry query when you want the weakest possible prompting context:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer prepare-java-bff --input ./xml --output ./analysis-output --entry-file orders.xml --entry-main-query OrderSearch
```

`prepare-java-bff` emits:

- `analysis/java_bff/overview.json`: project-level weak-model strategy for Java Spring Boot BFF + Oracle 19c
- `analysis/java_bff/chunk_manifest.json`: bundle and chunk manifest with token estimates and recommended sequence
- `analysis/java_bff/implementation_cards/*.json`: per-query implementation cards with SQL logic, binding hints, and manual-review flags
- `analysis/java_bff/sql_chunks/*.json`: chunked SQL excerpts sized for weak-model repository prompts
- `analysis/java_bff/bundles/*/bundle.json`: per-entry bundle plans with ordered phase prompts
- `analysis/java_bff/phase_packs/*/*.json`: phase-specific prompt payloads and answer schemas for `plan`, `repository chunk`, `repository merge`, `bff assembly`, and `verify`

The Java BFF pack is specifically tuned for a weak 128k-token model:

- never feed the full `java_bff` folder to Qwen3 in one shot
- feed only one bundle at a time
- feed one `phase-2-repository` chunk prompt at a time
- use `phase-2-repository-merge` only after all chunk-level outputs are complete
- if a query is `partial` or `failed`, treat diagnostics as blockers instead of asking the model to guess missing SQL

Invoke one Java BFF phase pack against an OpenAI-compatible provider:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer invoke-java-bff --analysis-root ./analysis-output --prompt-json ./analysis-output/analysis/java_bff/phase_packs/orders.xml_main_OrderSearch/phase-1-plan.json --provider-config ./provider.json --review
```

Review a saved Java BFF response, merge accepted phase outputs, and generate Java skeletons:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer compile-java-bff-context --analysis-root ./analysis-output --prompt-json ./analysis-output/analysis/java_bff/phase_packs/orders.xml_main_OrderSearch/phase-2-repository-merge-OrderSearch.json --prompt-profile qwen3-128k-java-bff
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer review-java-bff-response --analysis-root ./analysis-output --prompt-json ./analysis-output/analysis/java_bff/phase_packs/orders.xml_main_OrderSearch/phase-1-plan.json --response ./phase-1-plan.response.json
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer merge-java-bff-phases --analysis-root ./analysis-output --bundle-id orders.xml:main:OrderSearch
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer generate-java-bff-skeleton --analysis-root ./analysis-output --bundle-id orders.xml:main:OrderSearch --package-name com.example.legacybff
```

Run the autonomous Java BFF loop so Qwen3-style weak models can execute every phase until the required review, merge, and skeleton artifacts are complete:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-java-bff-loop --input ./xml --output ./analysis-output --runner-mode provider --provider-config ./provider.json --package-name com.example.legacybff
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-java-bff-loop --output ./analysis-output
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer inspect-java-bff-loop --output ./analysis-output
```

The Java BFF loop adds:

- `analysis/java_bff/context_packs/*/*.json|*.md|*.txt`: phase-specific context packs compiled for weak models before provider or bridge execution
- `analysis/java_bff/reviews/*/*-review.json`: accepted or repair-needed phase reviews
- `analysis/java_bff/tasks/*/*.json`: Cline-bridge task contracts with compiled context and answer-schema expectations
- `analysis/java_bff/agent_runs/*/*.result.json`: normalized provider, fake-runner, or bridge task results
- `analysis/java_bff/merged/*/implementation_plan.json`: merged repository, BFF, and verification logic
- `analysis/java_bff/skeletons/*/manifest.json`: emitted Java file manifest
- `analysis/java_bff/skeletons/*/README.md`: handoff readme for the generated skeleton bundle
- `analysis/java_bff/loop/loop_state.json`: resumable loop state
- `analysis/java_bff/loop/completion_report.json`: final status with missing artifact tracking

The same bridge pattern works for Java BFF:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-java-bff-loop \
  --input ./xml \
  --output ./analysis-output \
  --runner-mode cline_bridge \
  --cline-bridge-profile cline-json \
  --cline-cwd /path/to/your/workspace \
  --package-name com.example.legacybff
```

The Java BFF bridge reads:

- `analysis/java_bff/tasks/*/*.json`

and writes:

- `analysis/java_bff/agent_runs/*/*.result.json`

Each Java task already includes:

- `context_prompt_path`
- `context_pack_path`
- `expected_schema`
- `recommended_result_path`

So your bridge only needs to:

1. read the prompt from `context_prompt_path`
2. call Cline
3. write the raw response text
4. write the result JSON to `recommended_result_path`

The Java BFF weak-model workflow now applies stronger reviewer guardrails before a phase is accepted:

- repository chunk responses must keep the expected `query_id`, `chunk_id`, repository `method_name`, and known SQL parameter bindings
- repository and merge phases are rejected when controller or service layer terms leak into JDBC/repository logic
- assembly responses are rejected when JDBC details leak upward into service/controller planning
- verify responses cannot mark a bundle `ready` while token checks fail, required artifacts are missing, or guess risks remain
- responses that suggest rewriting Oracle 19c SQL shape instead of preserving analyzed query logic are rejected

Collect accepted reviewed patches into a candidate profile:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer propose-rules --analysis-root ./analysis-output --min-confidence 0.7
```

Apply a proposal bundle to a base profile and write a merged candidate profile:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer apply-profile-patch --patch-bundle ./analysis-output/analysis/proposals/rule_proposals.json --output ./profiles/candidate_profile.json
```

Simulate the candidate profile against the XML corpus before promotion:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer simulate-profile --input ./xml --analysis-root ./analysis-output --output ./simulation-output
```

This new promotion workflow is intended to be:

- `review-llm-response`: validate weak-model output and produce safe patch candidates
- `propose-rules`: collect only accepted, safe, high-confidence patch candidates
- `simulate-profile`: compare the candidate profile against baseline before any manual promotion

Grade a profile lifecycle state from a validation or simulation report:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer grade-profile --profile ./analysis-output/analysis/proposals/candidate_profile.json --report ./simulation-output/simulation/profile_simulation.json --output ./grade-output
```

Promote the profile to its next lifecycle state:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer promote-profile --profile ./analysis-output/analysis/proposals/candidate_profile.json --grade-report ./grade-output/grade/profile_grade.json --output ./profiles/promoted_profile.json --profile-name company-candidate
```

Rollback to the parent profile after a regression:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer rollback-profile --profile ./profiles/promoted_profile.json --output ./profiles/rollback_profile.json --reason "Regression detected during validation"
```

Lifecycle states currently supported:

- `candidate`: newly inferred or patched profile, not yet trusted
- `trial`: profile has demonstrated at least one meaningful improvement
- `trusted`: profile has repeated successful improvements without regressions
- `deprecated`: profile regressed and should not remain active

Profile promotion and rollback now also emit sibling history files:

- `*.history.json`: machine-readable validation and lifecycle timeline
- `*.history.md`: concise human-readable lifecycle summary

If you keep reusing the same `--output` directory across runs, the executive dashboard will also show trend direction and recent snapshot comparisons.

Learned profiles can currently auto-heal:

- external XML aliases that need filename or cross-folder path mapping
- source-scoped external XML aliases where the same alias points to different modules
- wrapped reference tokens such as `/*TOKEN*/`
- repeated top-level custom tags that should be ignored during parsing

To run the test suite:

```bash
python3 -m unittest discover -s tests
```
