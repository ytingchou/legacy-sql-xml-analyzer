# Legacy SQL XML Analyzer

Analyze legacy SQL XML mapping files, resolve cross-query references, lint Delphi and Oracle conventions, and emit artifacts that are usable by humans and 128k-token LLM workflows.

The tool also supports a self-calibration flow for environments where real XML samples cannot leave the company boundary: observe real XML shapes, infer a reusable rule profile, freeze it, and then analyze with that profile.

Current local release: `v0.10.0`

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
- `analysis/executive_complexity.csv`: spreadsheet-ready complexity hotspot export
- `analysis/executive_value.csv`: spreadsheet-ready value hotspot export
- `analysis/executive_diagnostics.csv`: spreadsheet-ready diagnostic hotspot export
- `analysis/executive_trend.csv`: spreadsheet-ready run trend export
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

Lifecycle states currently supported:

- `candidate`: newly inferred or patched profile, not yet trusted
- `trial`: profile has demonstrated at least one meaningful improvement
- `trusted`: profile has repeated successful improvements without regressions
- `deprecated`: profile regressed and should not remain active

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
