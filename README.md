# Legacy SQL XML Analyzer

Analyze legacy SQL XML mapping files, resolve cross-query references, lint Delphi and Oracle conventions, and emit artifacts that are usable by humans and 128k-token LLM workflows.

The tool also supports a self-calibration flow for environments where real XML samples cannot leave the company boundary: observe real XML shapes, infer a reusable rule profile, freeze it, and then analyze with that profile.

Current local release: `v0.5.0`

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
