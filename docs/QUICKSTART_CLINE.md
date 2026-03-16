# Quick Start: Use Artifacts with Cline CLI or VS Code Cline

This guide is the fastest way to get useful output from the analyzer when you are using:

- weak 128k-token models such as Qwen3
- Cline CLI
- or the VS Code Cline extension

The key rule is simple:

- do not feed the whole `analysis/` folder to the model
- do not feed the whole `analysis/java_bff/` folder to the model
- always feed one bundle, one phase, or one chunk at a time

When the flow stalls, do not guess the next command. Use:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer doctor-run --output "$OUT_DIR"
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer retry-from-doctor --output "$OUT_DIR"
```

This gives you:

- `analysis/doctor/doctor_report.json`
- `analysis/doctor/retry_plan.json`
- `analysis/handoff/*`
- `analysis/adaptive_prompts/*`

## 1. Pick the Right Workflow

There are two practical workflows:

1. `Cline CLI + automatic loop`
This is the fastest path when you want the analyzer to drive the phases and Cline to act as the executor.

2. `VS Code Cline extension + manual phase packs`
This is the safest path when you want to inspect prompts, copy them into Cline, and review each phase manually.

If you are in a hurry, start with `Cline CLI + automatic loop`.

## 2. Required Paths

Set these first:

```bash
export XML_DIR=/path/to/sql-xml
export OUT_DIR=/path/to/analyzer-output
export WORKSPACE_DIR=/path/to/java-bff-repo
```

- `XML_DIR`: legacy SQL XML folder
- `OUT_DIR`: analyzer output folder
- `WORKSPACE_DIR`: the repo or workspace that Cline should use

## 3. Fastest Path: Java BFF Artifacts + Cline CLI

If your goal is to produce useful Spring Boot BFF implementation logic, use the Java BFF workflow.

### Step 1: Generate Java BFF artifacts

For large projects, start with one entry query only:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer prepare-java-bff \
  --input "$XML_DIR" \
  --output "$OUT_DIR" \
  --prompt-profile qwen3-128k-java-bff \
  --entry-file <file.xml> \
  --entry-main-query <MainQueryName>
```

This creates:

- `analysis/java_bff/overview.json`
- `analysis/java_bff/chunk_manifest.json`
- `analysis/java_bff/bundles/*/bundle.json`
- `analysis/java_bff/phase_packs/*/*.json`

### Step 2: Run the Java BFF loop with Cline CLI

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-java-bff-loop \
  --input "$XML_DIR" \
  --output "$OUT_DIR" \
  --runner-mode cline_bridge \
  --prompt-profile qwen3-128k-java-bff \
  --package-name com.yourcompany.bff \
  --entry-file <file.xml> \
  --entry-main-query <MainQueryName> \
  --cline-bridge-profile cline-json \
  --cline-cwd "$WORKSPACE_DIR"
```

What happens:

- analyzer creates token-safe phase packs
- analyzer writes bridge task files
- Cline CLI executes the phase prompt
- bridge converts Cline output back into result artifacts
- analyzer reviews, merges, and continues until completion or a clear stop reason

### Step 3: Resume if needed

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-java-bff-loop \
  --output "$OUT_DIR" \
  --cline-bridge-profile cline-json \
  --cline-cwd "$WORKSPACE_DIR"
```

### Step 4: Inspect progress

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer inspect-java-bff-loop \
  --output "$OUT_DIR"
```

### Step 5: Read the final useful outputs

Start here:

- `analysis/java_bff/merged/*/implementation_plan.json`
- `analysis/java_bff/skeletons/*/manifest.json`
- `analysis/java_bff/skeletons/*/README.md`
- `analysis/java_bff/skeletons/*/starter_project/quality_gate.json`
- `analysis/java_bff/skeletons/*/starter_project/delivery_summary.json`
- `analysis/java_bff/loop/completion_report.json`

These are the highest-value outputs:

- `implementation_plan.json`: repository, service, controller, DTO, and SQL logic plan
- `manifest.json`: generated Java skeleton inventory
- `README.md`: handoff notes for implementation
- `quality_gate.json`: delivery blockers and warnings
- `delivery_summary.json`: shortest summary of remaining human work
- `completion_report.json`: tells you whether the loop really finished

## 4. Fastest Path: Generic Parser/Profile Repair + Cline CLI

If your XML references are still unresolved or the Java BFF flow stops because the analyzer cannot model the SQL cleanly, run the generic loop first:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-agent-loop \
  --input "$XML_DIR" \
  --output "$OUT_DIR" \
  --runner-mode cline_bridge \
  --prompt-profile qwen3-128k-autonomous \
  --cline-bridge-profile cline-json \
  --cline-cwd "$WORKSPACE_DIR"
```

Then rerun:

1. `prepare-java-bff`
2. `run-java-bff-loop`

This is the recommended order when the XML corpus contains alias problems, external reference problems, or unresolved placeholders.

## 5. VS Code Cline Extension Workflow

Use this mode when you do not want the automatic loop to call Cline CLI directly.

### Step 1: Generate Java BFF artifacts

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer prepare-java-bff \
  --input "$XML_DIR" \
  --output "$OUT_DIR" \
  --prompt-profile qwen3-128k-java-bff \
  --entry-file <file.xml> \
  --entry-main-query <MainQueryName>
```

### Step 2: Compile one phase context

Pick one phase pack, then compile a token-safe context:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer compile-java-bff-context \
  --analysis-root "$OUT_DIR" \
  --prompt-json "$OUT_DIR/analysis/java_bff/phase_packs/<bundle>/phase-1-plan.json"
```

This creates:

- `analysis/java_bff/context_packs/<bundle>/*.json`
- `analysis/java_bff/context_packs/<bundle>/*.md`
- `analysis/java_bff/context_packs/<bundle>/*.txt`

### Step 3: Open the `.txt` context pack in VS Code

Use the `.txt` file as the prompt body for the Cline extension.

Feed only:

- one bundle
- one phase
- one repository chunk at a time

Do not feed:

- the whole `analysis/` folder
- the whole `analysis/java_bff/` folder
- all repository chunks at once

### Step 4: Save the model response to a file

For example:

- `phase-1-plan.response.json`
- `phase-2-repository-chunk-01.response.json`

### Step 5: Review the response

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer review-java-bff-response \
  --analysis-root "$OUT_DIR" \
  --prompt-json "$OUT_DIR/analysis/java_bff/phase_packs/<bundle>/phase-1-plan.json" \
  --response ./phase-1-plan.response.json
```

### Step 6: Merge accepted phase outputs

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer merge-java-bff-phases \
  --analysis-root "$OUT_DIR" \
  --bundle-id <bundle-id>
```

### Step 7: Generate Java skeletons

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer generate-java-bff-skeleton \
  --analysis-root "$OUT_DIR" \
  --bundle-id <bundle-id> \
  --package-name com.yourcompany.bff
```

## 6. Which Files Should You Actually Open

If you are in a hurry, use this order.

### For Java BFF work

1. `analysis/java_bff/overview.json`
2. `analysis/java_bff/chunk_manifest.json`
3. `analysis/java_bff/bundles/*/bundle.json`
4. `analysis/java_bff/phase_packs/*/*.json`
5. `analysis/java_bff/context_packs/*/*.txt`
6. `analysis/java_bff/merged/*/implementation_plan.json`

### For generic analyzer work

1. `analysis/failure_clusters.json`
2. `analysis/context_packs/*.txt`
3. `analysis/prompt_packs/*.txt`
4. `analysis/llm_reviews/*.json`
5. `analysis/proposals/candidate_profile.json`

## 7. Qwen3 128k Rules

These rules matter more than anything else:

1. One bundle at a time.
2. One phase at a time.
3. For repository generation, one chunk at a time.
4. Prefer `cline-json` over text mode.
5. Use `--entry-file` and `--entry-main-query` first.
6. Treat missing analyzer diagnostics as blockers, not as something the model should guess.

## 8. If Something Fails

### Cline did not produce results

Check:

- `analysis/java_bff/tasks/`
- `analysis/java_bff/agent_runs/`
- `analysis/agent_tasks/`
- `analysis/agent_runs/`

If task files exist but result files do not, the bridge or Cline invocation failed.

### The model returned something but review failed

Check:

- `analysis/java_bff/reviews/`
- `analysis/llm_reviews/`
- `analysis/failure_explanations/`

This usually means:

- wrong phase schema
- non-JSON output
- model drift
- too much context

Generate an actionable diagnosis:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer explain-failure \
  --output "$OUT_DIR"
```

If you want a copy-ready prompt for Cline CLI or the VS Code extension, export a handoff pack instead of assembling your own prompt:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer export-vscode-cline-pack \
  --analysis-root "$OUT_DIR" \
  --prompt-json "$OUT_DIR/analysis/java_bff/phase_packs/<bundle>/phase-1-plan.json"
```

This creates:

- `analysis/handoff/*/prompt.txt`
- `analysis/handoff/*/schema.json`
- `analysis/handoff/*/response_template.json`
- `analysis/handoff/*/operator_notes.md`

If a review already failed and you need the next repair prompt:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer repair-company-prompt \
  --analysis-root "$OUT_DIR" \
  --review "$OUT_DIR/analysis/java_bff/reviews/<bundle>/<phase>-review.json"
```

If you want the analyzer to tell you the safest next command instead of guessing:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer doctor-run --output "$OUT_DIR"
```

If the current prompt is still too large for your company model, generate smaller retry variants:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer compile-adaptive-context \
  --analysis-root "$OUT_DIR" \
  --prompt-json "$OUT_DIR/analysis/java_bff/phase_packs/<bundle>/phase-1-plan.json"
```

### The analyzer itself still has unresolved XML problems

Run the generic loop first:

```bash
PYTHONPATH=src python3 -m legacy_sql_xml_analyzer run-agent-loop \
  --input "$XML_DIR" \
  --output "$OUT_DIR" \
  --runner-mode cline_bridge \
  --prompt-profile qwen3-128k-autonomous \
  --cline-bridge-profile cline-json \
  --cline-cwd "$WORKSPACE_DIR"
```

Then go back to `prepare-java-bff` and `run-java-bff-loop`.

## 9. Recommended Starting Point

If you only have a few hours and need useful output quickly:

1. choose one important `entry-file`
2. choose one important `entry-main-query`
3. run `prepare-java-bff`
4. run `run-java-bff-loop --runner-mode cline_bridge --cline-bridge-profile cline-json`
5. inspect `implementation_plan.json`
6. inspect `skeletons/*/README.md`

That path gives the highest chance of getting usable output without blowing the token budget.
