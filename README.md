# Pipeline

Pipeline is a local, YAML-driven record processing system for messy CSV exports.

It is designed to help an operator take inconsistent source files, normalize them into canonical fields, compare them against a reference dataset, score likely matches, and export clean review-ready outputs without relying on a remote backend.

The project includes:

- a Python CLI for repeatable jobs
- a local web UI for non-terminal workflows
- configurable normalization profiles
- matching, enrichment, scoring, and export stages
- job specs that describe input files, mappings, stages, and outputs
- custom stage-sequence jobs for advanced composition

## Why This Project Exists

Many CSV-heavy workflows break down because source files are inconsistent:

- headers change across exports
- addresses are split or malformed
- duplicate detection depends on brittle exact matches
- operators need review buckets, not just pass/fail output

This project turns those problems into a repeatable local workflow.

## About This Project

This project started from real scripts and YAML workflows I originally wrote for CSV-heavy EDA operations: cleaning files, reconciling records, matching against reference data, and exporting workable outputs.

Over time, I turned that original workflow set into a more reusable local tool with a shared engine, shipped demos, tests, and a browser UI so future operators would not need to work entirely from the terminal.

I also used Codex CLI to help finish and harden the project. I leveraged it to speed up refactoring, UI work, demo/test coverage, and documentation, while building from the original workflow design and real use cases.

## What It Does

Pipeline can:

- normalize source files into canonical field names
- split or clean address data before matching
- compare primary records against a reference dataset
- classify rows into confident matches, review matches, and unmatched records
- enrich rows with reference-side fields
- score priority for follow-up workflows
- export result files and a `run_summary.json` for each run

## Main Interfaces

### CLI

The main entrypoint is:

```bash
python3 pipeline_runner.py
```

You can:

- list available workflows
- describe workflow inputs and thresholds
- inspect headers from source files
- suggest canonical field mappings
- validate job specs
- run complete jobs

Examples:

```bash
python3 pipeline_runner.py list
python3 pipeline_runner.py describe match_records_to_reference
python3 pipeline_runner.py validate-job jobs/demo_match_job.yaml
python3 pipeline_runner.py run-job jobs/demo_match_job.yaml
python3 pipeline_runner.py run profiles/demo_split.yaml
```

### Local Web App

The project also includes a browser-based local UI for operators who do not want to manage YAML by hand.

Start it with:

```bash
./run_webapp.sh
```

Then open:

```text
http://127.0.0.1:8765
```

The web app supports:

- workflow selection grouped by intent
- richer workflow detail panes under the workflow picker
- file inspection
- grouped header-family inspection
- suggested field mappings
- ordered fallback mappings per canonical field
- clickable header chips that can populate mapping slots
- normalization profile selection
- a first-pass `custom_job` stage-plan editor
- richer stage detail panes inside the custom stage plan
- quick run-control presets plus collapsible advanced settings
- preset saving
- asynchronous background job runs with status polling
- output review previews

The top-level UI is now organized as:

- `Prep`: one-file cleanup and normalization
- `Match`: compare, match, and custom reconciliation flows
- `Utilities`: one-off operational jobs such as split, projection, and reference enrichment

## Architecture

At a high level, the system works like this:

1. Load source datasets from CSV into pandas dataframes.
2. Apply optional normalization profiles and text cleanup.
3. Rename or coalesce source-specific headers into canonical fields.
4. Build a preset or custom stage sequence.
5. Run matching, enrichment, classification, scoring, and export stages through the shared execution path.
6. Write output CSVs plus a `run_summary.json`.

In the web app, runs are now backgrounded rather than kept on one long blocking request:

- `POST /api/run-job-async` starts the run
- `GET /api/job-status?id=...` reports queued, running, completed, or failed state
- the browser polls status until results are ready

The orchestration layer is now partly engine-driven:

- shared runtime loading
- shared stage registry
- shared reporting
- preset adapters for shipped workflows
- direct custom jobs through `stage_sequence`

Important code areas:

- [pipeline_runner.py](pipeline_runner.py): CLI entrypoint and workflow execution
- [core/runtime_loader.py](core/runtime_loader.py): shared dataset loading and canonical mapping
- [core/stages.py](core/stages.py): reusable stage registry and stage runner
- [core/preset_plans.py](core/preset_plans.py): preset stage-plan builders
- [core/runtime_reporting.py](core/runtime_reporting.py): shared run summary helpers
- [webapp/server.py](webapp/server.py): local HTTP UI
- [core/jobs.py](core/jobs.py): runtime config and job loading
- [services/workflow_service.py](services/workflow_service.py): workflow metadata, validation, and mapping suggestions
- [normalize](normalize): normalization helpers
- [reconcile](reconcile): matching and reconciliation logic
- [enrich](enrich): field enrichment
- [score](score): priority scoring
- [export](export): output projection/export logic

## Example Workflow

One common use case is comparing a new incoming file against an existing reference file to determine:

- which rows are strong matches
- which rows need manual review
- which rows are likely truly new

The fastest shipped sample job file is:

- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml)

There is also a shipped synthetic demo pack for safe public walkthroughs:

- [demo_data](demo_data)
- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml)
- [jobs/demo_custom_match_job.yaml](jobs/demo_custom_match_job.yaml)
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml)
- [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml)
- [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml)
- [profiles/demo_enrich.yaml](profiles/demo_enrich.yaml)
- [profiles/demo_extract.yaml](profiles/demo_extract.yaml)
- [profiles/demo_full_process.yaml](profiles/demo_full_process.yaml)
- [profiles/demo_split.yaml](profiles/demo_split.yaml)

Typical operator flow:

1. Inspect headers from the source files.
2. Apply canonical mappings.
3. When a file spreads address data across multiple families, set fallback mappings for the same canonical field. The engine will use the first non-empty mapped source.
4. Use grouped header families in the Match tab to spot email, phone, identity, address, date, and money fields quickly.
5. Optionally normalize messy addresses or source fields first.
6. Validate the job spec.
7. Run the workflow.
8. Review the generated outputs and `run_summary.json`.

The shipped tests now cover both matching and utility demos, including:

- custom and preset matching
- broader custom stage sequences
- normalization-profile-driven imports
- full-process preset execution
- reference enrichment
- projection/extract jobs
- alternating split jobs
- address normalization and mapping regressions

## What The Shipped Demos Teach

Use the synthetic demo pack to learn the system by capability instead of by implementation file. The shipped match demos now come in a clear primary/reference pair and use roughly 1,000-row synthetic fixtures so the outputs feel more realistic, including a deliberate review bucket for ambiguous cases.

- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml)
  Teaches the simplest preset-style compare and match flow.
- [jobs/demo_custom_match_job.yaml](jobs/demo_custom_match_job.yaml)
  Teaches how the same match logic can run through a `custom_job` stage sequence.
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml)
  Teaches canonical mapping from non-standard source headers.
- [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml)
  Teaches how a normalization profile can reshape an awkward import before matching.
- [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml)
  Teaches a broader custom stage plan with date normalization, address normalization, dedupe, matching, address classification, contact aggregation, scoring, and multiple outputs.
- [profiles/demo_enrich.yaml](profiles/demo_enrich.yaml)
  Teaches exact-key reference enrichment.
- [profiles/demo_extract.yaml](profiles/demo_extract.yaml)
  Teaches projection and simple output formatting.
- [profiles/demo_split.yaml](profiles/demo_split.yaml)
  Teaches one-time utility splitting.
- [profiles/demo_full_process.yaml](profiles/demo_full_process.yaml)
  Teaches the heavier preset path that chains normalization, dedupe, reconcile, address status, contact aggregation, and scoring.

Together these demos cover `Prep`, `Match`, `Utilities`, normalization profiles, presets, and custom stage sequences.

## Primary Vs Reference

For compare and match workflows:

- `primary` is the incoming working file you want to evaluate
- `reference` is the existing system-of-record file you trust as the comparison baseline

In other words, if you are checking whether a normalized upload file already exists in a system of record:

- the normalized upload file should be `primary`
- the system export should be `reference`

The output buckets should be read like this:

- `matched_records`: rows in the new working file that already appear to exist in the reference export
- `review_records`: rows that need manual verification
- `new_records`: rows in the new working file that do not appear to exist in the reference export

## Setup

From the repo root:

```bash
cd <repo-root>
./setup_venv.sh
./run_local.sh list
```

Why this is preferred:

- the repo already has environment-aware launchers
- bare `python3` on this machine may not have the required packages

There is a launcher that prefers `.venv`, then `python3`:

```bash
./setup_venv.sh
./run_local.sh list
```

If you need to call Python directly, prefer:

```bash
./.venv/bin/python pipeline_runner.py list
```

## Tests

The repo now has a first unit-test layer under [tests](tests).

Current coverage includes:

- address splitting behavior
- mapping suggestion regressions
- header-family classification regressions
- `custom_job` validation around `stage_sequence`

Run the suite with the repo environment:

```bash
./.venv/bin/python -m unittest discover -s tests -v
```

Do not rely on bare system Python for the suite unless your global environment already has the required dependencies installed.

For the full day-to-day usage guide, see [OPERATOR_MANUAL.md](OPERATOR_MANUAL.md).

## Useful Files

- [DOCS_INDEX.md](DOCS_INDEX.md): documentation map
- [API.md](API.md): local web API reference
- [GETTING_STARTED.md](GETTING_STARTED.md): shortest operator path through the app
- [OPERATOR_MANUAL.md](OPERATOR_MANUAL.md): detailed day-to-day usage manual
- [TECHNICAL_DESIGN.md](TECHNICAL_DESIGN.md): deeper architecture notes
- [CUSTOM_MATCHING_GUIDE.md](CUSTOM_MATCHING_GUIDE.md): practical guide for custom matching jobs
- [CSV_HEADERS.md](CSV_HEADERS.md): canonical field reference
- [normalization_profiles](normalization_profiles): reusable normalization definitions
- [demo_data/README.md](demo_data/README.md): shipped synthetic demo fixtures

## Rule

This public repo ships only synthetic demo CSVs. Treat generated outputs as disposable runtime artifacts, not source data.
