# Getting Started

## What this project does

This project helps you:

- normalize messy CSV exports into canonical fields
- compare new records against an existing database
- score likely matches instead of relying on brittle exact checks
- export matched, review, and unmatched files
- run everything from either a CLI or a local web app

## Quick start

From the repo root:

```bash
./setup_venv.sh
./run_local.sh list
./run_webapp.sh
```

Use `./run_local.sh` instead of bare `python3`. The launcher prefers the repo environment, which is required on this machine.

Then open:

```text
http://127.0.0.1:8765
```

## Fastest operator workflow

1. Start the web app with `./run_webapp.sh`.
2. Open the `Prep` tab if your source CSV needs cleanup first.
3. Enter the source CSV path, choose a normalization profile, and run normalization.
4. Download the normalized CSV from the page, or use the emitted output path directly.
5. Switch to the `Match` tab for reconciliation workflows such as `match_records_to_reference`, or use `Utilities` for one-off jobs like split or projection.
6. Enter the absolute file paths for your primary and reference CSVs.
7. Click `Inspect` for each file.
8. Use the grouped header cards to identify likely identity, email, phone, address, date, and money fields.
9. Click the header chips to push likely source fields into canonical mapping slots.
10. Apply or adjust suggested mappings.
11. Add fallback mappings when one export spreads addresses across several column families.
12. Set the output directory.
13. Click `Validate`.
14. Click `Run`.
15. Wait for the background job to complete. The web UI now polls job status instead of holding one long blocking request open.
16. Review the counts, outputs, match reasons, and preview tables.

The current mapper also understands several older header families directly, including full-field names like `MailingAddress` and `ResidentialAddress`, plus typo variants such as `MailMunisipality` and `ResMunisipality`.

## CLI fallback

If you prefer the terminal:

```bash
./run_local.sh describe match_records_to_reference
./run_local.sh headers /absolute/path/to/file.csv
./run_local.sh suggest-mapping /absolute/path/to/file.csv
./run_local.sh validate-job jobs/demo_match_job.yaml
./run_local.sh run-job jobs/demo_match_job.yaml
```

## Run tests

Use the repo environment:

```bash
./.venv/bin/python -m unittest discover -s tests -v
```

The current tests cover core address-splitting and mapping-suggestion behavior.

## UI sections

- `Prep`: normalize one file and save a reusable cleaned CSV
- `Match`: compare, match, and `custom_job` workflows
- `Utilities`: split, projection, and enrichment-style one-off jobs

## Safe demo run

If you want a public-safe walkthrough without touching operational data:

```bash
./run_local.sh validate-job jobs/demo_match_job.yaml
./run_local.sh run-job jobs/demo_match_job.yaml
./run_local.sh validate-job jobs/demo_random_custom_job.yaml
./run_local.sh run-job jobs/demo_random_custom_job.yaml
./run_local.sh validate-job jobs/demo_profiled_custom_job.yaml
./run_local.sh run-job jobs/demo_profiled_custom_job.yaml
./run_local.sh validate-job jobs/demo_full_custom_job.yaml
./run_local.sh run-job jobs/demo_full_custom_job.yaml
./run_local.sh validate-job profiles/demo_enrich.yaml
./run_local.sh run profiles/demo_enrich.yaml
./run_local.sh validate-job profiles/demo_extract.yaml
./run_local.sh run profiles/demo_extract.yaml
./run_local.sh validate-job profiles/demo_full_process.yaml
./run_local.sh run profiles/demo_full_process.yaml
./run_local.sh run profiles/demo_split.yaml
```

## Important files

- [DOCS_INDEX.md](DOCS_INDEX.md): documentation map
- [README.md](README.md): broader project overview
- [OPERATOR_MANUAL.md](OPERATOR_MANUAL.md): detailed operator guide
- [API.md](API.md): local web API reference
- [demo_data](demo_data): shipped synthetic CSV fixtures for demos and tests
- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml): safe shipped match demo using the paired `demo_match_primary.csv` and `demo_match_reference.csv` files
- [jobs/demo_custom_match_job.yaml](jobs/demo_custom_match_job.yaml): safe shipped custom-job demo
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml): safe shipped custom mapping demo for non-canonical headers
- [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml): safe shipped normalization-profile plus custom mapping demo
- [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml): safe shipped broader custom workflow-builder demo
- [profiles/demo_enrich.yaml](profiles/demo_enrich.yaml): safe shipped reference enrichment demo
- [profiles/demo_extract.yaml](profiles/demo_extract.yaml): safe shipped projection/extract demo
- [profiles/demo_full_process.yaml](profiles/demo_full_process.yaml): safe shipped full-process preset demo
- [profiles/demo_split.yaml](profiles/demo_split.yaml): safe shipped alternating split demo
- [CUSTOM_MATCHING_GUIDE.md](CUSTOM_MATCHING_GUIDE.md): practical guide for custom matching jobs
- [normalization_profiles](normalization_profiles): reusable normalization definitions
- [presets](presets): saved UI presets
- [webapp](webapp): local browser UI

## Common use case: find truly new records

Use `match_records_to_reference` when you want to compare a new incoming file against your existing database.

For shipped utility walkthroughs:

- `enrich_records_from_reference`: [profiles/demo_enrich.yaml](profiles/demo_enrich.yaml)
- `extract_projection`: [profiles/demo_extract.yaml](profiles/demo_extract.yaml)
- `split_alternating_rows`: [profiles/demo_split.yaml](profiles/demo_split.yaml)

For deeper architecture walkthroughs:

- normalization profile plus matching: [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml)
- broader custom workflow-builder demo: [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml)
- full-process preset: [profiles/demo_full_process.yaml](profiles/demo_full_process.yaml)

Role reminder:

- `primary` = the new working file you are checking
- `reference` = the existing source-of-truth export you are checking against

The pipeline will split results into:

- confident matches
- review matches
- unmatched records

That means you do not need to manually guess whether a record is new from exact ID-only checks.

## When to use normalization profiles

Use a normalization profile when the source file has issues like:

- address spread across multiple columns
- unit/suite values scattered across several possible fields
- non-standard header names
- partial fields that need to be joined or coalesced before mapping
- separate imports that need to be normalized into a clean intermediate CSV before matching

Example:

- [normalization_profiles/split_address_3col.yaml](normalization_profiles/split_address_3col.yaml)

## Saving presets

The web app lets you save presets for:

- workflow choice
- file paths
- canonical mappings
- normalization profile selection
- stage toggles
- output directory

This is useful when you regularly process the same source formats.

## Custom jobs

If the preset workflows are too specific, switch the workflow to `custom_job`.

The Match tab now includes a visual workflow builder for that mode. It currently supports:

- grouped stage cards
- live builder templates for common workflow shapes
- a zoomable canvas with arrows between cards
- expanding only the cards you want to edit
- basic per-stage role settings for common stages

This is a practical local workflow builder, not a full freeform node engine.

## Custom normalization profiles

The `Normalize` tab also lets you create and save custom normalization profiles.

Use this when you need to:

- join several source columns into one canonical field
- coalesce several possible source columns into one output field
- create repeatable preprocessing for recurring weird exports

## Notes

- The web app is local-only.
- `HEAD /` now works for simple health checks.
- Web runs are now started asynchronously and polled through the local API.
- Output runs write a `run_summary.json` into the output folder.
- The review and unmatched previews in the web app show only a small slice of rows for convenience.
- If the web UI changes do not appear, restart `./run_webapp.sh` so the newest local frontend is loaded.

## Archived local-only material

This public copy focuses on the synthetic demos, reusable workflow engine, tests, and docs.
