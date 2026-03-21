# Custom Matching Guide

## Purpose

Use this guide when the built-in presets are too specific and you need to build a one-off or reusable matching job yourself.

This guide is intentionally generic. It is not tied to any one platform, voter file, CRM export, or source-of-truth system.

Use it for cases like:

- comparing a new import against an existing master file
- reconciling two systems with overlapping people or records
- checking whether rows from one source already exist in another
- matching a prior `unmatched_records` file against a new reference dataset
- building a reusable stage-based custom job for repeated record reconciliation

## Core model

For custom matching jobs:

- `primary` = the working file you want to classify
- `reference` = the file you trust as the comparison baseline

Examples:

- checking whether incoming records already exist in a master export
  - `primary` = incoming file
  - `reference` = trusted master export

- checking whether a prior unmatched output can be found in a second system
  - `primary` = prior unmatched file
  - `reference` = second-system export

- reconciling one legacy database extract against a cleaned canonical file
  - `primary` = legacy extract
  - `reference` = cleaned or trusted comparison file

## Required ingredients

At minimum, matching works best when both datasets map:

- `first_name`
- `last_name`
- `primary_address1`

If a shared stable identifier exists, also map:

- `person_id`

Recommended additional fields:

- `middle_name`
- `primary_address2`
- `mail_city` or `primary_city`
- `mail_state` or `primary_state`
- `mail_zip` or `primary_zip`
- `email`
- `phone`

## Best practice for messy exports

Do not assume one source file stores addresses or contact fields in one clean family.

Use fallback mappings when a file spreads address data across variants such as:

- `mailing_*`
- `primary_*`
- `address_*`
- full-field legacy address columns

The runtime uses the first non-empty mapped source for each canonical field.

That means custom jobs can handle:

- canonical files
- semi-canonical files
- ugly non-canonical imports
- legacy address schemas

## Common stage order

For most custom matching jobs, start with this stage sequence:

1. `normalize_addresses` on `primary`
2. `normalize_addresses` on `reference`
3. `match_records`
4. `write_records_bundle`

That is the default match-oriented template exposed in the Match tab for `custom_job`.

You can build from there if you need more.

## Common optional stages

These are often useful before or after matching:

- `normalize_date_columns`
  - if date fields need cleanup before later stages

- `dedupe_records`
  - if the working file may contain duplicates before export or scoring

- `flag_reference_identity`
  - if you want explicit identity flags from the reference dataset

- `aggregate_contacts`
  - if you want combined contact rollups on the output side

- `classify_address_status`
  - if you want address status categories after normalization

- `score_priority`
  - if you want output ranking or triage support

## Match thresholds

Default thresholds:

- `confident`: `160`
- `possible`: `120`
- `review`: `85`

These are not huge abstract numbers. They are a few-hundred-point scoring system built from weighted evidence such as:

- exact ID
- exact or partial name agreement
- normalized address agreement
- postal agreement
- email agreement
- phone agreement

Use the defaults unless you have a strong reason to change them.

Do not lower them casually. Lower thresholds usually increase false positives faster than they reduce review work.

## IDs vs fuzzy evidence

Treat exact IDs as the strongest signal.

If a useful shared ID exists:

- map it on both sides
- keep it in the match config
- use it as part of your post-run sanity checks

If no shared ID exists, the matcher falls back to combinations of:

- name
- normalized address
- postal/zip
- email
- phone

## Recommended post-run checks

After a custom matching run, check:

1. `run_summary.json`
   - look at `by_status`
   - look at `by_reason`

2. review bucket size
   - if it is too large, you likely need better mappings or better address-family selection

3. unmatched ID overlap
   - if both files have a real shared ID, compare unmatched IDs against the reference-side mapped ID column
   - exact overlap should usually be zero or very close to zero

4. reason distribution
   - lots of `ADDRESS_UNIT_CONFLICT` usually means address parsing or family selection still needs work
   - lots of `ID_ONLY` means you may be missing supporting address/contact evidence

## Strong generic matching patterns

### Pattern 1: Incoming records vs trusted master file

- `primary` = incoming file
- `reference` = trusted master export
- use shared IDs if they exist
- use fallback address families on the reference side when needed

### Pattern 2: Prior unmatched file vs second reference system

- `primary` = unmatched output from a prior pass
- `reference` = second comparison dataset
- map IDs only if they are genuinely comparable
- otherwise rely on names plus normalized address evidence

### Pattern 3: Legacy export vs canonical dataset

- `primary` = legacy or messy source
- `reference` = canonical or previously cleaned dataset
- normalize and map the legacy side carefully before trusting match outcomes

## Shipped safe demos

If you want public-safe examples in this repo, use:

- [demo_data/demo_match_primary.csv](demo_data/demo_match_primary.csv)
- [demo_data/demo_match_reference.csv](demo_data/demo_match_reference.csv)
- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml)
- [jobs/demo_custom_match_job.yaml](jobs/demo_custom_match_job.yaml)
- [demo_data/demo_random_import.csv](demo_data/demo_random_import.csv)
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml)
- [demo_data/demo_profiled_import.csv](demo_data/demo_profiled_import.csv)
- [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml)
- [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml)

Use them like this:

- `demo_match_job`
  - preset-style matching walkthrough on the shipped paired synthetic files

- `demo_custom_match_job`
  - same core match logic as a custom stage plan

- `demo_random_custom_job`
  - non-canonical header mapping walkthrough

- `demo_profiled_custom_job`
  - normalization profile plus matching walkthrough

- `demo_full_custom_job`
  - broader custom stage sequence with dedupe, scoring, and multiple outputs

## Current UI support

The Match tab currently supports:

- grouped header-family inspection
- clickable header chips to populate mapping slots
- ordered fallback mappings
- a first-pass `custom_job` stage-plan editor
- richer workflow and stage detail panes
- asynchronous local runs with job-status polling

What it does not fully support yet:

- deep per-stage configuration for every stage
- advanced output shaping for all custom workflows
- derived-field building directly inside the custom-job flow

## Minimal custom matching checklist

Before run:

- files selected correctly
- `primary` and `reference` roles make sense
- name fields mapped
- address fields mapped
- fallback address families added where needed
- IDs mapped if available
- output directory set

After run:

- review `run_summary.json`
- inspect review bucket size
- inspect unmatched bucket logic
- run an exact-ID overlap check if both sides have a meaningful shared ID

If you are testing the same custom job through the browser, remember that the web UI starts the run asynchronously and polls for status before rendering the summary.
