# Operator Manual

## What this is

This is the detailed practical guide for using CSV Record Pipeline as an operator.

If you want the shortest possible setup path, start with [GETTING_STARTED.md](GETTING_STARTED.md).

Use this manual when you want to understand:

- which tab to use
- what `primary`, `reference`, and `source` mean
- how mappings and fallbacks actually work
- when to normalize first
- what each workflow is for
- what the outputs mean
- what to check when something looks wrong

## The simple mental model

This tool helps you do three kinds of work:

- clean one messy file
- compare one file against another
- run one-off utility jobs on a file or pair of files

That is why the UI is split into:

- `Prep`
- `Match`
- `Utilities`

If you are not sure where to start, use this rule:

- one messy file that needs cleanup first -> `Prep`
- one file against another file -> `Match`
- one-off operational job like split, enrich, or extract -> `Utilities`

## File roles

### `primary`

`primary` is the working file you want to classify, clean up, or export.

Common examples:

- a new import
- a cleaned intermediate file
- a prior unmatched file
- a member or donor file you want to process

### `reference`

`reference` is the comparison or lookup file you trust more.

Common examples:

- a master export
- a system-of-record export
- a lookup file used for enrichment
- a second dataset you want to compare against

### `source`

Some utility workflows only use one file.

That single file is usually called `source`.

Common examples:

- split one file into two files
- extract selected columns from one file

## Canonical fields

The tool does not require every CSV to start with the same headers.

Instead, you map source columns into a smaller set of canonical fields such as:

- `person_id`
- `first_name`
- `middle_name`
- `last_name`
- `primary_address1`
- `primary_address2`
- `mail_city`
- `mail_state`
- `mail_zip`
- `email`
- `phone`

Once the mapping is done, the engine works on those canonical fields.

For the full public-safe reference, see [CSV_HEADERS.md](CSV_HEADERS.md).

## The three tabs

## Prep

Use `Prep` when one file needs cleanup before you do anything else.

Use it for:

- header mapping into canonical fields
- address cleanup
- unit splitting
- normalization profiles
- generating a reusable cleaned file in `input/normalized`

Good reasons to use Prep first:

- the file is ugly enough that matching directly would be confusing
- addresses are inconsistent or obviously broken
- you want a reusable cleaned intermediate CSV
- you want to inspect the transformed result before comparing it to anything

Good reasons to skip Prep:

- the file already maps cleanly
- you only need a simple one-off match
- the match workflow can handle the necessary normalization inline

### Prep workflow

Typical Prep flow:

1. pick the source file
2. inspect it
3. review the suggested mappings
4. add or adjust fallbacks if needed
5. choose a normalization profile if the file is a recurring ugly format
6. choose the output file name
7. run normalization
8. inspect or download the cleaned file

### Normalization profiles

Normalization profiles are saved cleanup recipes for repeating file shapes.

They are useful when you regularly receive a file that needs the same reshaping every time.

Profiles can do things like:

- copy one source field into one canonical field
- join several source columns into one output field
- coalesce several possible source columns into one output field

Example:

- [normalization_profiles/split_address_3col.yaml](normalization_profiles/split_address_3col.yaml)

## Match

Use `Match` when you want to compare one file against another and classify the rows.

This tab is for:

- compare-style workflows
- match-style workflows
- custom workflow-builder matching jobs

Typical Match questions:

- does this incoming file already exist in the trusted master file?
- which rows are clearly matched, which need review, and which look new?
- can I run a custom workflow instead of a preset?

### Match workflow

Typical Match flow:

1. choose the workflow
2. set the file paths for `primary` and `reference`
3. inspect both files
4. review the grouped headers and suggested mappings
5. fix any mappings or add fallbacks
6. validate
7. run
8. inspect the summary and output files

### Match buckets

You will usually see some combination of these outputs:

- `matched_records`
  - strong confident matches
- `review_records`
  - not safe to auto-classify, worth human review
- `unmatched_records` or `new_records`
  - rows the matcher did not find strong evidence for

### How to think about thresholds

The UI gives you quick presets and a `More Settings` section.

Use the presets unless you have a clear reason not to.

General rule:

- more conservative -> fewer false positives, more review/unmatched rows
- more aggressive -> more auto-matches, higher risk of bad matches

If you do not understand the thresholds yet, leave them on the default preset.

### What grouped headers are for

After inspection, the UI groups headers into broad families such as:

- identity
- email
- phone
- address
- dates
- money

Those groups are there to help you find useful columns faster.

They are hints, not truth.

### What header chips are for

The clickable header chips are shortcuts.

Use them when:

- the file uses weird header names
- several possible columns could feed the same canonical field
- you want to add mappings and fallbacks quickly

Still sanity-check the mapping after clicking.

### Fallback mappings

Fallbacks exist because messy files often store the same kind of data in several competing columns.

Example:

- `primary_address1 <- StreetLine`
- fallback 1 <- `MailingAddress`
- fallback 2 <- `Address1`

The runtime uses the first non-empty mapped source.

Use fallbacks when:

- the file mixes address families
- different rows populate different source columns
- a reference export is inconsistent

Do not add random fallback columns just because they exist. Too many bad fallback choices make debugging harder and matching weaker.

### Custom jobs

Use `custom_job` when the presets are close but not enough.

A custom job lets you build your own stage plan.

In the current UI, that means:

- start from a live template
- use grouped stage cards as a picker
- drop stages onto the workflow canvas
- expand only the cards you need to edit
- keep the default canvas compact and visual

Common match-oriented stage order:

1. `normalize_addresses` on `primary`
2. `normalize_addresses` on `reference`
3. `match_records`
4. `write_records_bundle`

You can expand from there with stages like:

- `dedupe_records`
- `flag_reference_identity`
- `aggregate_contacts`
- `classify_address_status`
- `score_priority`

For more on that, see [CUSTOM_MATCHING_GUIDE.md](CUSTOM_MATCHING_GUIDE.md).

## Utilities

Use `Utilities` for one-off jobs that are useful, but not really "matching workflows."

This tab is for jobs like:

- enrichment
- extraction
- split jobs

### Enrich from reference

Use this when you already know how records should join and you simply want to copy selected fields from a trusted reference file onto a working file.

This is not fuzzy matching. It is a join-and-copy job.

Use it when:

- both files share a stable key
- you want to fill missing fields or add trusted lookup data
- you do not need match scoring buckets

### Extract projection

Use this when you want a smaller export with selected columns and simple filtering or formatting logic.

Use it when:

- the source file is too wide
- you need a smaller operational export
- you want only a few fields kept in the output

### Split alternating rows

Use this when you want to split one file into two files by alternating row order.

Use it when:

- two operators need evenly divided workloads
- you want two simple split outputs without changing the data itself

This workflow preserves the columns exactly. It does not require canonical mapping.

## The workflow description pane

When you pick a workflow, the UI shows a detailed description panel.

Read that panel before you run anything if you are unsure.

It tells you:

- what the workflow is for
- which file roles it expects
- what it writes
- common warnings

The same idea also exists for cards inside the workflow builder.

## Run results

After a run, the UI shows:

- summary counts
- output files
- match reason summaries for matching workflows
- preview rows for match workflows

The machine-readable `run_summary.json` is still written to disk, but it is intentionally deemphasized in the UI.

## Where files go

Common output locations:

- cleaned Prep outputs -> `input/normalized`
- standard workflow outputs -> `output/`
- custom web-run outputs -> usually under `output/` or `output/webapp_runs`

The Recent Outputs section helps you reuse or download results.

## How to troubleshoot

## Problem: nothing matched when I expected matches

Check:

- did you mix up `primary` and `reference`?
- are the name fields mapped correctly?
- are addresses mapped to the right family?
- did you forget IDs, email, or phone that could help matching?
- should you normalize first?

## Problem: too many review rows

Usually this means:

- mappings are incomplete
- address families are inconsistent
- thresholds are too conservative for the use case
- the source file needs cleanup first

## Problem: Utilities ran but I do not see match-style previews

That is normal.

The row-preview panel is only meant for Match workflows.

Utilities focus on output files and summary counts instead.

## Problem: normalization created the wrong fields

Check:

- whether the source headers were mapped correctly
- whether a fallback column was beating the intended column
- whether a normalization profile is still too specific for the file you loaded

## Problem: the output filename changed

That usually means the target name already existed, so the app created a non-conflicting filename instead of overwriting an existing file.

## Safe shipped demos

The repo includes synthetic public-safe demo files and demo jobs.

Use those if you want to learn the system without touching your own operational data.

Good starting points:

- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml)
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml)
- [profiles/demo_extract.yaml](profiles/demo_extract.yaml)
- [profiles/demo_split.yaml](profiles/demo_split.yaml)

The web UI also preloads shipped demo files by section so you can try the main flows quickly.

## CLI equivalents

If you prefer the terminal, the main entrypoints are:

```bash
./run_local.sh validate-job jobs/demo_match_job.yaml
./run_local.sh run-job jobs/demo_match_job.yaml
./run_local.sh run profiles/demo_extract.yaml
./run_local.sh run profiles/demo_split.yaml
```

## Final advice

Use the simplest path that fits the job.

- one messy file -> `Prep`
- one file compared against another -> `Match`
- one-off operational transform -> `Utilities`

If you are fighting the tool, it is usually a sign that:

- the file roles are wrong
- the mappings are wrong
- or you are using the wrong tab for the job
