# Stage Contract

## What this document is

This document explains the stage model used by the workflow engine.

It is a technical reference for how reusable stages are described and executed in the public version of the project.

Use it if you want to understand:

- how presets and custom jobs share the same execution model
- what a stage expects
- what a stage changes
- how datasets move through the engine

## Core idea

A workflow is built from reusable stages.

That applies to:

- built-in preset workflows
- custom stage-sequence jobs
- CLI execution
- local web UI execution

The important rule is simple:

- presets may provide defaults and convenience
- custom jobs use the same underlying stage system

In other words, presets are not supposed to have secret powers that custom jobs cannot access.

## Shared run context

Stages operate on a shared run context.

Conceptually, that context looks like this:

```python
{
  "workflow": "match_records_to_reference",
  "inputs": {
    "primary": pd.DataFrame(...),
    "reference": pd.DataFrame(...),
    "source": pd.DataFrame(...),
  },
  "artifacts": {},
  "stats": {},
  "warnings": [],
  "outputs": {},
}
```

Dataset roles are referred to by logical names such as:

- `primary`
- `reference`
- `source`

The other context buckets are used like this:

- `artifacts`
  - intermediate derived data
- `stats`
  - row counts and stage metrics
- `warnings`
  - non-fatal issues
- `outputs`
  - output file paths and output references

## What a stage exposes

Each stage has metadata plus runtime behavior.

At a minimum, a stage should expose:

- `name`
- `label`
- `required_inputs`
- `summary`
- `operator_goal`
- `inputs_detail`
- `effects`
- `watch_for`
- runtime configuration expectations

Conceptually, the runtime shape is similar to:

```python
class Stage:
    name: str
    label: str
    required_inputs: list[str]

    def run(self, context: dict, config: dict) -> dict:
        ...
```

In the actual project, stage metadata is also used by the web UI so the operator can see what each stage does before adding it to a custom plan.

## Important stage metadata

### `required_inputs`

The dataset roles the stage expects to exist.

Examples:

- `normalize_addresses`
  - one dataset role such as `primary` or `reference`
- `match_records`
  - both `primary` and `reference`
- `split_rows`
  - `source`

### `summary`

Short description of what the stage does.

This is used in the web UI and in stage-plan displays.

### `operator_goal`

Plain-language explanation of when to use the stage.

This helps the stage picker make sense to a human operator instead of reading like raw engine internals.

### `inputs_detail`

Guidance about what kind of fields or preparation the stage expects.

### `effects`

What the stage changes, creates, or guarantees.

Examples:

- a normalization stage may create `_address_norm`
- a scoring stage may create `priority_score`
- a write stage may create CSV outputs on disk

### `watch_for`

Warnings or failure modes worth knowing about before using the stage.

## Common stage families

## Input and preprocessing stages

These stages make files usable by later workflows.

Typical examples:

- `normalize_addresses`
- `normalize_date_columns`
- mapping and normalization-profile application through runtime setup

Use these when the source data is still messy.

## Comparison and enrichment stages

These stages compare datasets or copy trusted fields from one dataset into another.

Typical examples:

- `match_records`
- `flag_reference_identity`
- `join_reference_fields`

Use these when one dataset needs to be compared against or enriched from another.

## Classification and scoring stages

These stages add downstream workflow signals.

Typical examples:

- `classify_address_status`
- `score_priority`
- `aggregate_contacts`

Use these when you want more structured output after normalization or matching.

## Output stages

These stages shape or write final files.

Typical examples:

- `project_records`
- `write_records_bundle`
- `split_rows`
- `extract_projection`

Use these when you need final exports rather than more transformation.

## Current reusable stage coverage

The public project already exposes reusable capabilities for:

- address normalization
- date normalization
- scored matching
- identity flagging
- enrichment from a reference dataset
- contact aggregation
- address-status classification
- priority scoring
- projection/extract workflows
- split workflows
- standard matched/review/unmatched output bundles

This is why the same engine can support both presets and custom jobs.

## Presets vs custom jobs

A preset is a prepared configuration over the same core stage system.

A preset usually provides:

- a workflow label
- expected dataset roles
- a recommended stage sequence
- default output behavior
- a clearer operator-facing description

A custom job removes that wrapper and lets the user assemble the stage sequence directly.

That is the relationship:

- preset = pre-arranged version of the engine
- custom job = direct use of the engine

## Output behavior

Output behavior is not limited to one workflow shape.

The engine can support patterns like:

- matched / review / unmatched bundles
- one processed output file
- one extracted projection file
- two split outputs

That is why the UI can support both Match workflows and Utilities without using completely different runtime systems.

## Validation

Before a stage or workflow runs, the system validates things like:

- required dataset roles exist
- mapped source columns exist
- stage sequences are coherent
- output settings are present where required

This same validation model is used by:

- the CLI
- the web UI
- preset workflows
- custom jobs

## Why this matters

The stage system is what makes the project reusable instead of being just a pile of one-off scripts.

It is the piece that allows:

- shipped presets for common tasks
- custom jobs for unusual tasks
- a local UI that can expose stages directly
- a public demo pack that still reflects the real engine
