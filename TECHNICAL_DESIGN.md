# Technical Design

## Purpose

This repository is a local CSV workflow application built around pandas dataframes, YAML job definitions, and a shared stage-based execution path.

Its current responsibilities are to:

- load CSV inputs
- map source headers into canonical fields
- normalize and reconcile records
- enrich rows from trusted reference data
- score or classify outputs
- write CSV outputs plus run metadata

The public repo includes both shipped presets and user-defined `custom_job` support through the same underlying engine, with a visual workflow builder on top of the stage system.

## System model

The system has four layers.

1. Interface layer
   - CLI in [pipeline_runner.py](pipeline_runner.py)
   - shell wrapper in [run_local.sh](run_local.sh)
   - local browser UI in [webapp/server.py](webapp/server.py) and [webapp/static](webapp/static)
2. Orchestration layer
   - runtime execution in [pipeline_runner.py](pipeline_runner.py)
   - preset plan builders in [core/preset_plans.py](core/preset_plans.py)
   - shared dataset loading in [core/runtime_loader.py](core/runtime_loader.py)
   - shared run reporting in [core/runtime_reporting.py](core/runtime_reporting.py)
3. Domain stage layer
   - stage registry in [core/stages.py](core/stages.py)
   - normalization in [normalize](normalize)
   - reconciliation in [reconcile](reconcile)
   - enrichment in [enrich](enrich)
   - classification in [classify](classify)
   - scoring in [score](score)
   - export in [export](export)
4. Data and config layer
   - CSV I/O in [dataio/csv.py](dataio/csv.py)
   - YAML jobs and profiles in [jobs](jobs) and [profiles](profiles)
   - schema reference in [CSV_HEADERS.md](CSV_HEADERS.md)
   - synthetic public demos in [demo_data](demo_data)

## Runtime flow

The normal runtime path is:

1. The user starts a preset workflow or a `custom_job`.
2. A runtime config is built from the YAML definition or web payload.
3. Shared loader helpers load one or more CSVs as dataframes and apply canonical mapping.
4. The stage runner executes the preset plan or custom `stage_sequence`.
5. Shared reporting helpers write CSV outputs plus `run_summary.json`.

The core execution model is synchronous and in-process.

The web app wraps that with an in-memory background job registry so the browser can:

- start a run with `POST /api/run-job-async`
- poll status with `GET /api/job-status?id=...`
- render results after completion without holding one long blocking request open

This is appropriate for a local operator tool. It is not a durable job system.

## Core design choices

### Dataframe-centric execution

Stages receive and return pandas dataframes.

Benefits:

- low implementation overhead
- easy CSV interoperability
- straightforward stage composition

Costs:

- schema discipline is mostly conventional
- validation has to happen before runtime, not through strict types
- row-wise logic can become expensive on larger files

### Canonical field strategy

The most important design choice in the repo is the canonical field layer.

Different source files are mapped into shared names such as:

- `person_id`
- `first_name`
- `last_name`
- `primary_address1`
- `primary_address2`
- `mail_city`
- `mail_state`
- `mail_zip`

This is what allows one engine to work across different imports, references, utilities, and demos.

The canonical layer is flexible enough to support:

- one source column per canonical field
- ordered fallback source columns
- ugly non-canonical imports through custom mapping
- recurring cleanup through normalization profiles

### Shared engine, different operator surfaces

The same engine now powers:

- preset workflows
- `custom_job`
- CLI execution
- web UI execution

The UI exposes this through three operator-oriented sections:

- `Prep`
- `Match`
- `Utilities`

That keeps the product understandable without changing the underlying runtime model.

## Module responsibilities

### Orchestration

[pipeline_runner.py](pipeline_runner.py) is still the main entrypoint, but much of the reusable work has already moved into shared runtime modules.

Important orchestration pieces:

- [core/runtime_loader.py](core/runtime_loader.py)
- [core/preset_plans.py](core/preset_plans.py)
- [core/runtime_reporting.py](core/runtime_reporting.py)
- [core/stages.py](core/stages.py)

### I/O

[dataio/csv.py](dataio/csv.py) is the CSV boundary.

It is intentionally conservative:

- load as strings
- avoid aggressive type coercion
- create output directories automatically

That is the right tradeoff for messy operator-driven CSV work.

### Normalization

The normalization layer is deterministic preprocessing.

Examples:

- [normalize/address.py](normalize/address.py)
  - builds `_address_norm`
- [normalize/address_split.py](normalize/address_split.py)
  - splits unit and street when possible
- [normalize/dates.py](normalize/dates.py)
  - normalizes date-like fields
- [core/normalization_profiles.py](core/normalization_profiles.py)
  - applies reusable normalization profiles

### Reconciliation

The reconciliation layer handles two related but different ideas:

- identity or existence checks
- scored matching and bucket classification

This is why the system can support both compare-style workflows and explicit match workflows.

### Enrichment

The enrichment layer appends trusted reference-side data without replacing row identity.

This powers workflows like:

- enrichment utilities
- contact aggregation
- post-match data shaping

### Classification and scoring

These stages turn operational signals into more usable fields.

Examples:

- `address_status`
- `priority_score`
- `priority_band`

### Export and output shaping

The export layer handles:

- matched/review/unmatched bundles
- extracted projection files
- split outputs
- processed-record outputs

This is one of the reasons the same runtime can support both Match and Utilities.

## Public demo design

The public repo ships synthetic demo data on purpose.

That demo layer exists to show:

- canonical matching
- non-canonical mapping
- normalization-profile cleanup
- enrich workflows
- extract workflows
- split workflows
- broader custom workflow-builder jobs
- full-process execution

Those demos are backed by tests so the public repo is not just documentation and screenshots.

## Current strengths

### Reusable execution model

The project now has a real shared runtime model rather than only one-off script entrypoints.

That shared model covers:

- runtime loading
- stage execution
- preset plan builders
- reporting
- async web execution wrappers

### Good operator model

The web UI is structured around operator intent instead of raw implementation detail.

That makes it easier to use without hiding the underlying engine.

### Strong public-safe demo story

The public repo includes:

- synthetic demo CSVs
- runnable demo jobs and profiles
- test coverage for core demo flows
- docs that explain how the main pieces fit together

## Current tradeoffs and limits

### Local-first runtime model

The app is intentionally local-first.

That means:

- no authentication layer
- no durable multi-user job queue
- no persistent run history beyond output files and `run_summary.json`
- no server-grade security model

This is acceptable for a local operator tool, but it is not a production web service design.

### Validation is good, not perfect

The project validates runtime configuration and mapped columns before running, but it still relies on conventions more than a fully typed schema system.

### Some preset logic still exists

Presets are now much thinner than before, but they still act as convenience adapters over the stage system.

That is acceptable in the public project as long as presets remain wrappers over shared capabilities.

## Architectural rule

The main architectural rule is:

- presets should not rely on hidden powers that `custom_job` does not have

If a preset can normalize, match, enrich, split, score, or export in a certain way, that capability should exist in the reusable engine.

## Testing implications

The current structure supports testing at several levels:

- unit tests for stage helpers and mapping logic
- validation tests for runtime config behavior
- integration-style tests for synthetic demo jobs and profiles

That is a good fit for a public local-tool repo because it proves the engine behavior without exposing private operational data.
