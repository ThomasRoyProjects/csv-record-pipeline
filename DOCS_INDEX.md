# Documentation Index

Use this file as the map for the public repo.

## Start Here

- [README.md](README.md): product-level overview, setup, shipped demos, and architecture summary
- [GETTING_STARTED.md](GETTING_STARTED.md): fastest path through the web app and demo jobs
- [OPERATOR_MANUAL.md](OPERATOR_MANUAL.md): detailed practical guide for day-to-day use
- [API.md](API.md): local HTTP API reference for the web app backend

## Technical Reference

- [TECHNICAL_DESIGN.md](TECHNICAL_DESIGN.md): current system shape and runtime architecture
- [STAGE_CONTRACT.md](STAGE_CONTRACT.md): reusable stage model behind presets and custom jobs
- [CUSTOM_MATCHING_GUIDE.md](CUSTOM_MATCHING_GUIDE.md): practical guide for building custom matching jobs
- [CSV_HEADERS.md](CSV_HEADERS.md): canonical fields, derived columns, and schema reference

## Demos And Examples

- [demo_data/README.md](demo_data/README.md): shipped synthetic demo fixtures and expected outcomes
- [demo_data](demo_data): safe demo CSVs for walkthroughs, screenshots, and tests
- [jobs/demo_match_job.yaml](jobs/demo_match_job.yaml): shipped safe preset-style match demo
- [jobs/demo_custom_match_job.yaml](jobs/demo_custom_match_job.yaml): shipped safe custom-job match demo
- [jobs/demo_random_custom_job.yaml](jobs/demo_random_custom_job.yaml): shipped safe non-canonical mapping demo
- [jobs/demo_profiled_custom_job.yaml](jobs/demo_profiled_custom_job.yaml): shipped normalization-profile plus matching demo
- [jobs/demo_full_custom_job.yaml](jobs/demo_full_custom_job.yaml): shipped broader custom stage-plan demo
- [profiles/demo_enrich.yaml](profiles/demo_enrich.yaml): shipped enrichment utility demo
- [profiles/demo_extract.yaml](profiles/demo_extract.yaml): shipped projection utility demo
- [profiles/demo_full_process.yaml](profiles/demo_full_process.yaml): shipped full-process preset demo
- [profiles/demo_split.yaml](profiles/demo_split.yaml): shipped split utility demo

## Runtime And Config Folders

- [normalization_profiles](normalization_profiles): reusable normalization definitions
- [profiles](profiles): bundled preset workflow YAMLs
- [jobs](jobs): example runtime job specs, including custom stage-sequence jobs
- [tests](tests): shipped regression and demo integration coverage

## Repo Note

- This public repo ships only synthetic demo CSVs and documentation references. Do not treat any generated outputs as source data.
