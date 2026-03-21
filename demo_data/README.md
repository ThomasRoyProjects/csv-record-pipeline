# Demo Data

This folder contains larger synthetic CSV fixtures that are safe to ship with the repository.

Use them for:

- screenshots and walkthroughs
- local demos
- integration tests
- onboarding without touching real operational data

Files:

- `demo_match_primary.csv`: canonical-style incoming records for matching demos
- `demo_match_reference.csv`: paired reference-side records for compare and match demos
- `demo_primary_people.csv`: backward-compatible copy of the canonical incoming match file
- `demo_reference_people.csv`: backward-compatible copy of the canonical reference match file
- `demo_legacy_members.csv`: legacy-style export with older address header families
- `demo_random_import.csv`: intentionally non-canonical import with custom headers for mapping demos
- `demo_process_members.csv`: synthetic member-style file for full-process preset and broader custom-stage demos
- `demo_profiled_import.csv`: synthetic split-address import intended to be cleaned with a normalization profile before matching
- `demo_split_source.csv`: simple input for split demos

Expected outcomes:

- `jobs/demo_match_job.yaml`
  - 600 confident matches
  - 50 review records
  - 350 unmatched records
- `jobs/demo_custom_match_job.yaml`
  - 600 confident matches
  - 50 review records
  - 350 unmatched records
- `jobs/demo_random_custom_job.yaml`
  - 600 confident matches
  - 50 review records
  - 350 unmatched records
- `jobs/demo_profiled_custom_job.yaml`
  - 600 confident matches
  - 50 review records
  - 350 unmatched records
- `jobs/demo_full_custom_job.yaml`
  - 600 confident matches
  - 50 review records
  - 350 unmatched records
  - 1 duplicate removed
  - one processed records export
- `profiles/demo_enrich.yaml`
  - one enriched CSV with a `reference_segment` column
- `profiles/demo_extract.yaml`
  - a reduced donation-style CSV with only positive-amount rows
- `profiles/demo_full_process.yaml`
  - 1 duplicate removed
  - 1000 processed rows exported
- `profiles/demo_split.yaml`
  - 500 rows in output A
  - 500 rows in output B

These shipped demos are also exercised by the repo test suite in [tests/test_demo_jobs.py](tests/test_demo_jobs.py).

These files are intentionally synthetic, fake, and large enough to make the workflows feel real in screenshots and demos. They should never be confused with real campaign, voter, or donor data.
