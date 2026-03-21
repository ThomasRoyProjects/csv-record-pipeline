# CSV Headers Reference

Public-safe schema reference for the shipped demo data and the canonical field model used by the pipeline.

This file is intentionally header-only. It does not include row data.

## Rule

- Prefer building synthetic or redacted CSVs that only use the headers listed here.
- In the public repo, the shipped demo data is synthetic and safe to inspect or extend.
- New imports do not need to match these headers exactly if you use canonical mapping or a `custom_job`.

## Canonical Columns

These are the core target fields the pipeline tries to normalize source files into.

Identity and names:

- `person_id`
- `first_name`
- `middle_name`
- `last_name`
- `signup_external_id`

Address:

- `primary_address1`
- `primary_address2`
- `mail_city`
- `mail_state`
- `mail_zip`
- `primary_city`
- `primary_state`
- `primary_zip`

Dates and lifecycle:

- `created_at`
- `membership_end_date`
- `memb_start_date`
- `memb_exp_date`

Contact and value:

- `email`
- `email1`
- `email2`
- `email3`
- `email4`
- `phone`
- `home_phone`
- `work_phone`
- `mobile_phone`
- `amount`

## Common Derived Columns

These are frequently created by stages in the workflow engine.

- `_address_norm`
- `_address_split_status`
- `_exists_by_external_id`
- `_exists_by_name_address`
- `_exists_by_name_address1`
- `_matched`
- `_name_match`
- `_addr_match`
- `_matched_to_voter`
- `_address_matched`
- `address_status`
- `emails_all`
- `phones_all`
- `email_count`
- `phone_count`
- `priority_score`
- `priority_band`

## Shipped Demo CSV Types

### 1. Canonical match primary file

Used by:

- `jobs/demo_match_job.yaml`
- `jobs/demo_custom_match_job.yaml`
- `profiles/demo_enrich.yaml`

Representative headers:

- `person_id`
- `first_name`
- `middle_name`
- `last_name`
- `primary_address1`
- `primary_address2`
- `mail_city`
- `mail_state`
- `mail_zip`
- `email1`
- `phone`

### 2. Canonical match reference file

Used by:

- `jobs/demo_match_job.yaml`
- `jobs/demo_custom_match_job.yaml`
- `profiles/demo_enrich.yaml`
- `profiles/demo_full_process.yaml`

Representative headers:

- `userID`
- `first_name`
- `middle_name`
- `last_name`
- `primary_address1`
- `primary_address2`
- `primary_city`
- `primary_state`
- `primary_zip`
- `email`
- `phone`

Note:

- `userID` is a demo-side source header. It can be mapped to canonical `person_id`.

### 3. Random import with non-canonical headers

Used by:

- `jobs/demo_random_custom_job.yaml`

Representative headers:

- `ExternalID`
- `GivenName`
- `Middle`
- `Surname`
- `StreetLine`
- `UnitLine`
- `Town`
- `ProvinceCode`
- `Postal`
- `EmailAddress`
- `PhoneNumber`

Purpose:

- demonstrates source-to-canonical mapping in a custom job
- demonstrates that imports do not need to start in canonical form

### 4. Profiled import source

Used by:

- `jobs/demo_profiled_custom_job.yaml`
- Prep tab demo defaults in the web UI

Representative headers:

- `StreetNumber`
- `StreetName`
- `StreetType`
- `Unit`
- `City`
- `Province`
- `PostalCode`
- `FirstName`
- `LastName`

Purpose:

- demonstrates a normalization-profile-driven cleanup step before matching

### 5. Legacy-style member file

Used by:

- normalization demo in the web UI
- `profiles/demo_extract.yaml`

Representative headers:

- `MembershipType`
- `MailingAddress`
- `MailMunisipality`
- `MailProvince`
- `MailPostalCode`
- `ResidentialAddress`
- `ResMunisipality`
- `ResProvince`
- `ResPostalCode`
- `LastDonationDate`
- `LastDonationReceiptableAmount`

Purpose:

- demonstrates cleanup of legacy address schemas
- demonstrates projection/extract workflows

### 6. Full-process demo member file

Used by:

- `profiles/demo_full_process.yaml`
- `jobs/demo_full_custom_job.yaml`

Representative headers:

- `person_id`
- `first_name`
- `last_name`
- `primary_address1`
- `primary_address2`
- `mail_city`
- `mail_state`
- `mail_zip`
- `email1`
- `phone`

Purpose:

- demonstrates dedupe, scoring, and processed-record export

### 7. Split source file

Used by:

- `profiles/demo_split.yaml`

Requirements:

- any headers are accepted
- all columns are preserved exactly
- no canonical mapping is required

## Practical Guidance

- If your source file is already close to canonical, map only the fields you need.
- If your source file uses odd header names, use the Match tab or a `custom_job` to map them.
- If your source file needs address cleanup first, use Prep or a normalization profile.
- The demo data exists to show the main shapes the pipeline can handle, not to limit what the tool accepts.
