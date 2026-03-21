"""Workflow identifiers and compatibility aliases."""

WORKFLOW_ALIASES = {
    "custom_job": "custom_job",
    "compare_records_to_reference": "compare_records_to_reference",
    "identify_missing_records_from_system": "identify_missing_records_from_system",
    "match_records_to_reference": "match_records_to_reference",
    "enrich_records_from_reference": "enrich_records_from_reference",
    "extract_projection": "extract_projection",
    "split_alternating_rows": "split_alternating_rows",
    "process_full_records": "process_full_records",
    # Legacy aliases kept for compatibility with older profile names.
    "hq_new_people_to_nb": "compare_records_to_reference",
    "hq_new_people_match_to_voters": "match_records_to_reference",
    "hq_new_people_match_to_voters_with_membership_end": "enrich_records_from_reference",
    "extract_last_donations": "extract_projection",
    "shuffle_split_sorted_csv": "split_alternating_rows",
    "full_members_to_nb": "process_full_records",
}


WORKFLOW_LABELS = {
    "custom_job": "Build a custom workflow",
    "compare_records_to_reference": "Find new records against a reference file",
    "identify_missing_records_from_system": "Find records missing from the current system",
    "match_records_to_reference": "Run full matching against a reference file",
    "enrich_records_from_reference": "Enrich primary rows from a reference file",
    "extract_projection": "Project a reduced export",
    "split_alternating_rows": "Split one file into alternating outputs",
    "process_full_records": "Run the full cleanup, match, enrich, and score pass",
}


WORKFLOW_CATEGORIES = {
    "custom_job": "match",
    "compare_records_to_reference": "match",
    "identify_missing_records_from_system": "match",
    "match_records_to_reference": "match",
    "enrich_records_from_reference": "utilities",
    "extract_projection": "utilities",
    "split_alternating_rows": "utilities",
    "process_full_records": "match",
}


WORKFLOW_DESCRIPTIONS = {
    "custom_job": {
        "summary": "Runs a user-defined sequence of engine stages over one or more datasets.",
        "operator_goal": "Build your own workflow from engine stages when the presets do not quite fit the job.",
        "does": [
            "Loads whichever named inputs the job defines.",
            "Executes the declared stage sequence in order.",
            "Writes any outputs requested by those stages.",
            "Produces a generic run summary when possible.",
        ],
        "inputs_detail": [
            "You choose the dataset roles. Most matching jobs will still use a primary file and a reference file.",
            "Each dataset can map one canonical field from multiple source headers by using primary and fallback columns.",
            "The stage plan decides which inputs are actually used.",
        ],
        "outputs_detail": [
            "Outputs depend on the stages you add.",
            "A matching-oriented custom job usually writes matched, review, and unmatched CSVs plus a run summary.",
            "A projection or enrichment job may write only one output file and a summary.",
        ],
        "step_by_step": [
            "Inspect the uploaded files and map source headers into canonical fields.",
            "Build or adjust the stage sequence.",
            "Validate the runtime config before running it.",
            "Run the job and inspect the generated outputs and summary.",
        ],
        "watch_for": [
            "If a preset can do the job cleanly, start there before reaching for custom_job.",
            "Custom jobs are only as good as the mapping and stage order you provide.",
            "If matching quality looks weak, normalization and fallback mappings usually matter more than lowering thresholds.",
        ],
        "best_for": "Use when the built-in presets are too specific and you want to compose your own workflow from the engine stages.",
    },
    "compare_records_to_reference": {
        "summary": "Compares an incoming file against a reference file and separates likely new rows from already-existing rows.",
        "operator_goal": "Answer the practical question: which rows in this incoming file look new enough to export, and which ones already appear to exist?",
        "does": [
            "Loads primary and reference CSVs.",
            "Applies any configured normalization profiles before matching.",
            "Optionally normalizes addresses and dates used for comparison.",
            "Runs the scored matcher to classify each primary row.",
            "Exports new, review, and matched record files plus a run summary.",
        ],
        "inputs_detail": [
            "Primary = the incoming working file you want to check.",
            "Reference = the source-of-truth or current-system export you trust when deciding whether a person already exists.",
            "IDs, names, address lines, postal codes, email, and phone all help when available.",
        ],
        "outputs_detail": [
            "new_records.csv: rows the matcher could not tie back to the reference strongly enough.",
            "review_records.csv: gray-area rows that need a human look before you call them new.",
            "matched_records.csv: rows that look already present in the reference file.",
            "run_summary.json: counts, reasons, output paths, and match-input notes.",
        ],
        "step_by_step": [
            "Load the primary file and the trusted reference file.",
            "Map canonical fields, using fallbacks where exports scatter addresses or emails across several header families.",
            "Normalize comparison fields used by the matcher.",
            "Score candidate matches and split primary rows into new, review, and matched buckets.",
            "Use the review bucket to catch the uncertain edge cases before exporting new rows.",
        ],
        "watch_for": [
            "This workflow is intentionally conservative. Weak matching can create false new rows.",
            "If address data is messy, use the Normalize tab first or add fallback mappings for mailing versus primary address families.",
            "Do not treat review rows as new rows without checking them.",
        ],
        "best_for": "Use when the main question is: which rows in this incoming file appear to be brand new?",
    },
    "identify_missing_records_from_system": {
        "summary": "Compares a headquarters or source-of-truth file against the current system and identifies which people are likely missing from the system today.",
        "operator_goal": "Run gap analysis between a source-of-truth list and the current system export.",
        "does": [
            "Loads the source file as primary and the current system export as reference.",
            "Applies normalization profiles and internal address normalization before comparison.",
            "Runs the scored matcher with unit-aware address handling.",
            "Separates rows into likely missing, needs review, and likely already in system outputs.",
            "Writes a run summary so operators can focus on the likely missing group first.",
        ],
        "inputs_detail": [
            "Primary = the authoritative list you believe should exist in the target system.",
            "Reference = the current system export you are checking against.",
            "Good ID fields help, but names and address coverage matter when systems are not perfectly aligned.",
        ],
        "outputs_detail": [
            "missing/new bucket: likely absent from the current system.",
            "review bucket: plausible overlaps that need inspection.",
            "matched bucket: likely already present in the current system.",
            "run summary: counts and match reasons for the operator.",
        ],
        "step_by_step": [
            "Treat the source-of-truth file as primary and the live system export as reference.",
            "Normalize name and address fields enough to avoid superficial mismatches.",
            "Run the matcher and keep the full bucket split.",
            "Prioritize the likely-missing output, but audit the review bucket before acting on it.",
        ],
        "watch_for": [
            "This workflow answers a different question than compare_records_to_reference, even though the mechanics are similar.",
            "If the current system export has weaker address coverage than the source file, lean harder on IDs and contact fields.",
        ],
        "best_for": "Use when your main task is gap analysis: who exists in headquarters but does not appear to exist in the current system export?",
    },
    "match_records_to_reference": {
        "summary": "Performs full record matching between a primary file and a reference file and keeps all match buckets.",
        "operator_goal": "Produce a full reconciliation result when you care about every bucket, not just who looks new.",
        "does": [
            "Loads primary and reference CSVs.",
            "Applies any configured normalization profiles before matching.",
            "Normalizes addresses for both datasets.",
            "Runs the scored matcher and assigns CONFIDENT, POSSIBLE, REVIEW, or UNMATCHED.",
            "Exports matched, review, and unmatched files with match score and reason columns.",
        ],
        "inputs_detail": [
            "Primary = the file you want classified against the reference.",
            "Reference = the comparison file you want to match against.",
            "This is the right workflow when you want the whole match picture, not only the new subset.",
        ],
        "outputs_detail": [
            "matched_records.csv: confident matches.",
            "review_records.csv: possible and review-level rows that need human judgment.",
            "unmatched_records.csv: rows with no accepted candidate under current rules.",
            "run_summary.json: counts, reasons, and output metadata.",
        ],
        "step_by_step": [
            "Load and inspect both files.",
            "Map canonical fields and add fallback mappings for unstable header families.",
            "Normalize address evidence used by the matcher.",
            "Run the scored matcher and keep every bucket for follow-up.",
            "Inspect reasons and thresholds when the review bucket is too large.",
        ],
        "watch_for": [
            "This workflow preserves the full bucket split. It is not optimized for export only the new people.",
            "Threshold changes alter bucket volume quickly. Tuning mappings is usually safer than lowering thresholds blindly.",
        ],
        "best_for": "Use when you want a complete reconciliation result, not just a new-versus-existing split.",
    },
    "enrich_records_from_reference": {
        "summary": "Joins fields from a reference file onto a primary file using configured join keys.",
        "operator_goal": "Copy trusted fields from one dataset onto another once you already know how the records should join.",
        "does": [
            "Loads primary and reference CSVs.",
            "Uses the configured enrichment join to pull reference columns into the primary dataset.",
            "Exports one enriched output file and a run summary.",
        ],
        "inputs_detail": [
            "Primary = the working file you want to enrich.",
            "Reference = the lookup file that owns the extra fields.",
            "A stable join key is more important here than fuzzy matching signals.",
        ],
        "outputs_detail": [
            "One enriched CSV that preserves the primary rows and adds copied reference fields.",
            "run_summary.json describing the output path and record counts.",
        ],
        "step_by_step": [
            "Load the working file and lookup file.",
            "Map the join key on both sides.",
            "Declare which reference columns should be copied over.",
            "Run the join and export the enriched result.",
        ],
        "watch_for": [
            "This is an enrichment workflow, not a fuzzy matcher.",
            "If the join keys are dirty, fix them before enrichment instead of expecting this workflow to reconcile them for you.",
        ],
        "best_for": "Use when you already know how records should join and want to copy additional fields over.",
    },
    "extract_projection": {
        "summary": "Reshapes one source file into a smaller export containing only selected columns.",
        "operator_goal": "Strip a larger dataset down to a clean export with only the fields you actually need.",
        "does": [
            "Loads one source CSV.",
            "Optionally reformats date and amount columns if present.",
            "Filters invalid or zero-value amount rows when configured logic applies.",
            "Exports only the requested projection columns.",
        ],
        "inputs_detail": [
            "Source = one input file.",
            "You choose which columns should survive in the final projection.",
        ],
        "outputs_detail": [
            "One projected CSV.",
            "run_summary.json for counts and output metadata.",
        ],
        "step_by_step": [
            "Load the source file.",
            "Map the fields you want to carry forward.",
            "Apply any column-specific formatting rules the projection expects.",
            "Export the reduced file.",
        ],
        "watch_for": [
            "This is for reshaping and trimming data, not matching it.",
            "If the source file is messy, normalize first so the projected export is stable.",
        ],
        "best_for": "Use when you need a clean reporting extract or a reduced export for another system.",
    },
    "split_alternating_rows": {
        "summary": "Splits one source file into two outputs by alternating rows.",
        "operator_goal": "Create a simple even split for assignment, QA, or paired review without changing the rows themselves.",
        "does": [
            "Loads one source CSV.",
            "Sends row 1, 3, 5 and so on to output A.",
            "Sends row 2, 4, 6 and so on to output B.",
        ],
        "inputs_detail": [
            "Source = one input file.",
            "No special canonical mapping is required unless you want the split file to be reused in later workflows.",
        ],
        "outputs_detail": [
            "Two output CSVs with alternating rows from the source file.",
            "run_summary.json with counts and file paths.",
        ],
        "step_by_step": [
            "Load one source file.",
            "Alternate rows into two output files.",
            "Review the counts and use the split files downstream.",
        ],
        "watch_for": [
            "This does not do matching, scoring, or content-aware assignment.",
            "Use it for a mechanical split only.",
        ],
        "best_for": "Use when you need a simple even split for review, assignment, or batching.",
    },
    "process_full_records": {
        "summary": "Runs the largest end-to-end processing path, combining normalization, matching, enrichment, and scoring.",
        "operator_goal": "Run the broadest cleanup and processing pass when you want one heavy workflow instead of a narrow tool.",
        "does": [
            "Loads primary and reference CSVs.",
            "Normalizes dates and addresses.",
            "Optionally deduplicates the primary dataset.",
            "Optionally matches to the reference dataset, classifies address status, enriches contacts, and scores priority.",
            "Exports one processed output file and a run summary.",
        ],
        "inputs_detail": [
            "Primary = the working file you want transformed.",
            "Reference = the comparison or enrichment file used during the run.",
            "This workflow assumes you want several processing stages chained together.",
        ],
        "outputs_detail": [
            "One processed output file containing the transformed primary dataset.",
            "run_summary.json for counts, stages, and output path.",
        ],
        "step_by_step": [
            "Load and normalize the primary and reference data needed by the run.",
            "Optionally deduplicate the primary dataset.",
            "Run matching, identity flagging, enrichment, classification, and scoring stages configured for the job.",
            "Export the processed result for downstream work.",
        ],
        "watch_for": [
            "This is the heaviest preset and can hide too many moving parts if you are still troubleshooting mappings.",
            "Use narrower presets first when you need to debug one specific part of the pipeline.",
        ],
        "best_for": "Use when you want the broadest cleanup and processing pass rather than a narrow single task.",
    },
}


WORKFLOW_INPUTS = {
    "custom_job": [],
    "compare_records_to_reference": ["primary", "reference"],
    "identify_missing_records_from_system": ["primary", "reference"],
    "match_records_to_reference": ["primary", "reference"],
    "enrich_records_from_reference": ["primary", "reference"],
    "extract_projection": ["source"],
    "split_alternating_rows": ["source"],
    "process_full_records": ["primary", "reference"],
}


WORKFLOW_REQUIRED_FIELDS = {
    "custom_job": {},
    "compare_records_to_reference": {
        "primary": ["person_id", "first_name", "last_name", "primary_address1"],
        "reference": ["person_id", "first_name", "last_name", "primary_address1"],
    },
    "identify_missing_records_from_system": {
        "primary": ["person_id", "first_name", "last_name", "primary_address1"],
        "reference": ["person_id", "first_name", "last_name", "primary_address1"],
    },
    "match_records_to_reference": {
        "primary": ["person_id", "first_name", "last_name", "primary_address1"],
        "reference": ["person_id", "first_name", "last_name", "primary_address1"],
    },
    "enrich_records_from_reference": {
        "primary": ["person_id"],
        "reference": ["person_id"],
    },
    "extract_projection": {
        "source": ["created_at", "amount"],
    },
    "split_alternating_rows": {
        "source": [],
    },
    "process_full_records": {
        "primary": ["person_id", "first_name", "last_name", "primary_address1"],
        "reference": ["person_id", "first_name", "last_name", "primary_address1"],
    },
}


WORKFLOW_RECOMMENDED_FIELDS = {
    "custom_job": {},
    "compare_records_to_reference": {
        "primary": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "email",
            "phone",
        ],
        "reference": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "primary_city",
            "primary_state",
            "primary_zip",
            "email",
            "phone",
        ],
    },
    "identify_missing_records_from_system": {
        "primary": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "email",
            "phone",
        ],
        "reference": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "primary_city",
            "primary_state",
            "primary_zip",
            "email",
            "phone",
        ],
    },
    "match_records_to_reference": {
        "primary": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "email",
            "phone",
        ],
        "reference": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "primary_city",
            "primary_state",
            "primary_zip",
            "email",
            "phone",
        ],
    },
    "enrich_records_from_reference": {
        "primary": [
            "person_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
        ],
        "reference": [
            "person_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "primary_address1",
            "primary_address2",
            "primary_city",
            "primary_state",
            "primary_zip",
            "membership_end_date",
        ],
    },
    "extract_projection": {
        "source": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "email",
            "phone",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "created_at",
            "amount",
        ],
    },
    "split_alternating_rows": {
        "source": [
            "person_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
        ],
    },
    "process_full_records": {
        "primary": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "mail_city",
            "mail_state",
            "mail_zip",
            "email",
            "phone",
        ],
        "reference": [
            "person_id",
            "first_name",
            "middle_name",
            "last_name",
            "primary_address1",
            "primary_address2",
            "primary_city",
            "primary_state",
            "primary_zip",
            "email",
            "phone",
        ],
    },
}


def resolve_workflow_name(name: str) -> str:
    return WORKFLOW_ALIASES.get(name, name)
