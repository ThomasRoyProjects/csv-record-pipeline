import unittest
import pandas as pd

from reconcile.generic import match_primary_to_reference


class ReconcileGenericEdgeCasesTests(unittest.TestCase):
    def test_unit_conflict_retains_plausible_candidate_when_penalized_score_nonpositive(self) -> None:
        primary = pd.DataFrame(
            [
                {
                    "person_id": "101",
                    "first_name": "Kristen",
                    "last_name": "Reynolds",
                    "primary_address1": "450 Spruce St",
                    "primary_address2": "Apt 2B",
                    "mail_city": "Victoria",
                    "mail_state": "BC",
                    "mail_zip": "V8V1A1",
                    "email": "",
                    "phone": "",
                }
            ]
        )
        # Reference has near-match person at the same street address with Apt 1A
        # (similarities .857 / .875 below score bonus thresholds, so penalized score <= 0)
        reference = pd.DataFrame(
            [
                {
                    "person_id": "201",
                    "first_name": "Kristin",
                    "last_name": "Raynolds",
                    "primary_address1": "450 Spruce St",
                    "primary_address2": "Apt 1A",
                    "primary_city": "Victoria",
                    "primary_state": "BC",
                    "primary_zip": "V8V9Z9",  # different postal code
                    "email": "",
                    "phone": "",
                }
            ]
        )

        matched = match_primary_to_reference(primary, reference)
        row = matched.iloc[0]
        self.assertLessEqual(row["_match_score"], 0)
        self.assertEqual(row["_match_status"], "REVIEW")
        self.assertEqual(row["_match_reason"], "ADDRESS_UNIT_CONFLICT")
        self.assertEqual(row["_matched_reference_id"], "201")
        self.assertTrue(row["_matched_review"])

    def test_unit_difference_does_not_flag_unrelated_people_just_because_units_differ(self) -> None:
        primary = pd.DataFrame(
            [
                {
                    "person_id": "102",
                    "first_name": "Arthur",
                    "last_name": "Pendleton",
                    "primary_address1": "789 Pine Street",
                    "primary_address2": "Suite 100",
                    "mail_city": "Victoria",
                    "mail_state": "BC",
                    "mail_zip": "V8V2B2",
                    "email": "",
                    "phone": "",
                }
            ]
        )
        # Reference has an unrelated person at a different street who also has a unit number
        reference = pd.DataFrame(
            [
                {
                    "person_id": "202",
                    "first_name": "Zoe",
                    "last_name": "Kowalski",
                    "primary_address1": "123 Elm Street",
                    "primary_address2": "Suite 200",
                    "primary_city": "Victoria",
                    "primary_state": "BC",
                    "primary_zip": "V8V2B2",  # same zip code pulls into pool
                    "email": "",
                    "phone": "",
                }
            ]
        )

        matched = match_primary_to_reference(primary, reference)
        row = matched.iloc[0]
        self.assertEqual(row["_match_status"], "UNMATCHED")
        self.assertEqual(row["_match_reason"], "NO_MATCH")
        self.assertFalse(row["_matched_review"])

    def test_same_building_different_name_not_flagged_for_review(self) -> None:
        primary = pd.DataFrame(
            [
                {
                    "person_id": "103",
                    "first_name": "Arthur",
                    "last_name": "Pendleton",
                    "primary_address1": "500 Oak Avenue",
                    "primary_address2": "Apt 1",
                    "mail_city": "Victoria",
                    "mail_state": "BC",
                    "mail_zip": "V8V2B2",
                    "email": "",
                    "phone": "",
                }
            ]
        )
        # Reference has an unrelated neighbor in the same building with the same postal code
        reference = pd.DataFrame(
            [
                {
                    "person_id": "203",
                    "first_name": "Zoe",
                    "last_name": "Kowalski",
                    "primary_address1": "500 Oak Avenue",
                    "primary_address2": "Apt 2",
                    "primary_city": "Victoria",
                    "primary_state": "BC",
                    "primary_zip": "V8V2B2",
                    "email": "",
                    "phone": "",
                }
            ]
        )

        matched = match_primary_to_reference(primary, reference)
        row = matched.iloc[0]
        self.assertEqual(row["_match_status"], "UNMATCHED")
        self.assertEqual(row["_match_reason"], "NO_MATCH")
        self.assertFalse(row["_matched_review"])

    def test_co_resident_with_address_postal_phone_does_not_displace_true_moved_candidate(self) -> None:
        # Primary is Alice Walker who moved to an updated address
        primary = pd.DataFrame(
            [
                {
                    "person_id": "104",
                    "first_name": "Alice",
                    "last_name": "Walker",
                    "primary_address1": "900 New Blvd",
                    "primary_address2": "",
                    "mail_city": "Victoria",
                    "mail_state": "BC",
                    "mail_zip": "V8V8N8",
                    "email": "",
                    "phone": "250-555-4321",
                }
            ]
        )
        # Reference contains:
        # 1. Bob Walker (surname + primary's address + postal + shared phone -> strong household evidence)
        # 2. Alice Walker (exact full name match at old address, no phone listed)
        reference = pd.DataFrame(
            [
                {
                    "person_id": "204",
                    "first_name": "Bob",
                    "last_name": "Walker",
                    "primary_address1": "900 New Blvd",
                    "primary_address2": "",
                    "primary_city": "Victoria",
                    "primary_state": "BC",
                    "primary_zip": "V8V8N8",
                    "email": "",
                    "phone": "250-555-4321",
                },
                {
                    "person_id": "205",
                    "first_name": "Alice",
                    "last_name": "Walker",
                    "primary_address1": "100 Old Road",
                    "primary_address2": "",
                    "primary_city": "Victoria",
                    "primary_state": "BC",
                    "primary_zip": "V8V1A1",
                    "email": "",
                    "phone": "",
                },
            ]
        )

        matched = match_primary_to_reference(primary, reference)
        row = matched.iloc[0]
        self.assertEqual(row["_matched_reference_id"], "205")


if __name__ == "__main__":
    unittest.main()
