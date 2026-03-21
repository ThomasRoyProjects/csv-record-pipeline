import csv
import tempfile
import unittest
from pathlib import Path

from services.workflow_service import classify_headers, suggest_mappings, validate_runtime_config


class SuggestMappingsTests(unittest.TestCase):
    def test_membership_end_date_does_not_map_to_membership_type(self) -> None:
        headers = [
            "ConstituantID",
            "FirstName",
            "LastName",
            "MembershipType",
            "PhoneNumber",
            "Email",
        ]

        suggestions = suggest_mappings(headers)

        self.assertNotIn("membership_end_date", suggestions)

    def test_membership_end_date_maps_to_actual_expiry_field(self) -> None:
        headers = [
            "ConstituantID",
            "first_name",
            "last_name",
            "memb_exp_date",
            "email1",
        ]

        suggestions = suggest_mappings(headers)

        self.assertEqual(suggestions.get("membership_end_date"), "memb_exp_date")

    def test_legacy_full_field_address_headers_are_suggested(self) -> None:
        headers = [
            "ConstituantID",
            "FirstName",
            "LastName",
            "MailingAddress",
            "MailMunisipality",
            "MailProvince",
            "MailPostalCode",
            "ResMunisipality",
            "ResProvince",
            "ResPostalCode",
        ]

        suggestions = suggest_mappings(headers)

        self.assertEqual(suggestions.get("primary_address1"), "MailingAddress")
        self.assertEqual(suggestions.get("mail_city"), "MailMunisipality")
        self.assertEqual(suggestions.get("mail_state"), "MailProvince")
        self.assertEqual(suggestions.get("mail_zip"), "MailPostalCode")
        self.assertEqual(suggestions.get("primary_city"), "ResMunisipality")
        self.assertEqual(suggestions.get("primary_state"), "ResProvince")
        self.assertEqual(suggestions.get("primary_zip"), "ResPostalCode")


class ClassifyHeadersTests(unittest.TestCase):
    def test_riding_donations_not_misclassified_as_identity(self) -> None:
        groups = classify_headers(
            [
                "external_id",
                "first_name",
                "RidingDonations_YTD",
                "RidingDonations_LT",
                "Email",
            ]
        )

        self.assertIn("external_id", groups.get("identity", []))
        self.assertNotIn("RidingDonations_YTD", groups.get("identity", []))
        self.assertNotIn("RidingDonations_LT", groups.get("identity", []))
        self.assertIn("RidingDonations_YTD", groups.get("money", []))
        self.assertIn("RidingDonations_LT", groups.get("money", []))


class ValidateRuntimeConfigTests(unittest.TestCase):
    def test_custom_job_requires_stage_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "input.csv"
            with csv_path.open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["person_id", "first_name", "last_name"])
                writer.writerow(["1", "Ada", "Lovelace"])

            config = {
                "profile": "custom_job",
                "inputs": {
                    "primary": {
                        "path": str(csv_path),
                        "columns": {
                            "person_id": "person_id",
                            "first_name": "first_name",
                            "last_name": "last_name",
                        },
                    }
                },
                "outputs": {"records": {"base_dir": str(Path(tmpdir) / "output")}},
                "stage_sequence": [],
            }

            errors = validate_runtime_config(config)

            self.assertTrue(any("stage_sequence" in error for error in errors))


if __name__ == "__main__":
    unittest.main()
