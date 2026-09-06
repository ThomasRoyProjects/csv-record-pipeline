import unittest
import pandas as pd

from core.runtime_loader import resolve_columns


class RuntimeLoaderResolveColumnsTests(unittest.TestCase):
    def test_stable_source_view_mapping_id_col_to_multiple_destinations(self) -> None:
        df = pd.DataFrame(
            {
                "ID_COL": ["ID100", "ID200", "ID300"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        mapping = {
            "person_id": "ID_COL",
            "external_id": "ID_COL",
        }
        resolved = resolve_columns(df, mapping)
        self.assertIn("person_id", resolved.columns)
        self.assertIn("external_id", resolved.columns)
        self.assertEqual(list(resolved["person_id"]), ["ID100", "ID200", "ID300"])
        self.assertEqual(list(resolved["external_id"]), ["ID100", "ID200", "ID300"])

    def test_canonical_coalescing_preserves_existing_values_and_fills_blanks(self) -> None:
        df = pd.DataFrame(
            {
                "person_id": ["P001", "", None],
                "legacy_id": ["L001", "L002", "L003"],
            }
        )
        mapping = {
            "person_id": "legacy_id",
        }
        resolved = resolve_columns(df, mapping)
        self.assertEqual(resolved["person_id"].iloc[0], "P001")
        self.assertEqual(resolved["person_id"].iloc[1], "L002")
        self.assertEqual(resolved["person_id"].iloc[2], "L003")

    def test_conflicting_destinations_order_independence(self) -> None:
        df1 = pd.DataFrame({"A": ["", "val_a"], "B": ["val_b", ""]})
        df2 = pd.DataFrame({"A": ["", "val_a"], "B": ["val_b", ""]})

        mapping_forward = {"A": "B", "C": "A"}
        mapping_reverse = {"C": "A", "A": "B"}

        res1 = resolve_columns(df1, mapping_forward)
        res2 = resolve_columns(df2, mapping_reverse)

        self.assertEqual(list(res1["A"]), ["val_b", "val_a"])
        self.assertEqual(list(res2["A"]), ["val_b", "val_a"])
        self.assertEqual(list(res1["C"]), ["", "val_a"])
        self.assertEqual(list(res2["C"]), ["", "val_a"])

if __name__ == "__main__":
    unittest.main()
