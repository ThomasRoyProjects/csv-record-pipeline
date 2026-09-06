import unittest

from normalize.address_split import split_unit_and_street


class AddressSplitTests(unittest.TestCase):
    def test_numeric_unit_prefix_splits(self):
        street, unit, status = split_unit_and_street("729-385 Example Street")
        self.assertEqual(street, "385 Example Street")
        self.assertEqual(unit, "729")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_alpha_unit_prefix_splits(self):
        street, unit, status = split_unit_and_street("C-2529 Example Road")
        self.assertEqual(street, "2529 Example Road")
        self.assertEqual(unit, "C")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_complex_multilevel_is_not_split(self):
        street, unit, status = split_unit_and_street("159-3-3690 Example Avenue")
        self.assertEqual(street, "159-3-3690 Example Avenue")
        self.assertEqual(unit, "")
        self.assertEqual(status, "COMPLEX_MULTI_LEVEL")

    def test_named_unit_is_not_split(self):
        street, unit, status = split_unit_and_street("Dock E-1 Example Drive")
        self.assertEqual(street, "Dock E-1 Example Drive")
        self.assertEqual(unit, "")
        self.assertEqual(status, "NAMED_UNIT")
    def test_suffix_unit_apt_splits(self):
        street, unit, status = split_unit_and_street("123 Main St Apt 4B")
        self.assertEqual(street, "123 Main St")
        self.assertEqual(unit, "4B")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_suffix_unit_hash_splits(self):
        street, unit, status = split_unit_and_street("50 Oak Ave #12")
        self.assertEqual(street, "50 Oak Ave")
        self.assertEqual(unit, "12")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_suffix_unit_hash_spaced_splits(self):
        street, unit, status = split_unit_and_street("50 Oak Ave # 12")
        self.assertEqual(street, "50 Oak Ave")
        self.assertEqual(unit, "12")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_suffix_unit_comma_and_dot_splits(self):
        street, unit, status = split_unit_and_street("123 Main St, Apt. 4B")
        self.assertEqual(street, "123 Main St")
        self.assertEqual(unit, "4B")
        self.assertEqual(status, "OK_UNIT_STREET")

    def test_suffix_suite_splits(self):
        street, unit, status = split_unit_and_street("100 Broadway Suite 500")
        self.assertEqual(street, "100 Broadway")
        self.assertEqual(unit, "500")
        self.assertEqual(status, "OK_UNIT_STREET")


if __name__ == "__main__":
    unittest.main()
