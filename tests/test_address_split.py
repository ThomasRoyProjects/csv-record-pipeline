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


if __name__ == "__main__":
    unittest.main()
