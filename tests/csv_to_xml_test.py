import unittest
from funcake_dags.scripts.csv_to_xml import csv_reader_to_xml_string, Counter
import csv

class TestCsvToXml(unittest.TestCase):

    def test_csv_reader_to_xml_string(self):
        counter = Counter()
        csv_string = "foo, bar, buzz\n1,,2"
        csv_reader = csv.reader(csv_string.splitlines(), delimiter=',')

        expected = b'<collection dag-id="foo" dag-timestamp="bar">\n  <record airflow-record-id="1">\n    <foo>1</foo>\n    <buzz>2</buzz>\n  </record>\n</collection>\n'
        actual = csv_reader_to_xml_string(csv_reader, "foo", "bar", counter)
        self.assertEqual(actual, expected, "Assert that empty fields are not added.")
        self.assertEqual(counter.count, 1, "Assert that this method updates the counter.")
