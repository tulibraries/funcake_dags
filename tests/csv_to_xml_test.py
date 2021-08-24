import unittest
from funcake_dags.scripts.csv_to_xml import csv_reader_to_xml_string, Counter
import csv
import shutil
import tempfile
import os

class TestCsvToXml(unittest.TestCase):

    def test_csv_reader_to_xml_string(self):
        counter = Counter()
        csv_string = "foo, bar, buzz\n1,,2"
        csv_reader = csv.reader(csv_string.splitlines(), delimiter=',')

        expected = b'<collection dag-id="foo" dag-timestamp="bar">\n  <record airflow-record-id="1">\n    <foo>1</foo>\n    <buzz>2</buzz>\n  </record>\n</collection>\n'
        actual = csv_reader_to_xml_string(csv_reader, "foo", "bar", counter)
        self.assertEqual(actual, expected, "Assert that empty fields are not added.")
        self.assertEqual(counter.count, 1, "Assert that this method updates the counter.")


    def test_csv_to_xml(self):
        orig_cwd = os.getcwd()
        tmp_dir = tempfile.TemporaryDirectory(dir="tests/tmp")

        shutil.copy2("tests/fixtures/csv-to-xml.csv", tmp_dir.name + "/csv-to-xml.csv")
        shutil.copy2("funcake_dags/scripts/csv_to_xml.py", tmp_dir.name + "/csv_to_xml.py")
        os.chdir(tmp_dir.name)
        output = os.system("DAGID=foo TIMESTAMP=bar python csv_to_xml.py")

        # If this fails you need to debug it using pdb.set_trace()
        self.assertEqual(output, 0, "Script execution did not fail.")
        self.assertIn("“Sarijs manuscripts,”", open("csv-to-xml.xml").read())
        os.chdir(orig_cwd)
