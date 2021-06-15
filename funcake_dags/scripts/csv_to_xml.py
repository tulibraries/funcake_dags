#! /usr/bin/python

""" Script to transform csv files to xml files """

import csv
import os
from lxml import etree

class Counter:
    def __init__(self):
        self.count = 0
    def click(self):
        self.count += 1

DAGID = os.environ.get("DAGID")
TIMESTAMP = os.environ.get("TIMESTAMP")
CSV_FILES = [f for f in os.listdir(".") if f.endswith(".csv") or f.endswith(".CSV")]

def csv_reader_to_xml_string(csv_reader, dag_id, timestamp, counter):
    root = etree.Element("collection")
    root.attrib["dag-id"] = dag_id
    root.attrib["dag-timestamp"] = timestamp
    headers = next(csv_reader, None) # skip the HEADERS
    for count, record in enumerate(csv_reader):
        record_xml = etree.SubElement(root, "record")
        record_xml.attrib["airflow-record-id"] = str(count + 1)
        counter.click()
        for i, field in enumerate(record):
            if field:
                header = headers[i].strip(" ").replace(" ", "_")
                field_xml = etree.SubElement(record_xml, header)
                field_xml.text = field
    return etree.tostring(root, pretty_print=True, encoding="utf-8").decode("utf-8")

def main():
    counter = Counter()
    for file in CSV_FILES:
        with open(file, encoding="ISO-8859-1") as ifile:
            XML_FILE = file[:-4] + ".xml"
            csv_reader = csv.reader(ifile)
            xml_string = csv_reader_to_xml_string(csv_reader, DAGID, TIMESTAMP, counter)
        with open(XML_FILE, "wb") as ofile:
            ofile.write(xml_string)
    print("Total CSV to XML Record Count: " + str(counter.count))


if __name__ == "__main__":
    main()
