#! /usr/bin/python

""" Script to transform csv files to xml files """

import csv
import os
from lxml import etree

DAGID = os.environ.get("DAGID")
TIMESTAMP = os.environ.get("TIMESTAMP")

CSV_FILES = [f for f in os.listdir(".") if f.endswith(".csv") or f.endswith(".CSV")]
for file in CSV_FILES:
    with open(file, encoding="ISO-8859-1") as ifile:
        XML_FILE = file[:-4] + ".xml"
        DATA = csv.reader(ifile)
        XML_DATA = etree.Element("collection")
        XML_DATA.attrib["dag-id"] = DAGID
        XML_DATA.attrib["dag-timestamp"] = TIMESTAMP
        HEADERS = next(DATA, None) # skip the HEADERS
        for count, record in enumerate(DATA):
            record_xml = etree.SubElement(XML_DATA, "record")
            record_xml.attrib["airflow-record-id"] = str(count + 1)
            for i, field in enumerate(record):
                header = HEADERS[i].strip(" ").replace(" ", "_")
                field_xml = etree.SubElement(record_xml, header)
                field_xml.text = field
    with open(XML_FILE, "wb") as ofile:
        ofile.write(etree.tostring(XML_DATA, pretty_print=True))
