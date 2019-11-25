#! /usr/bin/python

""" Script to transform csv files to xml files """

import csv
import os

DAGID = os.environ.get("DAGID")
TIMESTAMP = os.environ.get("TIMESTAMP")

CSV_FILES = [f for f in os.listdir(".") if f.endswith(".csv") or f.endswith(".CSV")]
for file in CSV_FILES:
    with open(file, encoding="ISO-8859-1") as ifile:
        XML_FILE = file[:-4] + ".xml"
        DATA = csv.reader(ifile)
        XML_DATA = open(XML_FILE, "w", encoding="UTF-8")
        XML_DATA.write('<?xml version="1.0"?>' + "\n")
        XML_DATA.write(f"<collection dag-id='{DAGID}' dag-timestamp='{TIMESTAMP}'>" + "\n")
        HEADERS = next(DATA, None) # skip the HEADERS
        for count, record in enumerate(DATA):
            XML_DATA.write(f"  <record airflow-record-id={count + 1}>" + "\n")
            for i, field in enumerate(record):
                header = HEADERS[i].strip(" ").replace(" ", "_")
                XML_DATA.write("    <" + header + ">"  + field + "</" + header + ">"  + "\n")
            XML_DATA.write("  </record>" + "\n")
        XML_DATA.write("</collection>" + "\n")
    XML_DATA.close()
