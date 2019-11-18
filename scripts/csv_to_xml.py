#! /usr/bin/python

import csv
import os

CSV_FILES = [f for f in os.listdir(".") if f.endswith(".csv") or f.endswith(".CSV") or f.endswith(".xlsx")]
for file in CSV_FILES:
    with open(file, encoding="ISO-8859-1") as ifile:
        xmlFile = file[:-4] + ".xml"
        DATA = csv.reader(ifile)
        xmlData = open(xmlFile, "w")
        xmlData.write('<?xml version="1.0"?>' + "\n")
        next(DATA, None) # skip the headers
        xmlData.write("<collection>" + "\n")
        for record in DATA:
            xmlData.write("  <record>" + "\n")
            for i, field in enumerate(record):
                xmlData.write("    <field%s>" % i + field + "</field%s>" % i + "\n")
            xmlData.write("  </record>" + "\n")
        xmlData.write("</collection>" + "\n")
    xmlData.close()
