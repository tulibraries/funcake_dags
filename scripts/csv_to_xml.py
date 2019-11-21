#! /usr/bin/python

import csv
import os

CSV_FILES = [f for f in os.listdir(".") if f.endswith(".csv") or f.endswith(".CSV")]
for file in CSV_FILES:
    with open(file, encoding="ISO-8859-1") as ifile:
        xmlFile = file[:-4] + ".xml"
        DATA = csv.reader(ifile)
        xmlData = open(xmlFile, "w", encoding="UTF-8")
        xmlData.write('<?xml version="1.0"?>' + "\n")
        xmlData.write("<collection>" + "\n")
        headers = next(DATA, None) # skip the headers
        for record in DATA:
            xmlData.write("  <record>" + "\n")
            for i, field in enumerate(record):
                header = headers[i].strip(" ").replace(" ", "_")
                xmlData.write("    <" + header + ">"  + field + "</" + header + ">"  + "\n")
            xmlData.write("  </record>" + "\n")
        xmlData.write("</collection>" + "\n")
    xmlData.close()
